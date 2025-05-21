use std::time::Duration;

use bytes::Bytes;
use eventsource_stream::Eventsource;
use futures::Stream;
use futures::StreamExt;
use futures::TryStreamExt;
use reqwest::StatusCode;
use serde_json::json;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tracing::debug;
use tracing::trace;

use crate::ModelProviderInfo;
use crate::client_common::Prompt;
use crate::client_common::ResponseEvent;
use crate::client_common::ResponseStream;
use crate::error::CodexErr;
use crate::error::Result;
use crate::flags::OPENAI_REQUEST_MAX_RETRIES;
use crate::flags::OPENAI_STREAM_IDLE_TIMEOUT_MS;
use crate::models::ContentItem;
use crate::models::ResponseItem;
use crate::util::backoff;
use crate::util::UrlExt;

/// Implementation for the classic Chat Completions API. This is intentionally
/// minimal: we only stream back plain assistant text.
pub(crate) async fn stream_chat_completions(
    prompt: &Prompt,
    model: &str,
    client: &reqwest::Client,
    provider: &ModelProviderInfo,
) -> Result<ResponseStream> {
    // Build messages array
    let mut messages = Vec::<serde_json::Value>::new();
    let mut full_instructions = prompt.get_full_instructions().into_owned();

    // Embed previous_response_id so the model can reference the prior turn.
    if let Some(prev_id) = &prompt.prev_id {
        full_instructions.push_str(&format!(
            "\n\n[METADATA] previous_response_id = {}",
            prev_id
        ));
    }
    messages.push(json!({"role": "system", "content": full_instructions}));

    for item in &prompt.input {
        match item {
            ResponseItem::Message { role, content } => {
                // Aggregate the textual parts into a single string. We ignore
                // images and other modalities for now; those are handled
                // separately.
                let mut text = String::new();
                for c in content {
                    match c {
                        ContentItem::InputText { text: t }
                        | ContentItem::OutputText { text: t } => {
                            text.push_str(t);
                        }
                        _ => {}
                    }
                }
                messages.push(json!({"role": role, "content": text}));
            }
            // Surface prior tool call results so the model can take them into
            // account. 
            ResponseItem::FunctionCallOutput { call_id, output } => {
                messages.push(json!({
                    "role": "tool",
                    "tool_call_id": call_id,
                    "content": output.content,
                }));
            }
            // Retain the *assistant-initiated* tool invocation in the history so the
            // following `tool` message is considered valid by the Chat Completions
            // protocol validator. Omitting it causes the 400:
            //   "messages with role 'tool' must be a response to a preceding message with 'tool_calls'".
            ResponseItem::FunctionCall { name, arguments, call_id } => {
                messages.push(json!({
                    "role": "assistant",
                    "content": serde_json::Value::Null,
                    "tool_calls": [{
                        "id": call_id,
                        "type": "function",
                        "function": {
                            "name": name,
                            "arguments": arguments,
                        }
                    }]
                }));
            }
            _ => {}
        }
    }

    // Minimal "shell" tool identical to what the Responses API advertises but
    // converted to the Chat-Completions schema.
    let mut tools_json = Vec::new();

    // Built-in shell function (always present so the model can decide to use
    // it – the CLI will execute it locally when received via
    // `ResponseItem::FunctionCall`).
    tools_json.push(json!({
        "type": "function",
        "function": {
            "name": "shell",
            "description": "Runs a shell command, and returns its output.",
            "parameters": {
                "type": "object",
                "properties": {
                    "command": {
                        "type": "array",
                        "items": { "type": "string" }
                    },
                    "workdir": { "type": "string" },
                    "timeout": { "type": "number" }
                },
                "required": ["command"],
                "additionalProperties": false
            }
        }
    }));

    // Extra tools coming from external MCP servers (if any) – they are
    // already valid JSON Schemas so we can wrap them inside the Chat tool
    // envelope without further transformation.
    for (name, tool) in &prompt.extra_tools {
        let mut desc = "External tool".to_string();
        let mut params = json!({ "type": "object" });

        if let Ok(val) = serde_json::to_value(tool) {
            // Best-effort extraction; ignore errors and fall back to defaults.
            if let Some(d) = val.get("description").and_then(|v| v.as_str()) {
                desc = d.to_string();
            }
            if let Some(p) = val.get("parameters") {
                params = p.clone();
            }
        }

        tools_json.push(json!({
            "type": "function",
            "function": {
                "name": name,
                "description": desc,
                "parameters": params
            }
        }));
    }

    // Final request payload
    let payload = json!({
        "model": model,
        "messages": messages,
        "stream": true,
        "tools": tools_json,
        "tool_choice": "auto"
    });

    let url = provider.base_url.clone().append_path("/chat/completions")?.to_string();

    debug!("{} POST (chat)", &url);
    trace!("request payload: {}", payload);

    let api_key = provider.api_key()?;
    let mut attempt = 0;
    loop {
        attempt += 1;

        let mut req_builder = client.post(&url);
        if let Some(api_key) = &api_key {
            req_builder = req_builder.bearer_auth(api_key.clone());
        }
        let res = req_builder
            .header(reqwest::header::ACCEPT, "text/event-stream")
            .json(&payload)
            .send()
            .await;

        match res {
            Ok(resp) if resp.status().is_success() => {
                let (tx_event, rx_event) = mpsc::channel::<Result<ResponseEvent>>(16);
                let stream = resp.bytes_stream().map_err(CodexErr::Reqwest);
                tokio::spawn(process_chat_sse(stream, tx_event));
                return Ok(ResponseStream { rx_event });
            }
            Ok(res) => {
                let status = res.status();
                if !(status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error()) {
                    let body = (res.text().await).unwrap_or_default();
                    return Err(CodexErr::UnexpectedStatus(status, body));
                }

                if attempt > *OPENAI_REQUEST_MAX_RETRIES {
                    return Err(CodexErr::RetryLimit(status));
                }

                let retry_after_secs = res
                    .headers()
                    .get(reqwest::header::RETRY_AFTER)
                    .and_then(|v| v.to_str().ok())
                    .and_then(|s| s.parse::<u64>().ok());

                let delay = retry_after_secs
                    .map(|s| Duration::from_millis(s * 1_000))
                    .unwrap_or_else(|| backoff(attempt));
                tokio::time::sleep(delay).await;
            }
            Err(e) => {
                if attempt > *OPENAI_REQUEST_MAX_RETRIES {
                    return Err(e.into());
                }
                let delay = backoff(attempt);
                tokio::time::sleep(delay).await;
            }
        }
    }
}

/// Lightweight SSE processor for the Chat Completions streaming format. The
/// output is mapped onto Codex's internal [`ResponseEvent`] so that the rest
/// of the pipeline can stay agnostic of the underlying wire format.
async fn process_chat_sse<S>(stream: S, tx_event: mpsc::Sender<Result<ResponseEvent>>)
where
    S: Stream<Item = Result<Bytes>> + Unpin,
{
    let mut stream = stream.eventsource();

    let idle_timeout = *OPENAI_STREAM_IDLE_TIMEOUT_MS;

    // Accumulators used when the assistant returns a streaming function/tool
    // call.  The OpenAI Chat Completions SSE format may emit `function_call`
    // or `tool_calls` deltas with the `name` and `arguments` fields spread
    // across multiple chunks.  We buffer the pieces here until the model
    // signals completion via `finish_reason`, at which point we forward a
    // fully-assembled `ResponseItem::FunctionCall` to higher layers.
    let mut cur_call_name = String::new();
    let mut cur_call_args = String::new();
    let mut cur_call_id = String::new();

    // Response id propagated through the Completed event so higher layers can
    // chain turns (parity with the Responses API).
    let mut response_id: Option<String> = None;

    loop {
        let sse = match timeout(idle_timeout, stream.next()).await {
            Ok(Some(Ok(ev))) => ev,
            Ok(Some(Err(e))) => {
                let _ = tx_event.send(Err(CodexErr::Stream(e.to_string()))).await;
                return;
            }
            Ok(None) => {
                // Stream closed gracefully – emit Completed with dummy id.
                let _ = tx_event
                    .send(Ok(ResponseEvent::Completed {
                        response_id: response_id.clone().unwrap_or_default(),
                    }))
                    .await;
                return;
            }
            Err(_) => {
                let _ = tx_event
                    .send(Err(CodexErr::Stream("idle timeout waiting for SSE".into())))
                    .await;
                return;
            }
        };

        // OpenAI Chat streaming sends a literal string "[DONE]" when finished.
        if sse.data.trim() == "[DONE]" {
            let _ = tx_event
                .send(Ok(ResponseEvent::Completed {
                    response_id: response_id.clone().unwrap_or_default(),
                }))
                .await;
            return;
        }

        // Parse JSON chunk
        let chunk: serde_json::Value = match serde_json::from_str(&sse.data) {
            Ok(v) => v,
            Err(e) => {
                debug!("Failed to parse chat SSE JSON: {e}; raw data: {}", &sse.data);
                continue;
            }
        };


        // Cache the stream-wide id (only first occurrence needed).
        if response_id.is_none() {
            if let Some(id_val) = chunk.get("id").and_then(|v| v.as_str()) {
                response_id = Some(id_val.to_string());
            }
        }

        trace!("chat-sse delta: {}", chunk);

        let choice = match chunk.get("choices").and_then(|c| c.get(0)) {
            Some(c) => c,
            None => continue,
        };
        let delta = choice.get("delta");

        // Assistant text tokens
        if let Some(content) = delta
            .and_then(|d| d.get("content"))
            .and_then(|c| c.as_str())
        {
            let item = ResponseItem::Message {
                role: "assistant".to_string(),
                content: vec![ContentItem::OutputText {
                    text: content.to_string(),
                }],
            };

            let _ = tx_event.send(Ok(ResponseEvent::OutputItemDone(item))).await;
        }

        // Legacy single function_call representation
        if let Some(fc) = delta.and_then(|d| d.get("function_call")) {
            let name_opt = fc.get("name").and_then(|v| v.as_str());
            let args_opt = fc.get("arguments").and_then(|v| v.as_str());
            if let Some(name) = name_opt {
                if cur_call_name.is_empty() {
                    cur_call_name.push_str(name);
                }
            }
                if let Some(args) = args_opt {
                    cur_call_args.push_str(args);
                }
        }

        // New tool_calls representation (array) – we only track the first
        //    entry for now which is sufficient for serial, single calls.
        if let Some(tool_calls) = delta.and_then(|d| d.get("tool_calls")).and_then(|v| v.as_array()) {
            if let Some(first) = tool_calls.first() {
                let id_opt = first.get("id").and_then(|v| v.as_str());
                let func_obj = first.get("function");
                let name_opt = func_obj.and_then(|f| f.get("name").and_then(|v| v.as_str()));
                let args_opt = func_obj.and_then(|f| f.get("arguments").and_then(|v| v.as_str()));

                if let Some(id) = id_opt {
                    if cur_call_id.is_empty() {
                        cur_call_id.push_str(id);
                    }
                }
                if let Some(name) = name_opt {
                    if cur_call_name.is_empty() {
                        cur_call_name.push_str(name);
                    }
                }
                if let Some(args) = args_opt {
                    cur_call_args.push_str(args);
                }
            }
        }

        // On finish_reason emit the accumulated function call.
        if let Some(_reason) = choice
            .get("finish_reason")
            .and_then(|v| v.as_str())
        {
            // Any non-null finish_reason marks the end of the streaming block
            // for this choice. For the legacy single `function_call` format
            // OpenAI uses `"stop"`; for the newer array-based tool-calling
            // it is `"tool_calls"`. We flush *whenever* we have an
            // accumulated call and the model says it is done.
            if !cur_call_name.is_empty() {
                if !cur_call_name.is_empty() {
                    // Replace any literal newlines that slipped through the
                    // streaming decoder with the escaped variant so the
                    // resulting JSON remains valid when downstream deserializes
                    // it via `serde_json::from_str()`.
                    let sanitized_args = cur_call_args
                        .replace('\n', "\\n")
                        .replace('\r', "\\r");

                    let item = ResponseItem::FunctionCall {
                        name: cur_call_name.clone(),
                        arguments: sanitized_args,
                        call_id: cur_call_id.clone(),
                    };
                    let _ = tx_event.send(Ok(ResponseEvent::OutputItemDone(item))).await;
                }

                // Clear accumulators for next call
                cur_call_name.clear();
                cur_call_args.clear();
                cur_call_id.clear();
            }
        }
    }
}

/// Optional client-side aggregation helper
///
/// Stream adapter that merges the incremental `OutputItemDone` chunks coming from
/// [`process_chat_sse`] into a *running* assistant message, **suppressing the
/// per-token deltas**.  The stream stays silent while the model is thinking
/// and only emits two events per turn:
///
///   1. `ResponseEvent::OutputItemDone` with the *complete* assistant message
///      (fully concatenated).
///   2. The original `ResponseEvent::Completed` right after it.
///
/// This mirrors the behaviour the TypeScript CLI exposes to its higher layers.
///
/// The adapter is intentionally *lossless*: callers who do **not** opt in via
/// [`AggregateStreamExt::aggregate()`] keep receiving the original unmodified
/// events.
pub(crate) struct AggregatedChatStream<S> {
    inner: S,
    cumulative: String,
    pending_completed: Option<ResponseEvent>,
}

impl<S> Stream for AggregatedChatStream<S>
where
    S: Stream<Item = Result<ResponseEvent>> + Unpin,
{
    type Item = Result<ResponseEvent>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        // First, flush any buffered Completed event from the previous call.
        if let Some(ev) = this.pending_completed.take() {
            return Poll::Ready(Some(Ok(ev)));
        }

        loop {
            match Pin::new(&mut this.inner).poll_next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                Poll::Ready(Some(Ok(ResponseEvent::OutputItemDone(item)))) => {
                    // Accumulate *assistant* text but do not emit yet.  All
                    // other item types (function calls, tool calls, …) are
                    // forwarded immediately so higher layers receive them in
                    // real-time.

                    if let crate::models::ResponseItem::Message { role, content } = &item {
                        if role == "assistant" {
                            if let Some(text) = content.iter().find_map(|c| match c {
                                crate::models::ContentItem::OutputText { text } => Some(text),
                                _ => None,
                            }) {
                                this.cumulative.push_str(text);
                            }

                            // Swallow partial assistant delta; keep polling
                            // until Completed so we can emit a *single*
                            // aggregated message at the end of the turn.
                            continue;
                        }
                    }

                    // Non-assistant items are forwarded immediately.
                    return Poll::Ready(Some(Ok(ResponseEvent::OutputItemDone(item))));
                }
                Poll::Ready(Some(Ok(ResponseEvent::Completed { response_id }))) => {
                    if !this.cumulative.is_empty() {
                        let aggregated_item = crate::models::ResponseItem::Message {
                            role: "assistant".to_string(),
                            content: vec![crate::models::ContentItem::OutputText {
                                text: std::mem::take(&mut this.cumulative),
                            }],
                        };

                        // Buffer Completed so it is returned *after* the aggregated message.
                        this.pending_completed = Some(ResponseEvent::Completed { response_id });

                        return Poll::Ready(Some(Ok(ResponseEvent::OutputItemDone(
                            aggregated_item,
                        ))));
                    }

                    // Nothing aggregated – forward Completed directly.
                    return Poll::Ready(Some(Ok(ResponseEvent::Completed { response_id })));
                } // No other `Ok` variants exist at the moment, continue polling.
            }
        }
    }
}

/// Extension trait that activates aggregation on any stream of [`ResponseEvent`].
pub(crate) trait AggregateStreamExt: Stream<Item = Result<ResponseEvent>> + Sized {
    /// Returns a new stream that emits **only** the final assistant message
    /// per turn instead of every incremental delta.  The produced
    /// `ResponseEvent` sequence for a typical text turn looks like:
    ///
    /// ```ignore
    ///     OutputItemDone(<full message>)
    ///     Completed { .. }
    /// ```
    ///
    /// No other `OutputItemDone` events will be seen by the caller.
    ///
    /// Usage:
    ///
    /// ```ignore
    /// let agg_stream = client.stream(&prompt).await?.aggregate();
    /// while let Some(event) = agg_stream.next().await {
    ///     // event now contains cumulative text
    /// }
    /// ```
    fn aggregate(self) -> AggregatedChatStream<Self> {
        AggregatedChatStream {
            inner: self,
            cumulative: String::new(),
            pending_completed: None,
        }
    }
}

impl<T> AggregateStreamExt for T where T: Stream<Item = Result<ResponseEvent>> + Sized {}
