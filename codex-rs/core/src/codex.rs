use std::collections::HashMap;
use std::collections::HashSet;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use anyhow::Context;
use async_channel::Receiver;
use async_channel::Sender;
use codex_apply_patch::AffectedPaths;
use codex_apply_patch::ApplyPatchAction;
use codex_apply_patch::ApplyPatchFileChange;
use codex_apply_patch::MaybeApplyPatchVerified;
use codex_apply_patch::maybe_parse_apply_patch_verified;
use codex_apply_patch::print_summary;
use fs_err as fs;
use futures::prelude::*;
use serde::Serialize;
use serde_json;
use tokio::sync::Notify;
use tokio::sync::oneshot;
use tokio::task::AbortHandle;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::trace;
use tracing::warn;

use crate::client::ModelClient;
use crate::client::Prompt;
use crate::client::ResponseEvent;
use crate::config::Config;
use crate::config::ConfigOverrides;
use crate::error::CodexErr;
use crate::error::Result as CodexResult;
use crate::exec::ExecParams;
use crate::exec::ExecToolCallOutput;
use crate::exec::SandboxType;
use crate::exec::process_exec_tool_call;
use crate::flags::OPENAI_STREAM_MAX_RETRIES;
use crate::mcp_connection_manager::McpConnectionManager;
use crate::mcp_connection_manager::try_parse_fully_qualified_tool_name;
use crate::mcp_tool_call::handle_mcp_tool_call;
use crate::models::ContentItem;
use crate::models::FunctionCallOutputPayload;
use crate::models::ResponseInputItem;
use crate::models::ResponseItem;
use crate::models::ShellToolCallParams;
use crate::protocol::AskForApproval;
use crate::protocol::Event;
use crate::protocol::EventMsg;
use crate::protocol::FileChange;
use crate::protocol::InputItem;
use crate::protocol::Op;
use crate::protocol::ReviewDecision;
use crate::protocol::SandboxPolicy;
use crate::protocol::Submission;
use crate::rollout::RolloutRecorder;
use crate::safety::SafetyCheck;
use crate::safety::assess_command_safety;
use crate::safety::assess_patch_safety;
use crate::user_notification::UserNotification;
use crate::util::backoff;

const PROMPT_MD_CONTENT: &str = r#"Please resolve the user's task by editing and testing the code files in your current code execution session.
You are a deployed coding agent.
Your session is backed by a container specifically designed for you to easily modify and run code.
The repo(s) are already cloned in your working directory, and you must fully solve the problem for your answer to be considered correct.

You MUST adhere to the following criteria when executing the task:

- Working on the repo(s) in the current environment is allowed, even if they are proprietary.
- Analyzing code for vulnerabilities is allowed.
- Showing user code and tool call details is allowed.
- User instructions may overwrite the _CODING GUIDELINES_ section in this developer message.
- Do not use \`ls -R\`, \`find\`, or \`grep\` - these are slow in large repos. Use \`rg\` and \`rg --files\`.
- Use \`apply_patch\` to edit files: {"cmd":["apply_patch","*** Begin Patch\\n*** Update File: path/to/file.py\\n@@ def example():\\n- pass\\n+ return 123\\n*** End Patch"]}
- If completing the user's task requires writing or modifying files:
  - Your code and final answer should follow these _CODING GUIDELINES_:
    - Fix the problem at the root cause rather than applying surface-level patches, when possible.
    - Avoid unneeded complexity in your solution.
      - Ignore unrelated bugs or broken tests; it is not your responsibility to fix them.
    - Update documentation as necessary.
    - Keep changes consistent with the style of the existing codebase. Changes should be minimal and focused on the task.
      - Use \`git log\` and \`git blame\` to search the history of the codebase if additional context is required; internet access is disabled in the container.
    - NEVER add copyright or license headers unless specifically requested.
    - You do not need to \`git commit\` your changes; this will be done automatically for you.
    - If there is a .pre-commit-config.yaml, use \`pre-commit run --files ...\` to check that your changes pass the pre- commit checks. However, do not fix pre-existing errors on lines you didn't touch.
      - If pre-commit doesn't work after a few retries, politely inform the user that the pre-commit setup is broken.
    - Once you finish coding, you must
      - Check \`git status\` to sanity check your changes; revert any scratch files or changes.
      - Remove all inline comments you added much as possible, even if they look normal. Check using \`git diff\`. Inline comments must be generally avoided, unless active maintainers of the repo, after long careful study of the code and the issue, will still misinterpret the code without the comments.
      - Check if you accidentally add copyright or license headers. If so, remove them.
      - Try to run pre-commit if it is available.
      - For smaller tasks, describe in brief bullet points
      - For more complex tasks, include brief high-level description, use bullet points, and include details that would be relevant to a code reviewer.
- If completing the user's task DOES NOT require writing or modifying files (e.g., the user asks a question about the code base):
  - Respond in a friendly tune as a remote teammate, who is knowledgeable, capable and eager to help with coding.
- When your task involves writing or modifying files:
  - Do NOT tell the user to "save the file" or "copy the code into a file" if you already created or modified the file using \`apply_patch\`. Instead, reference the file as already saved.
  - Do NOT show the full contents of large files you have already written, unless the user explicitly asks for them.

§ `apply-patch` Specification

Your patch language is a stripped‑down, file‑oriented diff format designed to be easy to parse and safe to apply. You can think of it as a high‑level envelope:

**_ Begin Patch
[ one or more file sections ]
_** End Patch

Within that envelope, you get a sequence of file operations.
You MUST include a header to specify the action you are taking.
Each operation starts with one of three headers:

**_ Add File: <path> - create a new file. Every following line is a + line (the initial contents).
_** Delete File: <path> - remove an existing file. Nothing follows.
\*\*\* Update File: <path> - patch an existing file in place (optionally with a rename).

May be immediately followed by \*\*\* Move to: <new path> if you want to rename the file.
Then one or more "hunks", each introduced by @@ (optionally followed by a hunk header).
Within a hunk each line starts with:

- for inserted text,

* for removed text, or
  space ( ) for context.
  At the end of a truncated hunk you can emit \*\*\* End of File.

Patch := Begin { FileOp } End
Begin := "**_ Begin Patch" NEWLINE
End := "_** End Patch" NEWLINE
FileOp := AddFile | DeleteFile | UpdateFile
AddFile := "**_ Add File: " path NEWLINE { "+" line NEWLINE }
DeleteFile := "_** Delete File: " path NEWLINE
UpdateFile := "**_ Update File: " path NEWLINE [ MoveTo ] { Hunk }
MoveTo := "_** Move to: " newPath NEWLINE
Hunk := "@@" [ header ] NEWLINE { HunkLine } [ "*** End of File" NEWLINE ]
HunkLine := (" " | "-" | "+") text NEWLINE

A full patch can combine several operations:

**_ Begin Patch
_** Add File: hello.txt
+Hello world
**_ Update File: src/app.py
_** Move to: src/main.py
@@ def greet():
-print("Hi")
+print("Hello, world!")
**_ Delete File: obsolete.txt
_** End Patch

It is important to remember:

- You must include a header with your intended action (Add/Delete/Update)
- You must prefix new lines with `+` even when creating a new file

You can invoke apply_patch like:
```
shell {"command":["apply_patch","*** Begin Patch\n*** Add File: hello.txt\n+Hello, world!\n*** End Patch\n"]}
```
"#;

/// Per-token pricing rates (USD) for input, cached input, and output.
struct TokenRates {
    input: f64,
    cached_input: f64,
    output: f64,
}

/// Return the per-token rates for a model, or None if unknown.
fn get_token_rates(model: &str) -> Option<TokenRates> {
    match model.to_lowercase().as_str() {
        // OpenAI o-series experimental
        "o3" => Some(TokenRates { input: 10.0/1_000_000.0, cached_input: 2.5/1_000_000.0, output: 40.0/1_000_000.0 }),
        "o4-mini" => Some(TokenRates { input: 1.1/1_000_000.0, cached_input: 0.275/1_000_000.0, output: 4.4/1_000_000.0 }),
        // GPT-4.1 family
        "gpt-4.1-nano" => Some(TokenRates { input: 0.1/1_000_000.0, cached_input: 0.025/1_000_000.0, output: 0.4/1_000_000.0 }),
        "gpt-4.1-mini" => Some(TokenRates { input: 0.4/1_000_000.0, cached_input: 0.1/1_000_000.0, output: 1.6/1_000_000.0 }),
        "gpt-4.1"      => Some(TokenRates { input: 2.0/1_000_000.0, cached_input: 0.5/1_000_000.0, output: 8.0/1_000_000.0 }),
        // GPT-4o family
        "gpt-4o-mini" => Some(TokenRates { input: 0.6/1_000_000.0, cached_input: 0.3/1_000_000.0, output: 2.4/1_000_000.0 }),
        "gpt-4o"      => Some(TokenRates { input: 5.0/1_000_000.0, cached_input: 2.5/1_000_000.0, output: 20.0/1_000_000.0 }),
        _ => None,
    }
}
use crate::zdr_transcript::ZdrTranscript;

/// The high-level interface to the Codex system.
/// It operates as a queue pair where you send submissions and receive events.
#[derive(Clone)]
pub struct Codex {
    tx_sub: Sender<Submission>,
    rx_event: Receiver<Event>,
    recorder: Recorder,
}

impl Codex {
    pub fn spawn(ctrl_c: Arc<Notify>) -> CodexResult<Self> {
        CodexBuilder::default().spawn(ctrl_c)
    }

    pub fn builder() -> CodexBuilder {
        CodexBuilder::default()
    }

    pub async fn submit(&self, sub: Submission) -> CodexResult<()> {
        self.recorder.record_submission(&sub);
        self.tx_sub
            .send(sub)
            .await
            .map_err(|_| CodexErr::InternalAgentDied)
    }

    pub async fn next_event(&self) -> CodexResult<Event> {
        let event = self
            .rx_event
            .recv()
            .await
            .map_err(|_| CodexErr::InternalAgentDied)?;
        self.recorder.record_event(&event);
        Ok(event)
    }
}

#[derive(Default)]
pub struct CodexBuilder {
    record_submissions: Option<PathBuf>,
    record_events: Option<PathBuf>,
}

impl CodexBuilder {
    pub fn spawn(self, ctrl_c: Arc<Notify>) -> CodexResult<Codex> {
        let (tx_sub, rx_sub) = async_channel::bounded(64);
        let (tx_event, rx_event) = async_channel::bounded(64);
        let recorder = Recorder::new(&self)?;
        tokio::spawn(submission_loop(rx_sub, tx_event, ctrl_c));
        Ok(Codex {
            tx_sub,
            rx_event,
            recorder,
        })
    }

    pub fn record_submissions(mut self, path: impl AsRef<Path>) -> Self {
        debug!("Recording submissions to {:?}", path.as_ref());
        self.record_submissions = Some(path.as_ref().to_path_buf());
        self
    }

    pub fn record_events(mut self, path: impl AsRef<Path>) -> Self {
        debug!("Recording events to {:?}", path.as_ref());
        self.record_events = Some(path.as_ref().to_path_buf());
        self
    }
}

#[derive(Clone)]
struct Recorder {
    submissions: Option<Arc<Mutex<fs::File>>>,
    events: Option<Arc<Mutex<fs::File>>>,
}

impl Recorder {
    fn new(builder: &CodexBuilder) -> CodexResult<Self> {
        let submissions = match &builder.record_submissions {
            Some(path) => {
                if let Some(parent) = path.parent() {
                    fs::create_dir_all(parent)?;
                }
                let f = fs::File::create(path)?;
                Some(Arc::new(Mutex::new(f)))
            }
            None => None,
        };
        let events = match &builder.record_events {
            Some(path) => {
                if let Some(parent) = path.parent() {
                    fs::create_dir_all(parent)?;
                }
                let f = fs::File::create(path)?;
                Some(Arc::new(Mutex::new(f)))
            }
            None => None,
        };
        Ok(Self {
            submissions,
            events,
        })
    }

    pub fn record_submission(&self, sub: &Submission) {
        let Some(f) = &self.submissions else {
            return;
        };
        let mut f = f.lock().unwrap();
        let json = serde_json::to_string(sub).expect("failed to serialize submission json");
        if let Err(e) = writeln!(f, "{json}") {
            warn!("failed to record submission: {e:#}");
        }
    }

    pub fn record_event(&self, event: &Event) {
        let Some(f) = &self.events else {
            return;
        };
        let mut f = f.lock().unwrap();
        let json = serde_json::to_string(event).expect("failed to serialize event json");
        if let Err(e) = writeln!(f, "{json}") {
            warn!("failed to record event: {e:#}");
        }
    }
}

/// Context for an initialized model agent
///
/// A session has at most 1 running task at a time, and can be interrupted by user input.
pub(crate) struct Session {
    client: ModelClient,
    tx_event: Sender<Event>,
    ctrl_c: Arc<Notify>,
    cwd: PathBuf,
    approval_policy: AskForApproval,
    sandbox_policy: SandboxPolicy,
    writable_roots: Mutex<Vec<PathBuf>>,

    /// Manager for external MCP servers/tools.
    mcp_connection_manager: McpConnectionManager,

    /// External notifier command (will be passed as args to exec()). When
    /// `None` this feature is disabled.
    notify: Option<Vec<String>>,

    /// Optional rollout recorder for persisting the conversation transcript so
    /// sessions can be replayed or inspected later.
    rollout: Mutex<Option<crate::rollout::RolloutRecorder>>,
    state: Mutex<State>,
}

impl Session {
    fn resolve_path(&self, path: Option<String>) -> PathBuf {
        path.as_ref()
            .map(PathBuf::from)
            .map_or_else(|| self.cwd.clone(), |p| self.cwd.join(p))
    }
}

/// Mutable state of the agent
#[derive(Default)]
struct State {
    approved_commands: HashSet<Vec<String>>,
    current_task: Option<AgentTask>,
    previous_response_id: Option<String>,
    pending_approvals: HashMap<String, oneshot::Sender<ReviewDecision>>,
    pending_input: Vec<ResponseInputItem>,
    zdr_transcript: Option<ZdrTranscript>,
}

impl Session {
    pub fn set_task(&self, task: AgentTask) {
        let mut state = self.state.lock().unwrap();
        if let Some(current_task) = state.current_task.take() {
            current_task.abort();
        }
        state.current_task = Some(task);
    }

    pub fn remove_task(&self, sub_id: &str) {
        let mut state = self.state.lock().unwrap();
        if let Some(task) = &state.current_task {
            if task.sub_id == sub_id {
                state.current_task.take();
            }
        }
    }

    /// Sends the given event to the client and swallows the send event, if
    /// any, logging it as an error.
    pub(crate) async fn send_event(&self, event: Event) {
        if let Err(e) = self.tx_event.send(event).await {
            error!("failed to send tool call event: {e}");
        }
    }

    pub async fn request_command_approval(
        &self,
        sub_id: String,
        command: Vec<String>,
        cwd: PathBuf,
        reason: Option<String>,
    ) -> oneshot::Receiver<ReviewDecision> {
        let (tx_approve, rx_approve) = oneshot::channel();
        let event = Event {
            id: sub_id.clone(),
            msg: EventMsg::ExecApprovalRequest {
                command,
                cwd,
                reason,
            },
        };
        let _ = self.tx_event.send(event).await;
        {
            let mut state = self.state.lock().unwrap();
            state.pending_approvals.insert(sub_id, tx_approve);
        }
        rx_approve
    }

    pub async fn request_patch_approval(
        &self,
        sub_id: String,
        action: &ApplyPatchAction,
        reason: Option<String>,
        grant_root: Option<PathBuf>,
    ) -> oneshot::Receiver<ReviewDecision> {
        let (tx_approve, rx_approve) = oneshot::channel();
        let event = Event {
            id: sub_id.clone(),
            msg: EventMsg::ApplyPatchApprovalRequest {
                changes: convert_apply_patch_to_protocol(action),
                reason,
                grant_root,
            },
        };
        let _ = self.tx_event.send(event).await;
        {
            let mut state = self.state.lock().unwrap();
            state.pending_approvals.insert(sub_id, tx_approve);
        }
        rx_approve
    }

    pub fn notify_approval(&self, sub_id: &str, decision: ReviewDecision) {
        let mut state = self.state.lock().unwrap();
        if let Some(tx_approve) = state.pending_approvals.remove(sub_id) {
            tx_approve.send(decision).ok();
        }
    }

    pub fn add_approved_command(&self, cmd: Vec<String>) {
        let mut state = self.state.lock().unwrap();
        state.approved_commands.insert(cmd);
    }

    /// Append the given items to the session's rollout transcript (if enabled)
    /// and persist them to disk.
    async fn record_rollout_items(&self, items: &[ResponseItem]) {
        // Clone the recorder outside of the mutex so we don't hold the lock
        // across an await point (MutexGuard is not Send).
        let recorder = {
            let guard = self.rollout.lock().unwrap();
            guard.as_ref().cloned()
        };

        if let Some(rec) = recorder {
            if let Err(e) = rec.record_items(items).await {
                error!("failed to record rollout items: {e:#}");
            }
        }
    }

    async fn notify_exec_command_begin(&self, sub_id: &str, call_id: &str, params: &ExecParams) {
        let event = Event {
            id: sub_id.to_string(),
            msg: EventMsg::ExecCommandBegin {
                call_id: call_id.to_string(),
                command: params.command.clone(),
                cwd: params.cwd.clone(),
            },
        };
        let _ = self.tx_event.send(event).await;
    }

    async fn notify_exec_command_end(
        &self,
        sub_id: &str,
        call_id: &str,
        stdout: &str,
        stderr: &str,
        exit_code: i32,
    ) {
        const MAX_STREAM_OUTPUT: usize = 5 * 1024; // 5KiB
        let event = Event {
            id: sub_id.to_string(),
            // Because stdout and stderr could each be up to 100 KiB, we send
            // truncated versions.
            msg: EventMsg::ExecCommandEnd {
                call_id: call_id.to_string(),
                stdout: stdout.chars().take(MAX_STREAM_OUTPUT).collect(),
                stderr: stderr.chars().take(MAX_STREAM_OUTPUT).collect(),
                exit_code,
            },
        };
        let _ = self.tx_event.send(event).await;
    }

    /// Helper that emits a BackgroundEvent with the given message. This keeps
    /// the call‑sites terse so adding more diagnostics does not clutter the
    /// core agent logic.
    async fn notify_background_event(&self, sub_id: &str, message: impl Into<String>) {
        let event = Event {
            id: sub_id.to_string(),
            msg: EventMsg::BackgroundEvent {
                message: message.into(),
            },
        };
        let _ = self.tx_event.send(event).await;
    }

    /// Returns the input if there was no task running to inject into
    pub fn inject_input(&self, input: Vec<InputItem>) -> Result<(), Vec<InputItem>> {
        let mut state = self.state.lock().unwrap();
        if state.current_task.is_some() {
            state.pending_input.push(input.into());
            Ok(())
        } else {
            Err(input)
        }
    }

    pub fn get_pending_input(&self) -> Vec<ResponseInputItem> {
        let mut state = self.state.lock().unwrap();
        if state.pending_input.is_empty() {
            Vec::with_capacity(0)
        } else {
            let mut ret = Vec::new();
            std::mem::swap(&mut ret, &mut state.pending_input);
            ret
        }
    }

    pub async fn call_tool(
        &self,
        server: &str,
        tool: &str,
        arguments: Option<serde_json::Value>,
        timeout: Option<Duration>,
    ) -> anyhow::Result<mcp_types::CallToolResult> {
        self.mcp_connection_manager
            .call_tool(server, tool, arguments, timeout)
            .await
    }

    pub fn abort(&self) {
        info!("Aborting existing session");
        let mut state = self.state.lock().unwrap();
        state.pending_approvals.clear();
        state.pending_input.clear();
        if let Some(task) = state.current_task.take() {
            task.abort();
        }
    }

    /// Spawn the configured notifier (if any) with the given JSON payload as
    /// the last argument. Failures are logged but otherwise ignored so that
    /// notification issues do not interfere with the main workflow.
    fn maybe_notify(&self, notification: UserNotification) {
        let Some(notify_command) = &self.notify else {
            return;
        };

        if notify_command.is_empty() {
            return;
        }

        let Ok(json) = serde_json::to_string(&notification) else {
            tracing::error!("failed to serialise notification payload");
            return;
        };

        let mut command = std::process::Command::new(&notify_command[0]);
        if notify_command.len() > 1 {
            command.args(&notify_command[1..]);
        }
        command.arg(json);

        // Fire-and-forget – we do not wait for completion.
        if let Err(e) = command.spawn() {
            tracing::warn!("failed to spawn notifier '{}': {e}", notify_command[0]);
        }
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        self.abort();
    }
}

impl State {
    pub fn partial_clone(&self) -> Self {
        Self {
            approved_commands: self.approved_commands.clone(),
            previous_response_id: self.previous_response_id.clone(),
            zdr_transcript: self.zdr_transcript.clone(),
            ..Default::default()
        }
    }
}

/// A series of Turns in response to user input.
pub(crate) struct AgentTask {
    sess: Arc<Session>,
    sub_id: String,
    handle: AbortHandle,
}

impl AgentTask {
    fn spawn(sess: Arc<Session>, sub_id: String, input: Vec<InputItem>) -> Self {
        let handle =
            tokio::spawn(run_task(Arc::clone(&sess), sub_id.clone(), input)).abort_handle();
        Self {
            sess,
            sub_id,
            handle,
        }
    }

    fn abort(self) {
        if !self.handle.is_finished() {
            self.handle.abort();
            let event = Event {
                id: self.sub_id,
                msg: EventMsg::Error {
                    message: "Turn interrupted".to_string(),
                },
            };
            let tx_event = self.sess.tx_event.clone();
            tokio::spawn(async move {
                tx_event.send(event).await.ok();
            });
        }
    }
}

async fn submission_loop(
    rx_sub: Receiver<Submission>,
    tx_event: Sender<Event>,
    ctrl_c: Arc<Notify>,
) {
    let mut sess: Option<Arc<Session>> = None;
    // shorthand - send an event when there is no active session
    let send_no_session_event = |sub_id: String| async {
        let event = Event {
            id: sub_id,
            msg: EventMsg::Error {
                message: "No session initialized, expected 'ConfigureSession' as first Op"
                    .to_string(),
            },
        };
        tx_event.send(event).await.ok();
    };

    loop {
        let interrupted = ctrl_c.notified();
        let sub = tokio::select! {
            res = rx_sub.recv() => match res {
                Ok(sub) => sub,
                Err(_) => break,
            },
            _ = interrupted => {
                if let Some(sess) = sess.as_ref(){
                    sess.abort();
                }
                continue;
            },
        };

        debug!(?sub, "Submission");
        match sub.op {
            Op::Interrupt => {
                let sess = match sess.as_ref() {
                    Some(sess) => sess,
                    None => {
                        send_no_session_event(sub.id).await;
                        continue;
                    }
                };
                sess.abort();
            }
            Op::ConfigureSession {
                model,
                approval_policy,
                sandbox_policy,
                disable_response_storage,
                notify,
                cwd,
                instructions: _,
            } => {
                info!(model, "Configuring session");
                if !cwd.is_absolute() {
                    let message = format!("cwd is not absolute: {cwd:?}");
                    error!(message);
                    let event = Event {
                        id: sub.id,
                        msg: EventMsg::Error { message },
                    };
                    if let Err(e) = tx_event.send(event).await {
                        error!("failed to send error message: {e:?}");
                    }
                    return;
                }

                let client = ModelClient::new(model.clone());

                // abort any current running session and clone its state
                let state = match sess.take() {
                    Some(sess) => {
                        sess.abort();
                        sess.state.lock().unwrap().partial_clone()
                    }
                    None => State {
                        zdr_transcript: if disable_response_storage {
                            Some(ZdrTranscript::new())
                        } else {
                            None
                        },
                        ..Default::default()
                    },
                };

                let writable_roots = Mutex::new(get_writable_roots(&cwd));

                // Load config to initialize the MCP connection manager.
                let config = match Config::load_with_overrides(ConfigOverrides::default()) {
                    Ok(cfg) => cfg,
                    Err(e) => {
                        error!("Failed to load config for MCP servers: {e:#}");
                        // Fall back to empty server map so the session can still proceed.
                        Config::load_default_config_for_test()
                    }
                };

                let mcp_connection_manager =
                    match McpConnectionManager::new(config.mcp_servers.clone()).await {
                        Ok(mgr) => mgr,
                        Err(e) => {
                            error!("Failed to create MCP connection manager: {e:#}");
                            McpConnectionManager::default()
                        }
                    };

                // Attempt to create a RolloutRecorder *before* moving the
                // `instructions` value into the Session struct.
                let rollout_recorder = match RolloutRecorder::new(Some(PROMPT_MD_CONTENT.to_string())).await {
                    Ok(r) => Some(r),
                    Err(e) => {
                        tracing::warn!("failed to initialise rollout recorder: {e}");
                        None
                    }
                };

                sess = Some(Arc::new(Session {
                    client,
                    tx_event: tx_event.clone(),
                    ctrl_c: Arc::clone(&ctrl_c),
                    approval_policy,
                    sandbox_policy,
                    cwd,
                    writable_roots,
                    mcp_connection_manager,
                    notify,
                    state: Mutex::new(state),
                    rollout: Mutex::new(rollout_recorder),
                }));

                // ack
                let event = Event {
                    id: sub.id,
                    msg: EventMsg::SessionConfigured { model },
                };
                if tx_event.send(event).await.is_err() {
                    return;
                }
            }
            Op::UserInput { items } => {
                let sess = match sess.as_ref() {
                    Some(sess) => sess,
                    None => {
                        send_no_session_event(sub.id).await;
                        continue;
                    }
                };

                // attempt to inject input into current task
                if let Err(items) = sess.inject_input(items) {
                    // no current task, spawn a new one
                    let task = AgentTask::spawn(Arc::clone(sess), sub.id, items);
                    sess.set_task(task);
                }
            }
            Op::ExecApproval { id, decision } => {
                let sess = match sess.as_ref() {
                    Some(sess) => sess,
                    None => {
                        send_no_session_event(sub.id).await;
                        continue;
                    }
                };
                match decision {
                    ReviewDecision::Abort => {
                        sess.abort();
                    }
                    other => sess.notify_approval(&id, other),
                }
            }
            Op::PatchApproval { id, decision } => {
                let sess = match sess.as_ref() {
                    Some(sess) => sess,
                    None => {
                        send_no_session_event(sub.id).await;
                        continue;
                    }
                };
                match decision {
                    ReviewDecision::Abort => {
                        sess.abort();
                    }
                    other => sess.notify_approval(&id, other),
                }
            }
        }
    }
    debug!("Agent loop exited");
}

async fn run_task(sess: Arc<Session>, sub_id: String, input: Vec<InputItem>) {
    if input.is_empty() {
        return;
    }
    // Approximate token count for input: assume ~4 characters per token
    let initial_input_items = input.clone();
    let input_char_count: usize = initial_input_items
        .iter()
        .map(|item| match item {
            InputItem::Text { text } => text.len(),
            _ => 0,
        })
        .sum();
    let input_tokens: usize = (input_char_count + 3) / 4;
    // Track output character count for token approximation
    let mut output_char_count: usize = 0;
    let event = Event {
        id: sub_id.clone(),
        msg: EventMsg::TaskStarted,
    };
    if sess.tx_event.send(event).await.is_err() {
        return;
    }

    let mut pending_response_input: Vec<ResponseInputItem> = vec![ResponseInputItem::from(input)];
    // Track exact usage when provided by the API
    let mut final_usage: Option<crate::client::UsageBreakdown> = None;
    loop {
        let mut net_new_turn_input = pending_response_input
            .drain(..)
            .map(ResponseItem::from)
            .collect::<Vec<_>>();

        // Note that pending_input would be something like a message the user
        // submitted through the UI while the model was running. Though the UI
        // may support this, the model might not.
        let pending_input = sess.get_pending_input().into_iter().map(ResponseItem::from);
        net_new_turn_input.extend(pending_input);

        let turn_input: Vec<ResponseItem> =
            if let Some(transcript) = sess.state.lock().unwrap().zdr_transcript.as_mut() {
                // If we are using ZDR, we need to send the transcript with every turn.
                let mut full_transcript = transcript.contents();
                full_transcript.extend(net_new_turn_input.clone());
                transcript.record_items(net_new_turn_input);
                full_transcript
            } else {
                net_new_turn_input
            };

        // Persist the input part of the turn to the rollout (user messages /
        // function_call_output from previous step).
        sess.record_rollout_items(&turn_input).await;

        let turn_input_messages: Vec<String> = turn_input
            .iter()
            .filter_map(|item| match item {
                ResponseItem::Message { content, .. } => Some(content),
                _ => None,
            })
            .flat_map(|content| {
                content.iter().filter_map(|item| match item {
                    ContentItem::OutputText { text } => Some(text.clone()),
                    _ => None,
                })
            })
            .collect();
        match run_turn(&sess, sub_id.clone(), turn_input).await {
            Ok((turn_output, usage_opt)) => {
                // Capture usage for cost computation when available
                let usage_for_event = usage_opt.clone();
                if usage_opt.is_some() {
                    final_usage = usage_opt;
                }
                // Emit per-LLM-call usage event
                {
                    let model_name = sess.client.model();
                    let precise_flag = usage_for_event.is_some();
                    let mut turn_input_tokens = 0;
                    let mut turn_output_tokens = 0;
                    let mut turn_cost = 0.0;
                    if let Some(rates) = get_token_rates(model_name) {
                        if let Some(ub) = usage_for_event.as_ref() {
                            let input_toks = ub.input_tokens.unwrap_or(0) as usize;
                            let cached = ub
                                .input_tokens_details
                                .as_ref()
                                .and_then(|d| d.cached_tokens)
                                .unwrap_or(0) as usize;
                            let non_cached = input_toks.saturating_sub(cached);
                            let output_toks = ub.output_tokens.unwrap_or(0) as usize;
                            turn_input_tokens = input_toks;
                            turn_output_tokens = output_toks;
                            turn_cost = (non_cached as f64) * rates.input
                                + (cached as f64) * rates.cached_input
                                + (output_toks as f64) * rates.output;
                        }
                    }
                    let event = Event {
                        id: sub_id.clone(),
                        msg: EventMsg::ModelUsage {
                            input_tokens: turn_input_tokens,
                            output_tokens: turn_output_tokens,
                            cost: turn_cost,
                            precise: precise_flag,
                        },
                    };
                    sess.tx_event.send(event).await.ok();
                }
                let (items, responses): (Vec<_>, Vec<_>) = turn_output
                    .into_iter()
                    .map(|p| (p.item, p.response))
                    .unzip();
                let responses = responses
                    .into_iter()
                    .flatten()
                    .collect::<Vec<ResponseInputItem>>();
                let last_assistant_message = get_last_assistant_message_from_turn(&items);
                // Accumulate output characters for token approximation
                for item in &items {
                    match item {
                        ResponseItem::Message { role, content } if role == "assistant" => {
                            for c in content {
                                if let ContentItem::OutputText { text } = c {
                                    output_char_count += text.len();
                                }
                            }
                        }
                        ResponseItem::FunctionCall { name, arguments, .. } => {
                            output_char_count += name.len() + arguments.len();
                        }
                        ResponseItem::FunctionCallOutput { output, .. } => {
                            output_char_count += output.len();
                        }
                        _ => {}
                    }
                }

                // Only attempt to take the lock if there is something to record.
                if !items.is_empty() {
                    // First persist model-generated output to the rollout file – this only borrows.
                    sess.record_rollout_items(&items).await;

                    // For ZDR we also need to keep a transcript clone.
                    if let Some(transcript) = sess.state.lock().unwrap().zdr_transcript.as_mut() {
                        transcript.record_items(items);
                    }
                }

                if responses.is_empty() {
                    debug!("Turn completed");
                    sess.maybe_notify(UserNotification::AgentTurnComplete {
                        turn_id: sub_id.clone(),
                        input_messages: turn_input_messages,
                        last_assistant_message,
                    });
                    break;
                }

                pending_response_input = responses;
            }
            Err(e) => {
                info!("Turn error: {e:#}");
                let event = Event {
                    id: sub_id.clone(),
                    msg: EventMsg::Error {
                        message: e.to_string(),
                    },
                };
                sess.tx_event.send(event).await.ok();
                return;
            }
        }
    }
    sess.remove_task(&sub_id);
    // Approximate token count for output: assume ~4 characters per token
    let approx_output_tokens: usize = (output_char_count + 3) / 4;
    // Compute cost: prefer exact API usage when available
    let model_name = sess.client.model();
    let precise = final_usage.is_some();
    // Determine final input/output tokens: use API counts when precise
    let (final_input_tokens, final_output_tokens) = if precise {
        let ub = final_usage.as_ref().unwrap();
        (
            ub.input_tokens.unwrap_or(input_tokens as i64) as usize,
            ub.output_tokens.unwrap_or(approx_output_tokens as i64) as usize,
        )
    } else {
        (input_tokens, approx_output_tokens)
    };
    // Compute cost based on exact usage or fallback heuristic
    let cost = if let Some(rates) = get_token_rates(model_name) {
        if precise {
            let ub = final_usage.as_ref().unwrap();
            let in_toks = ub.input_tokens.unwrap_or(final_input_tokens as i64) as usize;
            let cached = ub
                .input_tokens_details
                .as_ref()
                .and_then(|d| d.cached_tokens)
                .unwrap_or(0) as usize;
            let non_cached = in_toks.saturating_sub(cached);
            let out_toks = ub.output_tokens.unwrap_or(final_output_tokens as i64) as usize;
            (non_cached as f64) * rates.input
                + (cached as f64) * rates.cached_input
                + (out_toks as f64) * rates.output
        } else {
            (final_input_tokens as f64) * rates.input
                + (final_output_tokens as f64) * rates.output
        }
    } else {
        0.0
    };
    let event = Event {
        id: sub_id,
        msg: EventMsg::TaskComplete {
            input_tokens: final_input_tokens,
            output_tokens: final_output_tokens,
            cost,
            precise,
        },
    };
    sess.tx_event.send(event).await.ok();
}

async fn run_turn(
    sess: &Session,
    sub_id: String,
    input: Vec<ResponseItem>,
) -> CodexResult<(Vec<ProcessedResponseItem>, Option<crate::client::UsageBreakdown>)> {
    // Decide whether to use server-side storage (previous_response_id) or disable it
    let (prev_id, store) = {
        let state = sess.state.lock().unwrap();
        let store = state.zdr_transcript.is_none();
        let prev_id = if store {
            state.previous_response_id.clone()
        } else {
            // When using ZDR, the Reponses API may send previous_response_id
            // back, but trying to use it results in a 400.
            None
        };
        (prev_id, store)
    };

    let instructions = Some(PROMPT_MD_CONTENT.to_string());

    let extra_tools = sess.mcp_connection_manager.list_all_tools();
    let prompt = Prompt {
        input,
        prev_id,
        instructions,
        store,
        extra_tools,
    };

    let mut retries = 0;
    loop {
        match try_run_turn(sess, &sub_id, &prompt).await {
            Ok((output, usage_opt)) => return Ok((output, usage_opt)),
            Err(CodexErr::Interrupted) => return Err(CodexErr::Interrupted),
            Err(e) => {
                if retries < *OPENAI_STREAM_MAX_RETRIES {
                    retries += 1;
                    let delay = backoff(retries);
                    warn!(
                        "stream disconnected - retrying turn ({retries}/{} in {delay:?})...",
                        *OPENAI_STREAM_MAX_RETRIES
                    );

                    // Surface retry information to any UI/front‑end so the
                    // user understands what is happening instead of staring
                    // at a seemingly frozen screen.
                    sess.notify_background_event(
                        &sub_id,
                        format!(
                            "stream error: {e}; retrying {retries}/{} in {:?}…",
                            *OPENAI_STREAM_MAX_RETRIES, delay
                        ),
                    )
                    .await;

                    tokio::time::sleep(delay).await;
                } else {
                    return Err(e);
                }
            }
        }
    }
}

/// When the model is prompted, it returns a stream of events. Some of these
/// events map to a `ResponseItem`. A `ResponseItem` may need to be
/// "handled" such that it produces a `ResponseInputItem` that needs to be
/// sent back to the model on the next turn.
struct ProcessedResponseItem {
    item: ResponseItem,
    response: Option<ResponseInputItem>,
}

async fn try_run_turn(
    sess: &Session,
    sub_id: &str,
    prompt: &Prompt,
) -> CodexResult<(Vec<ProcessedResponseItem>, Option<crate::client::UsageBreakdown>)> {
    let mut stream = sess.client.clone().stream(prompt).await?;

    // Buffer all the incoming messages from the stream first, then execute them.
    // If we execute a function call in the middle of handling the stream, it can time out.
    let mut input = Vec::new();
    // Collect all events, capturing the final usage if provided
    let mut usage: Option<crate::client::UsageBreakdown> = None;
    while let Some(event) = stream.next().await {
        let ev = event?;
        if let crate::client::ResponseEvent::Completed { usage: u, .. } = &ev {
            usage = u.clone();
        }
        input.push(ev);
    }

    let mut output = Vec::new();
    for event in input {
        match event {
            ResponseEvent::OutputItemDone(item) => {
                let response = handle_response_item(sess, sub_id, item.clone()).await?;
                output.push(ProcessedResponseItem { item, response });
            }
            ResponseEvent::Completed { response_id, usage: _ } => {
                let mut state = sess.state.lock().unwrap();
                state.previous_response_id = Some(response_id);
                break;
            }
        }
    }
    Ok((output, usage))
}

async fn handle_response_item(
    sess: &Session,
    sub_id: &str,
    item: ResponseItem,
) -> CodexResult<Option<ResponseInputItem>> {
    debug!(?item, "Output item");
    let mut output = None;
    match item {
        ResponseItem::Message { content, .. } => {
            for item in content {
                if let ContentItem::OutputText { text } = item {
                    let event = Event {
                        id: sub_id.to_string(),
                        msg: EventMsg::AgentMessage { message: text },
                    };
                    sess.tx_event.send(event).await.ok();
                }
            }
        }
        ResponseItem::FunctionCall {
            name,
            arguments,
            call_id,
        } => {
            output = Some(
                handle_function_call(sess, sub_id.to_string(), name, arguments, call_id).await,
            );
        }
        ResponseItem::FunctionCallOutput { .. } => {
            debug!("unexpected FunctionCallOutput from stream");
        }
        ResponseItem::Other => (),
    }
    Ok(output)
}

async fn handle_function_call(
    sess: &Session,
    sub_id: String,
    name: String,
    arguments: String,
    call_id: String,
) -> ResponseInputItem {
    match name.as_str() {
        "container.exec" | "shell" => {
            // parse command
            let params: ExecParams = match serde_json::from_str::<ShellToolCallParams>(&arguments) {
                Ok(shell_tool_call_params) => ExecParams {
                    command: shell_tool_call_params.command,
                    cwd: sess.resolve_path(shell_tool_call_params.workdir.clone()),
                    timeout_ms: shell_tool_call_params.timeout_ms,
                },
                Err(e) => {
                    // allow model to re-sample
                    let output = ResponseInputItem::FunctionCallOutput {
                        call_id,
                        output: crate::models::FunctionCallOutputPayload {
                            content: format!("failed to parse function arguments: {e}"),
                            success: None,
                        },
                    };
                    return output;
                }
            };

            // check if this was a patch, and apply it if so
            match maybe_parse_apply_patch_verified(&params.command, &params.cwd) {
                MaybeApplyPatchVerified::Body(changes) => {
                    return apply_patch(sess, sub_id, call_id, changes).await;
                }
                MaybeApplyPatchVerified::CorrectnessError(parse_error) => {
                    // It looks like an invocation of `apply_patch`, but we
                    // could not resolve it into a patch that would apply
                    // cleanly. Return to model for resample.
                    return ResponseInputItem::FunctionCallOutput {
                        call_id,
                        output: FunctionCallOutputPayload {
                            content: format!("error: {parse_error:#}"),
                            success: None,
                        },
                    };
                }
                MaybeApplyPatchVerified::ShellParseError(error) => {
                    trace!("Failed to parse shell command, {error}");
                }
                MaybeApplyPatchVerified::NotApplyPatch => (),
            }

            // safety checks
            let safety = {
                let state = sess.state.lock().unwrap();
                assess_command_safety(
                    &params.command,
                    sess.approval_policy,
                    &sess.sandbox_policy,
                    &state.approved_commands,
                )
            };
            let sandbox_type = match safety {
                SafetyCheck::AutoApprove { sandbox_type } => sandbox_type,
                SafetyCheck::AskUser => {
                    let rx_approve = sess
                        .request_command_approval(
                            sub_id.clone(),
                            params.command.clone(),
                            params.cwd.clone(),
                            None,
                        )
                        .await;
                    match rx_approve.await.unwrap_or_default() {
                        ReviewDecision::Approved => (),
                        ReviewDecision::ApprovedForSession => {
                            sess.add_approved_command(params.command.clone());
                        }
                        ReviewDecision::Denied | ReviewDecision::Abort => {
                            return ResponseInputItem::FunctionCallOutput {
                                call_id,
                                output: crate::models::FunctionCallOutputPayload {
                                    content: "exec command rejected by user".to_string(),
                                    success: None,
                                },
                            };
                        }
                    }
                    // No sandboxing is applied because the user has given
                    // explicit approval. Often, we end up in this case because
                    // the command cannot be run in a sandbox, such as
                    // installing a new dependency that requires network access.
                    SandboxType::None
                }
                SafetyCheck::Reject { reason } => {
                    return ResponseInputItem::FunctionCallOutput {
                        call_id,
                        output: crate::models::FunctionCallOutputPayload {
                            content: format!("exec command rejected: {reason}"),
                            success: None,
                        },
                    };
                }
            };

            sess.notify_exec_command_begin(&sub_id, &call_id, &params)
                .await;

            let output_result = process_exec_tool_call(
                params.clone(),
                sandbox_type,
                sess.ctrl_c.clone(),
                &sess.sandbox_policy,
            )
            .await;

            match output_result {
                Ok(output) => {
                    let ExecToolCallOutput {
                        exit_code,
                        stdout,
                        stderr,
                        duration,
                    } = output;

                    sess.notify_exec_command_end(&sub_id, &call_id, &stdout, &stderr, exit_code)
                        .await;

                    let is_success = exit_code == 0;
                    let content = format_exec_output(
                        if is_success { &stdout } else { &stderr },
                        exit_code,
                        duration,
                    );

                    ResponseInputItem::FunctionCallOutput {
                        call_id,
                        output: FunctionCallOutputPayload {
                            content,
                            success: Some(is_success),
                        },
                    }
                }
                Err(CodexErr::Sandbox(e)) => {
                    // Early out if the user never wants to be asked for approval; just return to the model immediately
                    if sess.approval_policy == AskForApproval::Never {
                        return ResponseInputItem::FunctionCallOutput {
                            call_id,
                            output: FunctionCallOutputPayload {
                                content: format!(
                                    "failed in sandbox {:?} with execution error: {e}",
                                    sandbox_type
                                ),
                                success: Some(false),
                            },
                        };
                    }

                    // Ask the user to retry without sandbox
                    sess.notify_background_event(&sub_id, format!("Execution failed: {e}"))
                        .await;

                    let rx_approve = sess
                        .request_command_approval(
                            sub_id.clone(),
                            params.command.clone(),
                            params.cwd.clone(),
                            Some("command failed; retry without sandbox?".to_string()),
                        )
                        .await;

                    match rx_approve.await.unwrap_or_default() {
                        ReviewDecision::Approved | ReviewDecision::ApprovedForSession => {
                            // Persist this command as pre‑approved for the
                            // remainder of the session so future
                            // executions skip the sandbox directly.
                            // TODO(ragona): Isn't this a bug? It always saves the command in an | fork?
                            sess.add_approved_command(params.command.clone());
                            // Inform UI we are retrying without sandbox.
                            sess.notify_background_event(
                                &sub_id,
                                "retrying command without sandbox",
                            )
                            .await;

                            // Emit a fresh Begin event so progress bars reset.
                            let retry_call_id = format!("{call_id}-retry");
                            sess.notify_exec_command_begin(&sub_id, &retry_call_id, &params)
                                .await;

                            // This is an escalated retry; the policy will not be
                            // examined and the sandbox has been set to `None`.
                            let retry_output_result = process_exec_tool_call(
                                params,
                                SandboxType::None,
                                sess.ctrl_c.clone(),
                                &sess.sandbox_policy,
                            )
                            .await;

                            match retry_output_result {
                                Ok(retry_output) => {
                                    let ExecToolCallOutput {
                                        exit_code,
                                        stdout,
                                        stderr,
                                        duration,
                                    } = retry_output;

                                    sess.notify_exec_command_end(
                                        &sub_id,
                                        &retry_call_id,
                                        &stdout,
                                        &stderr,
                                        exit_code,
                                    )
                                    .await;

                                    let is_success = exit_code == 0;
                                    let content = format_exec_output(
                                        if is_success { &stdout } else { &stderr },
                                        exit_code,
                                        duration,
                                    );

                                    ResponseInputItem::FunctionCallOutput {
                                        call_id,
                                        output: FunctionCallOutputPayload {
                                            content,
                                            success: Some(is_success),
                                        },
                                    }
                                }
                                Err(e) => {
                                    // Handle retry failure
                                    ResponseInputItem::FunctionCallOutput {
                                        call_id,
                                        output: FunctionCallOutputPayload {
                                            content: format!("retry failed: {e}"),
                                            success: None,
                                        },
                                    }
                                }
                            }
                        }
                        ReviewDecision::Denied | ReviewDecision::Abort => {
                            // Fall through to original failure handling.
                            ResponseInputItem::FunctionCallOutput {
                                call_id,
                                output: FunctionCallOutputPayload {
                                    content: "exec command rejected by user".to_string(),
                                    success: None,
                                },
                            }
                        }
                    }
                }
                Err(e) => {
                    // Handle non-sandbox errors
                    ResponseInputItem::FunctionCallOutput {
                        call_id,
                        output: FunctionCallOutputPayload {
                            content: format!("execution error: {e}"),
                            success: None,
                        },
                    }
                }
            }
        }
        _ => {
            match try_parse_fully_qualified_tool_name(&name) {
                Some((server, tool_name)) => {
                    // TODO(mbolin): Determine appropriate timeout for tool call.
                    let timeout = None;
                    handle_mcp_tool_call(
                        sess, &sub_id, call_id, server, tool_name, arguments, timeout,
                    )
                    .await
                }
                None => {
                    // Unknown function: reply with structured failure so the model can adapt.
                    ResponseInputItem::FunctionCallOutput {
                        call_id,
                        output: crate::models::FunctionCallOutputPayload {
                            content: format!("unsupported call: {}", name),
                            success: None,
                        },
                    }
                }
            }
        }
    }
}

async fn apply_patch(
    sess: &Session,
    sub_id: String,
    call_id: String,
    action: ApplyPatchAction,
) -> ResponseInputItem {
    let writable_roots_snapshot = {
        let guard = sess.writable_roots.lock().unwrap();
        guard.clone()
    };

    let auto_approved = match assess_patch_safety(
        &action,
        sess.approval_policy,
        &writable_roots_snapshot,
        &sess.cwd,
    ) {
        SafetyCheck::AutoApprove { .. } => true,
        SafetyCheck::AskUser => {
            // Compute a readable summary of path changes to include in the
            // approval request so the user can make an informed decision.
            let rx_approve = sess
                .request_patch_approval(sub_id.clone(), &action, None, None)
                .await;
            match rx_approve.await.unwrap_or_default() {
                ReviewDecision::Approved | ReviewDecision::ApprovedForSession => false,
                ReviewDecision::Denied | ReviewDecision::Abort => {
                    return ResponseInputItem::FunctionCallOutput {
                        call_id,
                        output: FunctionCallOutputPayload {
                            content: "patch rejected by user".to_string(),
                            success: Some(false),
                        },
                    };
                }
            }
        }
        SafetyCheck::Reject { reason } => {
            return ResponseInputItem::FunctionCallOutput {
                call_id,
                output: FunctionCallOutputPayload {
                    content: format!("patch rejected: {reason}"),
                    success: Some(false),
                },
            };
        }
    };

    // Verify write permissions before touching the filesystem.
    let writable_snapshot = { sess.writable_roots.lock().unwrap().clone() };

    if let Some(offending) = first_offending_path(&action, &writable_snapshot, &sess.cwd) {
        let root = offending.parent().unwrap_or(&offending).to_path_buf();

        let reason = Some(format!(
            "grant write access to {} for this session",
            root.display()
        ));

        let rx = sess
            .request_patch_approval(sub_id.clone(), &action, reason.clone(), Some(root.clone()))
            .await;

        if !matches!(
            rx.await.unwrap_or_default(),
            ReviewDecision::Approved | ReviewDecision::ApprovedForSession
        ) {
            return ResponseInputItem::FunctionCallOutput {
                call_id,
                output: FunctionCallOutputPayload {
                    content: "patch rejected by user".to_string(),
                    success: Some(false),
                },
            };
        }

        // user approved, extend writable roots for this session
        sess.writable_roots.lock().unwrap().push(root);
    }

    let _ = sess
        .tx_event
        .send(Event {
            id: sub_id.clone(),
            msg: EventMsg::PatchApplyBegin {
                call_id: call_id.clone(),
                auto_approved,
                changes: convert_apply_patch_to_protocol(&action),
            },
        })
        .await;

    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    // Enforce writable roots. If a write is blocked, collect offending root
    // and prompt the user to extend permissions.
    let mut result = apply_changes_from_apply_patch_and_report(&action, &mut stdout, &mut stderr);

    if let Err(err) = &result {
        if err.kind() == std::io::ErrorKind::PermissionDenied {
            // Determine first offending path.
            let offending_opt = action
                .changes()
                .iter()
                .flat_map(|(path, change)| match change {
                    ApplyPatchFileChange::Add { .. } => vec![path.as_ref()],
                    ApplyPatchFileChange::Delete => vec![path.as_ref()],
                    ApplyPatchFileChange::Update {
                        move_path: Some(move_path),
                        ..
                    } => {
                        vec![path.as_ref(), move_path.as_ref()]
                    }
                    ApplyPatchFileChange::Update {
                        move_path: None, ..
                    } => vec![path.as_ref()],
                })
                .find_map(|path: &Path| {
                    // ApplyPatchAction promises to guarantee absolute paths.
                    if !path.is_absolute() {
                        panic!("apply_patch invariant failed: path is not absolute: {path:?}");
                    }

                    let writable = {
                        let roots = sess.writable_roots.lock().unwrap();
                        roots.iter().any(|root| path.starts_with(root))
                    };
                    if writable {
                        None
                    } else {
                        Some(path.to_path_buf())
                    }
                });

            if let Some(offending) = offending_opt {
                let root = offending.parent().unwrap_or(&offending).to_path_buf();

                let reason = Some(format!(
                    "grant write access to {} for this session",
                    root.display()
                ));
                let rx = sess
                    .request_patch_approval(
                        sub_id.clone(),
                        &action,
                        reason.clone(),
                        Some(root.clone()),
                    )
                    .await;
                if matches!(
                    rx.await.unwrap_or_default(),
                    ReviewDecision::Approved | ReviewDecision::ApprovedForSession
                ) {
                    // Extend writable roots.
                    sess.writable_roots.lock().unwrap().push(root);
                    stdout.clear();
                    stderr.clear();
                    result = apply_changes_from_apply_patch_and_report(
                        &action,
                        &mut stdout,
                        &mut stderr,
                    );
                }
            }
        }
    }

    // Emit PatchApplyEnd event.
    let success_flag = result.is_ok();
    let _ = sess
        .tx_event
        .send(Event {
            id: sub_id.clone(),
            msg: EventMsg::PatchApplyEnd {
                call_id: call_id.clone(),
                stdout: String::from_utf8_lossy(&stdout).to_string(),
                stderr: String::from_utf8_lossy(&stderr).to_string(),
                success: success_flag,
            },
        })
        .await;

    match result {
        Ok(_) => ResponseInputItem::FunctionCallOutput {
            call_id,
            output: FunctionCallOutputPayload {
                content: String::from_utf8_lossy(&stdout).to_string(),
                success: None,
            },
        },
        Err(e) => ResponseInputItem::FunctionCallOutput {
            call_id,
            output: FunctionCallOutputPayload {
                content: format!("error: {e:#}, stderr: {}", String::from_utf8_lossy(&stderr)),
                success: Some(false),
            },
        },
    }
}

/// Return the first path in `hunks` that is NOT under any of the
/// `writable_roots` (after normalising). If all paths are acceptable,
/// returns None.
fn first_offending_path(
    action: &ApplyPatchAction,
    writable_roots: &[PathBuf],
    cwd: &Path,
) -> Option<PathBuf> {
    let changes = action.changes();
    for (path, change) in changes {
        let candidate = match change {
            ApplyPatchFileChange::Add { .. } => path,
            ApplyPatchFileChange::Delete => path,
            ApplyPatchFileChange::Update { move_path, .. } => move_path.as_ref().unwrap_or(path),
        };

        let abs = if candidate.is_absolute() {
            candidate.clone()
        } else {
            cwd.join(candidate)
        };

        let mut allowed = false;
        for root in writable_roots {
            let root_abs = if root.is_absolute() {
                root.clone()
            } else {
                cwd.join(root)
            };
            if abs.starts_with(&root_abs) {
                allowed = true;
                break;
            }
        }

        if !allowed {
            return Some(candidate.clone());
        }
    }
    None
}

fn convert_apply_patch_to_protocol(action: &ApplyPatchAction) -> HashMap<PathBuf, FileChange> {
    let changes = action.changes();
    let mut result = HashMap::with_capacity(changes.len());
    for (path, change) in changes {
        let protocol_change = match change {
            ApplyPatchFileChange::Add { content } => FileChange::Add {
                content: content.clone(),
            },
            ApplyPatchFileChange::Delete => FileChange::Delete,
            ApplyPatchFileChange::Update {
                unified_diff,
                move_path,
                new_content: _new_content,
            } => FileChange::Update {
                unified_diff: unified_diff.clone(),
                move_path: move_path.clone(),
            },
        };
        result.insert(path.clone(), protocol_change);
    }
    result
}

fn apply_changes_from_apply_patch_and_report(
    action: &ApplyPatchAction,
    stdout: &mut impl std::io::Write,
    stderr: &mut impl std::io::Write,
) -> std::io::Result<()> {
    match apply_changes_from_apply_patch(action) {
        Ok(affected_paths) => {
            print_summary(&affected_paths, stdout)?;
        }
        Err(err) => {
            writeln!(stderr, "{err:?}")?;
        }
    }

    Ok(())
}

fn apply_changes_from_apply_patch(action: &ApplyPatchAction) -> anyhow::Result<AffectedPaths> {
    let mut added: Vec<PathBuf> = Vec::new();
    let mut modified: Vec<PathBuf> = Vec::new();
    let mut deleted: Vec<PathBuf> = Vec::new();

    let changes = action.changes();
    for (path, change) in changes {
        match change {
            ApplyPatchFileChange::Add { content } => {
                if let Some(parent) = path.parent() {
                    if !parent.as_os_str().is_empty() {
                        std::fs::create_dir_all(parent).with_context(|| {
                            format!("Failed to create parent directories for {}", path.display())
                        })?;
                    }
                }
                std::fs::write(path, content)
                    .with_context(|| format!("Failed to write file {}", path.display()))?;
                added.push(path.clone());
            }
            ApplyPatchFileChange::Delete => {
                std::fs::remove_file(path)
                    .with_context(|| format!("Failed to delete file {}", path.display()))?;
                deleted.push(path.clone());
            }
            ApplyPatchFileChange::Update {
                unified_diff: _unified_diff,
                move_path,
                new_content,
            } => {
                if let Some(move_path) = move_path {
                    if let Some(parent) = move_path.parent() {
                        if !parent.as_os_str().is_empty() {
                            std::fs::create_dir_all(parent).with_context(|| {
                                format!(
                                    "Failed to create parent directories for {}",
                                    move_path.display()
                                )
                            })?;
                        }
                    }

                    std::fs::rename(path, move_path)
                        .with_context(|| format!("Failed to rename file {}", path.display()))?;
                    std::fs::write(move_path, new_content)?;
                    modified.push(move_path.clone());
                    deleted.push(path.clone());
                } else {
                    std::fs::write(path, new_content)?;
                    modified.push(path.clone());
                }
            }
        }
    }

    Ok(AffectedPaths {
        added,
        modified,
        deleted,
    })
}

fn get_writable_roots(cwd: &Path) -> Vec<std::path::PathBuf> {
    let mut writable_roots = Vec::new();
    if cfg!(target_os = "macos") {
        // On macOS, $TMPDIR is private to the user.
        writable_roots.push(std::env::temp_dir());

        // Allow pyenv to update its shims directory. Without this, any tool
        // that happens to be managed by `pyenv` will fail with an error like:
        //
        //   pyenv: cannot rehash: $HOME/.pyenv/shims isn't writable
        //
        // which is emitted every time `pyenv` tries to run `rehash` (for
        // example, after installing a new Python package that drops an entry
        // point). Although the sandbox is intentionally read‑only by default,
        // writing to the user's local `pyenv` directory is safe because it
        // is already user‑writable and scoped to the current user account.
        if let Ok(home_dir) = std::env::var("HOME") {
            let pyenv_dir = PathBuf::from(home_dir).join(".pyenv");
            writable_roots.push(pyenv_dir);
        }
    }

    writable_roots.push(cwd.to_path_buf());

    writable_roots
}

/// Exec output is a pre-serialized JSON payload
fn format_exec_output(output: &str, exit_code: i32, duration: std::time::Duration) -> String {
    #[derive(Serialize)]
    struct ExecMetadata {
        exit_code: i32,
        duration_seconds: f32,
    }

    #[derive(Serialize)]
    struct ExecOutput<'a> {
        output: &'a str,
        metadata: ExecMetadata,
    }

    // round to 1 decimal place
    let duration_seconds = ((duration.as_secs_f32()) * 10.0).round() / 10.0;

    let payload = ExecOutput {
        output,
        metadata: ExecMetadata {
            exit_code,
            duration_seconds,
        },
    };

    serde_json::to_string(&payload).expect("serialize ExecOutput")
}

fn get_last_assistant_message_from_turn(responses: &[ResponseItem]) -> Option<String> {
    responses.iter().rev().find_map(|item| {
        if let ResponseItem::Message { role, content } = item {
            if role == "assistant" {
                content.iter().rev().find_map(|ci| {
                    if let ContentItem::OutputText { text } = ci {
                        Some(text.clone())
                    } else {
                        None
                    }
                })
            } else {
                None
            }
        } else {
            None
        }
    })
}
