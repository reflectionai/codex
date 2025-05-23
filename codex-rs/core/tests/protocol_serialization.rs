use codex_core::protocol::*;
use uuid::Uuid;

/// Serialize Event to verify that its JSON representation has the expected
/// amount of nesting.
#[test]
fn serialize_event() {
    let session_id: Uuid = uuid::uuid!("67e55044-10b1-426f-9247-bb680e5fe0c8");
    let event = Event {
        id: "1234".to_string(),
        msg: EventMsg::SessionConfigured(SessionConfiguredEvent {
            session_id,
            model: "o4-mini".to_string(),
            history_log_id: 0,
            history_entry_count: 0,
        }),
    };
    let serialized = serde_json::to_string(&event).unwrap();
    assert_eq!(
        serialized,
        r#"{"id":"1234","msg":{"type":"session_configured","session_id":"67e55044-10b1-426f-9247-bb680e5fe0c8","model":"o4-mini","history_log_id":0,"history_entry_count":0}}"#
    );
}

/// Serialize TaskComplete event with token usage to verify JSON format
#[test]
fn serialize_task_complete_with_usage() {
    let event = Event {
        id: "5678".to_string(),
        msg: EventMsg::TaskComplete(TaskCompleteEvent {
            last_agent_message: None,
            token_usage: Some(TokenUsage {
                input_tokens: 1000,
                output_tokens: 500,
                total_cost: Some(0.0125),
            }),
        }),
    };
    let serialized = serde_json::to_string(&event).unwrap();
    println!("JSON with cost: {}", serialized);
    assert_eq!(
        serialized,
        r#"{"id":"5678","msg":{"type":"task_complete","last_agent_message":null,"input_tokens":1000,"output_tokens":500,"total_cost":0.0125}}"#
    );
}

/// Serialize TaskComplete event without cost to verify JSON format
#[test]
fn serialize_task_complete_no_cost() {
    let event = Event {
        id: "9999".to_string(),
        msg: EventMsg::TaskComplete(TaskCompleteEvent {
            last_agent_message: None,
            token_usage: Some(TokenUsage {
                input_tokens: 1500,
                output_tokens: 750,
                total_cost: None,
            }),
        }),
    };
    let serialized = serde_json::to_string(&event).unwrap();
    println!("JSON without cost: {}", serialized);
    assert_eq!(
        serialized,
        r#"{"id":"9999","msg":{"type":"task_complete","last_agent_message":null,"input_tokens":1500,"output_tokens":750}}"#
    );
}

/// Serialize TaskComplete event with no token usage
#[test]
fn serialize_task_complete_no_usage() {
    let event = Event {
        id: "0000".to_string(),
        msg: EventMsg::TaskComplete(TaskCompleteEvent {
            last_agent_message: None,
            token_usage: None,
        }),
    };
    let serialized = serde_json::to_string(&event).unwrap();
    println!("JSON no usage: {}", serialized);
    assert_eq!(
        serialized,
        r#"{"id":"0000","msg":{"type":"task_complete","last_agent_message":null}}"#
    );
}