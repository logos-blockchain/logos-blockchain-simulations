use std::{
    error::Error,
    fs::File,
    io::{BufRead, BufReader},
    ops::{Add, Mul},
    path::PathBuf,
    time::Duration,
};

use netrunner::node::NodeId;
use serde::{Deserialize, Serialize};

use crate::node::blend::{
    log::TopicLog,
    message::{MessageEvent, MessageEventType, PayloadId},
};

pub fn analyze_message_history(
    log_file: PathBuf,
    step_duration: Duration,
    payload_id: PayloadId,
) -> Result<(), Box<dyn Error>> {
    let file = File::open(log_file)?;
    let reader = BufReader::new(file);

    let mut history = Vec::new();
    let mut target_node_id: Option<NodeId> = None;
    let mut target_event: Option<MessageEventType> = None;

    let lines: Vec<String> = reader.lines().collect::<Result<_, _>>()?;
    for line in lines.iter().rev() {
        if let Ok(topic_log) = serde_json::from_str::<TopicLog<MessageEvent>>(line) {
            assert_eq!(topic_log.topic, "MessageEvent");
            let event = topic_log.message;
            if event.payload_id == payload_id
                && (target_node_id.is_none() || target_node_id.unwrap() == event.node_id)
                && (target_event.is_none() || target_event.as_ref().unwrap() == &event.event_type)
            {
                match event.event_type {
                    MessageEventType::FullyUnwrapped => {
                        assert!(history.is_empty());
                        assert!(target_node_id.is_none());
                        target_node_id = Some(event.node_id);
                        history.push(event);
                    }
                    MessageEventType::Created => {
                        assert!(!history.is_empty());
                        assert!(target_node_id.is_some());
                        history.push(event);
                    }
                    MessageEventType::PersistentTransmissionScheduled { .. } => {
                        assert!(target_node_id.is_some());
                        assert!(matches!(
                            history.last().unwrap().event_type,
                            MessageEventType::PersistentTransmissionReleased { .. }
                        ));
                        history.push(event);
                    }
                    MessageEventType::PersistentTransmissionReleased => {
                        assert!(target_node_id.is_some());
                        history.push(event);
                    }
                    MessageEventType::TemporalProcessorScheduled { .. } => {
                        assert!(target_node_id.is_some());
                        assert!(matches!(
                            history.last().unwrap().event_type,
                            MessageEventType::TemporalProcessorReleased { .. }
                        ));
                        history.push(event);
                    }
                    MessageEventType::TemporalProcessorReleased => {
                        assert!(target_node_id.is_some());
                        history.push(event);
                    }
                    MessageEventType::NetworkReceived { from } => {
                        assert!(!history.is_empty());
                        assert!(target_node_id.is_some());
                        assert_ne!(target_node_id.unwrap(), from);
                        target_node_id = Some(from);
                        target_event = Some(MessageEventType::NetworkSent { to: event.node_id });
                        history.push(event);
                    }
                    MessageEventType::NetworkSent { .. } => {
                        assert!(!history.is_empty());
                        assert!(target_node_id.is_some());
                        if target_event.is_none()
                            || target_event.as_ref().unwrap() != &event.event_type
                        {
                            continue;
                        }
                        target_event = None;
                        history.push(event);
                    }
                }
            }
        }
    }

    let mut history_with_durations: Vec<MessageEventWithDuration> = Vec::new();
    let (_, total_duration) = history.iter().rev().fold(
        (None, Duration::ZERO),
        |(prev_step_id, total_duration): (Option<usize>, Duration), event| {
            let duration = match prev_step_id {
                Some(prev_step_id) => {
                    step_duration.mul((event.step_id - prev_step_id).try_into().unwrap())
                }
                None => Duration::ZERO,
            };
            history_with_durations.push(MessageEventWithDuration {
                event: event.clone(),
                duration,
            });
            (Some(event.step_id), total_duration.add(duration))
        },
    );
    let output = Output {
        history: history_with_durations,
        total_duration,
    };
    println!("{}", serde_json::to_string(&output).unwrap());
    Ok(())
}

#[derive(Serialize, Deserialize)]
struct Output {
    history: Vec<MessageEventWithDuration>,
    #[serde(with = "humantime_serde")]
    total_duration: Duration,
}

#[derive(Serialize, Deserialize)]
struct MessageEventWithDuration {
    event: MessageEvent,
    #[serde(with = "humantime_serde")]
    duration: Duration,
}
