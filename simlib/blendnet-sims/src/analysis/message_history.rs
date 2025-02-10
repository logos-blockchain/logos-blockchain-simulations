use std::{
    error::Error,
    fs::File,
    io::{BufRead, BufReader},
    iter::Rev,
    ops::{Add, Mul},
    path::PathBuf,
    slice::Iter,
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

    let lines: Vec<String> = reader.lines().collect::<Result<_, _>>()?;
    let mut rev_iter = lines.iter().rev();

    let event = find_event(
        &payload_id,
        None,
        |event_type| matches!(event_type, MessageEventType::FullyUnwrapped),
        &mut rev_iter,
    )
    .unwrap();
    history.push(event);
    loop {
        let last_event = history.last().unwrap();
        let event = match &last_event.event_type {
            MessageEventType::Created => break,
            MessageEventType::PersistentTransmissionScheduled { .. } => find_event(
                &payload_id,
                Some(&last_event.node_id),
                |event_type| {
                    matches!(
                        event_type,
                        MessageEventType::Created | MessageEventType::TemporalProcessorReleased
                    )
                },
                &mut rev_iter,
            )
            .unwrap(),
            MessageEventType::PersistentTransmissionReleased => find_event(
                &payload_id,
                Some(&last_event.node_id),
                |event_type| {
                    matches!(
                        event_type,
                        MessageEventType::PersistentTransmissionScheduled { .. }
                    )
                },
                &mut rev_iter,
            )
            .unwrap(),
            MessageEventType::TemporalProcessorScheduled { .. } => find_event(
                &payload_id,
                Some(&last_event.node_id),
                |event_type| {
                    matches!(
                        event_type,
                        MessageEventType::NetworkReceived {
                            duplicate: false,
                            ..
                        }
                    )
                },
                &mut rev_iter,
            )
            .unwrap(),
            MessageEventType::TemporalProcessorReleased => find_event(
                &payload_id,
                Some(&last_event.node_id),
                |event_type| {
                    matches!(
                        event_type,
                        MessageEventType::TemporalProcessorScheduled { .. }
                    )
                },
                &mut rev_iter,
            )
            .unwrap(),
            MessageEventType::NetworkSent {
                message_hash: target_message_hash,
                ..
            } => find_event(
                &payload_id,
                Some(&last_event.node_id),
                |event_type| match event_type {
                    MessageEventType::NetworkReceived {
                        message_hash,
                        duplicate: false,
                        ..
                    } => message_hash == target_message_hash,
                    MessageEventType::PersistentTransmissionReleased => true,
                    _ => false,
                },
                &mut rev_iter,
            )
            .unwrap(),
            MessageEventType::NetworkReceived {
                from,
                message_hash: target_message_hash,
                duplicate: false,
            } => {
                let to_node = last_event.node_id;
                match find_event(
                    &payload_id,
                    Some(from),
                    |event_type: &MessageEventType| match event_type {
                        MessageEventType::NetworkSent { to, message_hash } => {
                            to == &to_node && target_message_hash == message_hash
                        }
                        _ => false,
                    },
                    &mut rev_iter,
                ) {
                    Some(ev) => ev,
                    None => {
                        panic!(
                            "No matching NetworkSent event found for NetworkReceived event: {:?}",
                            last_event
                        );
                    }
                }
            }
            MessageEventType::FullyUnwrapped => find_event(
                &payload_id,
                Some(&last_event.node_id),
                |event_type| matches!(event_type, MessageEventType::TemporalProcessorReleased),
                &mut rev_iter,
            )
            .unwrap(),
            event_type => {
                panic!("Unexpected event type: {:?}", event_type);
            }
        };

        history.push(event);
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

fn find_event<F>(
    payload_id: &PayloadId,
    node_id: Option<&NodeId>,
    match_event: F,
    rev_iter: &mut Rev<Iter<'_, String>>,
) -> Option<MessageEvent>
where
    F: Fn(&MessageEventType) -> bool,
{
    for line in rev_iter {
        if let Ok(topic_log) = serde_json::from_str::<TopicLog<MessageEvent>>(line) {
            assert_eq!(topic_log.topic, "MessageEvent");
            let event = topic_log.message;
            if &event.payload_id == payload_id
                && node_id.map_or(true, |node_id| &event.node_id == node_id)
                && match_event(&event.event_type)
            {
                return Some(event);
            }
        }
    }
    None
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
