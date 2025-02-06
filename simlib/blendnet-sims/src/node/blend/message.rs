use std::{ops::Mul, time::Duration};

use netrunner::node::serialize_node_id_as_index;
use netrunner::node::NodeId;
use serde::Serialize;
use uuid::Uuid;

pub type PayloadId = String;

pub struct Payload(Uuid);

impl Payload {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    pub fn id(&self) -> PayloadId {
        self.0.to_string()
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }

    pub fn load(data: Vec<u8>) -> Self {
        assert_eq!(data.len(), 16);
        Self(data.try_into().unwrap())
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct MessageHistory(Vec<MessageEvent>);

impl MessageHistory {
    pub fn new() -> Self {
        Self(Vec::new())
    }

    pub fn add(
        &mut self,
        node_id: NodeId,
        step_id: usize,
        step_time: Duration,
        event_type: MessageEventType,
    ) {
        let duration_from_prev = self.0.last().map_or(Duration::ZERO, |prev_event| {
            step_time.mul((step_id - prev_event.step_id).try_into().unwrap())
        });
        self.0.push(MessageEvent {
            node_id,
            step_id,
            duration_from_prev,
            event_type,
        });
    }

    pub fn last_event_type(&self) -> Option<&MessageEventType> {
        self.0.last().map(|event| &event.event_type)
    }

    pub fn total_duration(&self) -> Duration {
        self.0.iter().map(|event| event.duration_from_prev).sum()
    }
}

#[derive(Debug, Clone, Serialize)]
struct MessageEvent {
    #[serde(serialize_with = "serialize_node_id_as_index")]
    node_id: NodeId,
    step_id: usize,
    #[serde(serialize_with = "duration_as_millis")]
    duration_from_prev: Duration,
    event_type: MessageEventType,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum MessageEventType {
    Created,
    PersistentTransmissionScheduled {
        index: usize,
    },
    PersistentTransmissionReleased,
    TemporalProcessorScheduled {
        index: usize,
    },
    TemporalProcessorReleased,
    NetworkSent {
        #[serde(serialize_with = "serialize_node_id_as_index")]
        to: NodeId,
    },
    NetworkReceived {
        #[serde(serialize_with = "serialize_node_id_as_index")]
        from: NodeId,
    },
}

pub fn duration_as_millis<S>(duration: &Duration, s: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    s.serialize_u64(duration.as_millis().try_into().unwrap())
}

#[cfg(test)]
mod tests {
    use super::Payload;

    #[test]
    fn payload() {
        let payload = Payload::new();
        println!("{}", payload.id());
        let bytes = payload.as_bytes();
        assert_eq!(bytes.len(), 16);
        let loaded_payload = Payload::load(bytes.to_vec());
        assert_eq!(bytes, loaded_payload.as_bytes());
    }
}
