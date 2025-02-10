use netrunner::node::NodeId;
use serde::Deserialize;
use serde::Serialize;
use serde_with::{hex::Hex, serde_as};
use uuid::Uuid;

use super::Sha256Hash;

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageEvent {
    pub payload_id: PayloadId,
    pub step_id: usize,
    #[serde(with = "node_id_serde")]
    pub node_id: NodeId,
    pub event_type: MessageEventType,
}

#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
        #[serde(with = "node_id_serde")]
        to: NodeId,
        #[serde_as(as = "Hex")]
        message_hash: Sha256Hash,
    },
    NetworkReceived {
        #[serde(with = "node_id_serde")]
        from: NodeId,
        #[serde_as(as = "Hex")]
        message_hash: Sha256Hash,
        duplicate: bool,
    },
    FullyUnwrapped,
}

mod node_id_serde {
    use netrunner::node::{NodeId, NodeIdExt};
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(node_id: &NodeId, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u64(node_id.index().try_into().unwrap())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<NodeId, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(NodeId::from_index(
            u64::deserialize(deserializer)?.try_into().unwrap(),
        ))
    }
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
