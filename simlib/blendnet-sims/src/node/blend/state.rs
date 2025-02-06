use std::any::Any;

use serde::Serialize;

use netrunner::{
    node::{serialize_node_id_as_index, NodeId},
    output_processors::{Record, RecordType, Runtime},
    settings::SimulationSettings,
    warding::SimulationState,
};

#[derive(Debug, Clone, Serialize)]
pub struct BlendnodeState {
    #[serde(serialize_with = "serialize_node_id_as_index")]
    pub node_id: NodeId,
    pub step_id: usize,
    pub num_messages_fully_unwrapped: usize,
    pub cur_num_persistent_scheduled: usize,
    pub cur_num_temporal_scheduled: usize,
}

#[derive(Serialize)]
#[serde(untagged)]
pub enum BlendnodeRecord {
    Runtime(Runtime),
    Settings(Box<SimulationSettings>),
    #[allow(clippy::vec_box)] // we downcast stuff and we need the extra boxing
    Data(Vec<Box<BlendnodeState>>),
}

impl From<Runtime> for BlendnodeRecord {
    fn from(value: Runtime) -> Self {
        Self::Runtime(value)
    }
}

impl From<SimulationSettings> for BlendnodeRecord {
    fn from(value: SimulationSettings) -> Self {
        Self::Settings(Box::new(value))
    }
}

impl Record for BlendnodeRecord {
    type Data = BlendnodeState;

    fn record_type(&self) -> RecordType {
        match self {
            BlendnodeRecord::Runtime(_) => RecordType::Meta,
            BlendnodeRecord::Settings(_) => RecordType::Settings,
            BlendnodeRecord::Data(_) => RecordType::Data,
        }
    }

    fn data(&self) -> Vec<&BlendnodeState> {
        match self {
            BlendnodeRecord::Data(d) => d.iter().map(AsRef::as_ref).collect(),
            _ => vec![],
        }
    }
}

impl<S, T: Clone + Serialize + 'static> TryFrom<&SimulationState<S, T>> for BlendnodeRecord {
    type Error = anyhow::Error;

    fn try_from(state: &SimulationState<S, T>) -> Result<Self, Self::Error> {
        let Ok(states) = state
            .nodes
            .read()
            .iter()
            .map(|n| Box::<dyn Any + 'static>::downcast(Box::new(n.state().clone())))
            .collect::<Result<Vec<_>, _>>()
        else {
            return Err(anyhow::anyhow!("use carnot record on other node"));
        };
        Ok(Self::Data(states))
    }
}
