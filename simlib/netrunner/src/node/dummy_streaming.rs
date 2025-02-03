use serde::{Deserialize, Serialize};
use std::time::Duration;

use crate::warding::WardCondition;

use super::{Node, NodeId};

#[derive(Debug, Default, Copy, Clone, Serialize, Deserialize)]
pub struct DummyStreamingState {
    pub counter: usize,
}

/// This node implementation only used for testing different streaming implementation purposes.
pub struct DummyStreamingNode<S> {
    id: NodeId,
    state: DummyStreamingState,
    #[allow(dead_code)]
    settings: S,
}

impl<S> DummyStreamingNode<S> {
    pub fn new(id: NodeId, settings: S) -> Self {
        Self {
            id,
            state: DummyStreamingState::default(),
            settings,
        }
    }
}

impl<S> Node for DummyStreamingNode<S> {
    type Settings = S;

    type State = DummyStreamingState;

    fn id(&self) -> NodeId {
        self.id
    }

    fn state(&self) -> &Self::State {
        &self.state
    }

    fn step(&mut self, _: Duration) {
        self.state.counter += 1;
    }

    fn analyze(&self, ward: &mut crate::warding::WardCondition) -> bool {
        match ward {
            WardCondition::Max(ward) => self.state.counter >= ward.max_count,
            WardCondition::Sum(condition) => {
                *condition.step_result.borrow_mut() += self.state.counter;
                false
            }
        }
    }
}
