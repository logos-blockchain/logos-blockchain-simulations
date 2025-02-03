#[cfg(test)]
pub mod dummy_streaming;

// std
use std::{
    ops::{Deref, DerefMut},
    sync::Arc,
    time::Duration,
};
// crates
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use crate::warding::WardCondition;
// internal

#[serde_with::serde_as]
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct StepTime(#[serde_as(as = "serde_with::DurationMilliSeconds")] Duration);

impl From<Duration> for StepTime {
    fn from(duration: Duration) -> Self {
        Self(duration)
    }
}

impl StepTime {
    #[inline]
    pub const fn new(duration: Duration) -> Self {
        Self(duration)
    }

    #[inline]
    pub const fn into_inner(&self) -> Duration {
        self.0
    }

    #[inline]
    pub const fn from_millis(millis: u64) -> Self {
        Self(Duration::from_millis(millis))
    }

    #[inline]
    pub const fn from_secs(secs: u64) -> Self {
        Self(Duration::from_secs(secs))
    }
}

impl Deref for StepTime {
    type Target = Duration;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for StepTime {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl core::iter::Sum<Self> for StepTime {
    fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
        Self(iter.into_iter().map(|s| s.0).sum())
    }
}

impl core::iter::Sum<Duration> for StepTime {
    fn sum<I: Iterator<Item = Duration>>(iter: I) -> Self {
        Self(iter.into_iter().sum())
    }
}

impl core::iter::Sum<StepTime> for Duration {
    fn sum<I: Iterator<Item = StepTime>>(iter: I) -> Self {
        iter.into_iter().map(|s| s.0).sum()
    }
}

pub type SharedState<S> = Arc<RwLock<S>>;

pub type Step = usize;

pub trait Node {
    type Settings;
    type State;

    fn id(&self) -> NodeId;
    fn state(&self) -> &Self::State;
    fn step(&mut self, elapsed: Duration);
    fn analyze(&self, ward: &mut WardCondition) -> bool;
}

#[derive(
    Clone, Copy, Default, Debug, Eq, PartialEq, Hash, Ord, PartialOrd, Serialize, Deserialize,
)]
pub struct NodeId(pub [u8; 32]);

impl NodeId {
    pub const fn new(val: [u8; 32]) -> Self {
        Self(val)
    }

    /// Returns a random node id
    pub fn random<R: rand::Rng>(rng: &mut R) -> Self {
        let mut bytes = [0u8; 32];
        rng.fill_bytes(&mut bytes);
        Self(bytes)
    }
}

impl From<[u8; 32]> for NodeId {
    fn from(id: [u8; 32]) -> Self {
        Self(id)
    }
}

impl From<&[u8; 32]> for NodeId {
    fn from(id: &[u8; 32]) -> Self {
        Self(*id)
    }
}

impl From<NodeId> for [u8; 32] {
    fn from(id: NodeId) -> Self {
        id.0
    }
}

impl<'a> From<&'a NodeId> for &'a [u8; 32] {
    fn from(id: &'a NodeId) -> Self {
        &id.0
    }
}

impl core::fmt::Display for NodeId {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "0x")?;
        for v in self.0 {
            write!(f, "{:02x}", v)?;
        }
        Ok(())
    }
}

#[cfg(test)]
impl Node for usize {
    type Settings = ();
    type State = Self;

    fn id(&self) -> NodeId {
        todo!()
    }

    fn state(&self) -> &Self::State {
        self
    }

    fn step(&mut self, _: Duration) {
        use std::ops::AddAssign;
        self.add_assign(1);
    }

    fn analyze(&self, ward: &mut WardCondition) -> bool {
        match ward {
            WardCondition::Max(ward) => *self >= ward.max_count,
            WardCondition::Sum(condition) => {
                *condition.step_result.borrow_mut() += *self;
                false
            }
        }
    }
}

pub trait NodeIdExt {
    fn index(&self) -> usize;

    fn from_index(idx: usize) -> Self;
}

impl NodeIdExt for NodeId {
    fn index(&self) -> usize {
        const SIZE: usize = core::mem::size_of::<usize>();
        let mut bytes = [0u8; SIZE];
        let src: [u8; 32] = (*self).into();
        bytes.copy_from_slice(&src[..SIZE]);
        usize::from_be_bytes(bytes)
    }

    fn from_index(idx: usize) -> Self {
        let mut bytes = [0u8; 32];
        bytes[..core::mem::size_of::<usize>()].copy_from_slice(&idx.to_be_bytes());
        NodeId::new(bytes)
    }
}

pub fn serialize_node_id_as_index<S>(id: &NodeId, s: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    s.serialize_u64(id.index() as u64)
}
