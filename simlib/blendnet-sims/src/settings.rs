use netrunner::settings::SimulationSettings;
use nomos_blend::persistent_transmission::PersistentTransmissionSettings;
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Serialize, Deserialize, Clone)]
pub struct SimSettings {
    #[serde(flatten)]
    pub simulation_settings: SimulationSettings,
    pub connected_peers_count: usize,
    #[serde(with = "humantime_serde")]
    pub data_message_lottery_interval: Duration,
    pub stake_proportion: f64,
    // For tier 3: cover traffic
    #[serde(with = "humantime_serde")]
    pub epoch_duration: Duration,
    #[serde(with = "humantime_serde")]
    pub slot_duration: Duration,
    pub slots_per_epoch: usize,
    pub number_of_hops: usize,
    // For tier 1
    pub persistent_transmission: PersistentTransmissionSettings,
    // For tier 2
    pub number_of_blend_layers: usize,
    pub max_delay_seconds: u64,
}
