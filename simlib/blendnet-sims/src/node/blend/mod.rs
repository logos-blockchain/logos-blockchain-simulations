pub mod consensus_streams;
#[macro_use]
pub mod log;
pub mod lottery;
mod message;
pub mod scheduler;
pub mod state;
pub mod stream_wrapper;
pub mod topology;

use crate::node::blend::consensus_streams::{Epoch, Slot};
use cached::{Cached, TimedCache};
use crossbeam::channel;
use futures::Stream;
use lottery::StakeLottery;
use message::{Payload, PayloadId};
use multiaddr::Multiaddr;
use netrunner::network::regions::RegionsData;
use netrunner::network::NetworkMessage;
use netrunner::node::{Node, NodeId, NodeIdExt};
use netrunner::{
    network::{InMemoryNetworkInterface, NetworkInterface, PayloadSize},
    warding::WardCondition,
};
use nomos_blend::message_blend::temporal::{TemporalProcessorExt, TemporalStream};
use nomos_blend::{
    cover_traffic::{CoverTraffic, CoverTrafficSettings},
    membership::Membership,
    message_blend::{crypto::CryptographicProcessor, MessageBlendSettings},
    persistent_transmission::{
        PersistentTransmissionExt, PersistentTransmissionSettings, PersistentTransmissionStream,
    },
    BlendOutgoingMessage,
};
use nomos_blend_message::mock::MockBlendMessage;
use rand::SeedableRng;
use rand_chacha::ChaCha12Rng;
use scheduler::{Interval, TemporalScheduler};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use state::BlendnodeState;
use std::{pin::pin, task::Poll, time::Duration};
use stream_wrapper::CrossbeamReceiverStream;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlendMessage {
    message: Vec<u8>,
    network_route: Vec<(NodeId, Duration)>,
}

impl BlendMessage {
    pub fn new(message: Vec<u8>, origin: NodeId) -> Self {
        Self {
            message,
            network_route: vec![(origin, Duration::ZERO)],
        }
    }
}

impl PayloadSize for BlendMessage {
    fn size_bytes(&self) -> u32 {
        2208
    }
}

struct BlendOutgoingMessageWithRoute {
    outgoing_message: BlendOutgoingMessage,
    network_route: Vec<(NodeId, Duration)>,
}

#[derive(Deserialize)]
pub struct BlendnodeSettings {
    pub connected_peers: Vec<NodeId>,
    pub data_message_lottery_interval: Duration,
    pub stake_proportion: f64,
    pub seed: u64,
    pub epoch_duration: Duration,
    pub slot_duration: Duration,
    pub persistent_transmission: PersistentTransmissionSettings,
    pub message_blend: MessageBlendSettings<MockBlendMessage>,
    pub cover_traffic_settings: CoverTrafficSettings,
    pub membership: Vec<<MockBlendMessage as nomos_blend_message::BlendMessage>::PublicKey>,
}

type Sha256Hash = [u8; 32];

/// This node implementation only used for testing different streaming implementation purposes.
pub struct BlendNode {
    id: NodeId,
    state: BlendnodeState,
    settings: BlendnodeSettings,
    network_interface: InMemoryNetworkInterface<BlendMessage>,
    regions_data: RegionsData,
    message_cache: TimedCache<Sha256Hash, ()>,

    data_msg_lottery_update_time_sender: channel::Sender<Duration>,
    data_msg_lottery_interval: Interval,
    data_msg_lottery: StakeLottery<ChaCha12Rng>,

    persistent_sender: channel::Sender<Vec<u8>>,
    persistent_update_time_sender: channel::Sender<Duration>,
    persistent_transmission_messages: PersistentTransmissionStream<
        CrossbeamReceiverStream<Vec<u8>>,
        ChaCha12Rng,
        MockBlendMessage,
        Interval,
    >,
    crypto_processor: CryptographicProcessor<ChaCha12Rng, MockBlendMessage>,
    temporal_sender: channel::Sender<BlendOutgoingMessageWithRoute>,
    temporal_update_time_sender: channel::Sender<Duration>,
    temporal_processor_messages:
        TemporalStream<CrossbeamReceiverStream<BlendOutgoingMessageWithRoute>, TemporalScheduler>,
    epoch_update_sender: channel::Sender<Duration>,
    slot_update_sender: channel::Sender<Duration>,
    cover_traffic: CoverTraffic<Epoch, Slot, MockBlendMessage>,
}

impl BlendNode {
    pub fn new(
        id: NodeId,
        settings: BlendnodeSettings,
        network_interface: InMemoryNetworkInterface<BlendMessage>,
        regions_data: RegionsData,
    ) -> Self {
        let mut rng_generator = ChaCha12Rng::seed_from_u64(settings.seed);

        // Init Interval for data message lottery
        let (data_msg_lottery_update_time_sender, data_msg_lottery_update_time_receiver) =
            channel::unbounded();
        let data_msg_lottery_interval = Interval::new(
            settings.data_message_lottery_interval,
            data_msg_lottery_update_time_receiver,
        );
        let data_msg_lottery = StakeLottery::new(
            ChaCha12Rng::from_rng(&mut rng_generator).unwrap(),
            settings.stake_proportion,
        );

        // Init Tier-1: Persistent transmission
        let (persistent_sender, persistent_receiver) = channel::unbounded();
        let (persistent_update_time_sender, persistent_update_time_receiver) = channel::unbounded();
        let persistent_transmission_messages = CrossbeamReceiverStream::new(persistent_receiver)
            .persistent_transmission(
                settings.persistent_transmission,
                ChaCha12Rng::from_rng(&mut rng_generator).unwrap(),
                Interval::new(
                    Duration::from_secs_f64(
                        1.0 / settings.persistent_transmission.max_emission_frequency,
                    ),
                    persistent_update_time_receiver,
                ),
            );

        // Init Tier-2: message blend: CryptographicProcessor and TemporalProcessor
        let nodes: Vec<
            nomos_blend::membership::Node<
                <MockBlendMessage as nomos_blend_message::BlendMessage>::PublicKey,
            >,
        > = settings
            .membership
            .iter()
            .map(|&public_key| nomos_blend::membership::Node {
                address: Multiaddr::empty(),
                public_key,
            })
            .collect();
        let membership = Membership::<MockBlendMessage>::new(nodes, id.into());
        let crypto_processor = CryptographicProcessor::new(
            settings.message_blend.cryptographic_processor.clone(),
            membership.clone(),
            ChaCha12Rng::from_rng(&mut rng_generator).unwrap(),
        );
        let (temporal_sender, temporal_receiver) = channel::unbounded();
        let (temporal_update_time_sender, temporal_update_time_receiver) = channel::unbounded();
        let temporal_processor_messages = CrossbeamReceiverStream::new(temporal_receiver)
            .temporal_stream(TemporalScheduler::new(
                ChaCha12Rng::from_rng(&mut rng_generator).unwrap(),
                temporal_update_time_receiver,
                (
                    1,
                    settings.message_blend.temporal_processor.max_delay_seconds,
                ),
            ));

        // tier 3 cover traffic
        let (epoch_update_sender, epoch_updater_update_receiver) = channel::unbounded();
        let (slot_update_sender, slot_updater_update_receiver) = channel::unbounded();
        let cover_traffic: CoverTraffic<Epoch, Slot, MockBlendMessage> = CoverTraffic::new(
            settings.cover_traffic_settings,
            Epoch::new(settings.epoch_duration, epoch_updater_update_receiver),
            Slot::new(
                settings.cover_traffic_settings.slots_per_epoch,
                settings.slot_duration,
                slot_updater_update_receiver,
            ),
        );

        Self {
            id,
            network_interface,
            regions_data,
            // We're not coupling this lifespan with the steps now, but it's okay
            // We expected that a message will be delivered to most of nodes within 60s.
            message_cache: TimedCache::with_lifespan(60),
            settings,
            state: BlendnodeState {
                node_id: id,
                step_id: 0,
                num_messages_fully_unwrapped: 0,
            },
            data_msg_lottery_update_time_sender,
            data_msg_lottery_interval,
            data_msg_lottery,
            persistent_sender,
            persistent_update_time_sender,
            persistent_transmission_messages,
            crypto_processor,
            temporal_sender,
            temporal_update_time_sender,
            temporal_processor_messages,
            epoch_update_sender,
            slot_update_sender,
            cover_traffic,
        }
    }

    fn forward(
        &mut self,
        message: BlendMessage,
        exclude_node: Option<NodeId>,
        log: Option<EmissionLog>,
    ) {
        for (i, node_id) in self
            .settings
            .connected_peers
            .iter()
            .filter(|&id| Some(*id) != exclude_node)
            .enumerate()
        {
            if i == 0 {
                if let Some(log) = &log {
                    Self::log_emission(log);
                }
            }
            self.network_interface
                .send_message(*node_id, message.clone())
        }
        self.message_cache
            .cache_set(Self::sha256(&message.message), ());
    }

    fn receive(&mut self) -> Vec<NetworkMessage<BlendMessage>> {
        self.network_interface
            .receive_messages()
            .into_iter()
            // Retain only messages that have not been seen before
            .filter(|msg| {
                self.message_cache
                    .cache_set(Self::sha256(&msg.payload().message), ())
                    .is_none()
            })
            .collect()
    }

    fn sha256(message: &[u8]) -> Sha256Hash {
        let mut hasher = Sha256::new();
        hasher.update(message);
        hasher.finalize().into()
    }

    fn schedule_persistent_transmission(&mut self, message: BlendMessage) {
        self.log_message(
            "PersistentTransmissionScheduled",
            &Self::parse_payload(&message.message),
        );
        self.persistent_sender
            .send(bincode::serialize(&message).unwrap())
            .unwrap();
    }

    fn handle_incoming_message(&mut self, message: BlendMessage) {
        match self.crypto_processor.unwrap_message(&message.message) {
            Ok((unwrapped_message, fully_unwrapped)) => {
                let temporal_message = if fully_unwrapped {
                    BlendOutgoingMessage::FullyUnwrapped(unwrapped_message)
                } else {
                    BlendOutgoingMessage::Outbound(unwrapped_message)
                };

                self.schedule_temporal_processor(BlendOutgoingMessageWithRoute {
                    outgoing_message: temporal_message,
                    network_route: message.network_route,
                });
            }
            Err(e) => {
                tracing::debug!("Failed to unwrap message: {:?}", e);
            }
        }
    }

    fn schedule_temporal_processor(&mut self, message: BlendOutgoingMessageWithRoute) {
        let payload = match &message.outgoing_message {
            BlendOutgoingMessage::FullyUnwrapped(payload) => Payload::load(payload.clone()),
            BlendOutgoingMessage::Outbound(msg) => Self::parse_payload(msg),
        };
        self.log_message("TemporalProcessorScheduled", &payload);
        self.temporal_sender.send(message).unwrap();
    }

    fn parse_payload(message: &[u8]) -> Payload {
        Payload::load(MockBlendMessage::payload(message).unwrap())
    }

    fn update_time(&mut self, elapsed: Duration) {
        self.data_msg_lottery_update_time_sender
            .send(elapsed)
            .unwrap();
        self.persistent_update_time_sender.send(elapsed).unwrap();
        self.temporal_update_time_sender.send(elapsed).unwrap();
        self.epoch_update_sender.send(elapsed).unwrap();
        self.slot_update_sender.send(elapsed).unwrap();
    }

    fn log_message_generated(&self, msg_type: &str, payload: &Payload) {
        self.log_message(format!("{}MessageGenerated", msg_type).as_str(), payload);
    }

    fn log_message_released_from(&self, from: &str, payload: &Payload) {
        self.log_message(format!("MessageReleasedFrom{}", from).as_str(), payload);
    }

    fn log_message_fully_unwrapped(
        &self,
        payload: &Payload,
        network_route: Vec<(NodeId, Duration)>,
    ) {
        log!(
            "MessageFullyUnwrapped",
            MessageWithRouteLog {
                message: MessageLog {
                    payload_id: payload.id(),
                    step_id: self.state.step_id,
                    node_id: self.id.index(),
                },
                network_route: network_route
                    .into_iter()
                    .map(|(id, delay)| (id.index(), delay.as_millis()))
                    .collect(),
            }
        );
    }

    fn log_message(&self, topic: &str, payload: &Payload) {
        log!(
            topic,
            MessageLog {
                payload_id: payload.id(),
                step_id: self.state.step_id,
                node_id: self.id.index(),
            }
        );
    }

    fn log_emission(log: &EmissionLog) {
        log!("Emission", log);
    }

    fn new_emission_log(&self, emission_type: &str) -> EmissionLog {
        EmissionLog {
            emission_type: emission_type.to_string(),
            step_id: self.state.step_id,
            node_id: self.id.index(),
        }
    }
}

impl Node for BlendNode {
    type Settings = BlendnodeSettings;

    type State = BlendnodeState;

    fn id(&self) -> NodeId {
        self.id
    }

    fn state(&self) -> &Self::State {
        &self.state
    }

    fn step(&mut self, elapsed: Duration) {
        self.update_time(elapsed);
        let waker = futures::task::noop_waker();
        let mut cx = futures::task::Context::from_waker(&waker);

        // Generate a data message probabilistically
        if let Poll::Ready(Some(_)) = pin!(&mut self.data_msg_lottery_interval).poll_next(&mut cx) {
            if self.data_msg_lottery.run() {
                let payload = Payload::new();
                self.log_message_generated("Data", &payload);
                let message = self
                    .crypto_processor
                    .wrap_message(payload.as_bytes())
                    .unwrap();
                self.schedule_persistent_transmission(BlendMessage::new(message, self.id));
            }
        }

        // Handle incoming messages
        for mut network_message in self.receive() {
            assert_eq!(
                network_message.payload().network_route.last().unwrap().0,
                network_message.from
            );
            network_message.payload.network_route.push((
                self.id,
                self.regions_data
                    .network_behaviour(network_message.from, self.id)
                    .delay(),
            ));
            self.forward(
                network_message.payload().clone(),
                Some(network_message.from),
                None,
            );
            self.handle_incoming_message(network_message.into_payload());
        }

        // Proceed temporal processor
        if let Poll::Ready(Some(outgoing_msg_with_route)) =
            pin!(&mut self.temporal_processor_messages).poll_next(&mut cx)
        {
            match outgoing_msg_with_route.outgoing_message {
                BlendOutgoingMessage::Outbound(message) => {
                    self.log_message_released_from(
                        "TemporalProcessor",
                        &Self::parse_payload(&message),
                    );
                    self.schedule_persistent_transmission(BlendMessage {
                        message,
                        network_route: outgoing_msg_with_route.network_route,
                    });
                }
                BlendOutgoingMessage::FullyUnwrapped(payload) => {
                    let payload = Payload::load(payload);
                    self.log_message_released_from("TemporalProcessor", &payload);
                    self.log_message_fully_unwrapped(
                        &payload,
                        outgoing_msg_with_route.network_route,
                    );
                    self.state.num_messages_fully_unwrapped += 1;
                    //TODO: create a tracing event
                }
            }
        }

        // Generate a cover message probabilistically
        if let Poll::Ready(Some(_)) = pin!(&mut self.cover_traffic).poll_next(&mut cx) {
            let payload = Payload::new();
            self.log_message_generated("Cover", &payload);
            let message = self
                .crypto_processor
                .wrap_message(payload.as_bytes())
                .unwrap();
            self.schedule_persistent_transmission(BlendMessage::new(message, self.id));
        }

        // Proceed persistent transmission
        if let Poll::Ready(Some(msg)) =
            pin!(&mut self.persistent_transmission_messages).poll_next(&mut cx)
        {
            let msg: BlendMessage = bincode::deserialize(&msg).unwrap();
            self.log_message_released_from(
                "PersistentTransmission",
                &Self::parse_payload(&msg.message),
            );
            self.forward(msg, None, Some(self.new_emission_log("FromPersistent")));
        }

        self.state.step_id += 1;
    }

    fn analyze(&self, ward: &mut WardCondition) -> bool {
        match ward {
            WardCondition::Max(_) => false,
            WardCondition::Sum(condition) => {
                *condition.step_result.borrow_mut() += self.state.num_messages_fully_unwrapped;
                false
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct MessageLog {
    payload_id: PayloadId,
    step_id: usize,
    node_id: usize,
}

#[derive(Debug, Serialize, Deserialize)]
struct MessageWithRouteLog {
    message: MessageLog,
    network_route: Vec<(usize, u128)>,
}

#[derive(Debug, Serialize, Deserialize)]
struct EmissionLog {
    emission_type: String,
    step_id: usize,
    node_id: usize,
}
