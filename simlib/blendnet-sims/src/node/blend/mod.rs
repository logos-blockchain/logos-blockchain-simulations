pub mod consensus_streams;
#[macro_use]
pub mod log;
pub mod lottery;
pub mod message;
pub mod scheduler;
pub mod state;
pub mod stream_wrapper;
pub mod topology;

use crate::node::blend::consensus_streams::{Epoch, Slot};
use cached::{Cached, TimedCache};
use crossbeam::channel;
use futures::Stream;
use lottery::StakeLottery;
use message::{MessageEvent, MessageEventType, Payload};
use netrunner::network::NetworkMessage;
use netrunner::node::{Node, NodeId};
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
use nomos_blend_message::BlendMessage as _;
use rand::SeedableRng;
use rand_chacha::ChaCha12Rng;
use scheduler::{Interval, TemporalScheduler};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use state::BlendnodeState;
use std::{pin::pin, task::Poll, time::Duration};
use stream_wrapper::CrossbeamReceiverStream;

#[derive(Debug, Clone)]
pub struct SimMessage(Vec<u8>);

impl PayloadSize for SimMessage {
    fn size_bytes(&self) -> u32 {
        // payload: 32 KiB
        // header encryption overhead: 133 bytes = 48 + 17 * max_blend_hops(=5)
        // payload encryption overhaed: 16 bytes
        // economic data overhead: 8043 bytes
        40960
    }
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
    pub membership: Vec<
        nomos_blend::membership::Node<
            NodeId,
            <MockBlendMessage as nomos_blend_message::BlendMessage>::PublicKey,
        >,
    >,
}

pub type Sha256Hash = [u8; 32];

/// This node implementation only used for testing different streaming implementation purposes.
pub struct BlendNode {
    id: NodeId,
    state: BlendnodeState,
    settings: BlendnodeSettings,
    network_interface: InMemoryNetworkInterface<SimMessage>,
    message_cache: TimedCache<Sha256Hash, ()>,

    data_msg_lottery_update_time_sender: channel::Sender<Duration>,
    data_msg_lottery_interval: Interval,
    data_msg_lottery: StakeLottery<ChaCha12Rng>,

    persistent_sender: channel::Sender<SimMessage>,
    persistent_update_time_sender: channel::Sender<Duration>,
    persistent_transmission_messages:
        PersistentTransmissionStream<CrossbeamReceiverStream<SimMessage>, ChaCha12Rng, Interval>,

    crypto_processor: CryptographicProcessor<NodeId, ChaCha12Rng, MockBlendMessage>,
    temporal_sender: channel::Sender<BlendOutgoingMessage>,
    temporal_update_time_sender: channel::Sender<Duration>,
    temporal_processor_messages:
        TemporalStream<CrossbeamReceiverStream<BlendOutgoingMessage>, TemporalScheduler>,

    epoch_update_sender: channel::Sender<Duration>,
    slot_update_sender: channel::Sender<Duration>,
    cover_traffic: CoverTraffic<Epoch, Slot, MockBlendMessage>,
}

impl BlendNode {
    pub fn new(
        id: NodeId,
        settings: BlendnodeSettings,
        network_interface: InMemoryNetworkInterface<SimMessage>,
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
        let (persistent_sender, persistent_receiver) = channel::unbounded::<SimMessage>();
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
                SimMessage(MockBlendMessage::DROP_MESSAGE.to_vec()),
            );

        // Init Tier-2: message blend: CryptographicProcessor and TemporalProcessor
        let membership =
            Membership::<NodeId, MockBlendMessage>::new(settings.membership.clone(), id.into());
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
                    0,
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
            // We're not coupling this lifespan with the steps now, but it's okay
            // We expected that a message will be delivered to most of nodes within 60s.
            message_cache: TimedCache::with_lifespan(60),
            settings,
            state: BlendnodeState {
                node_id: id,
                step_id: 0,
                num_messages_fully_unwrapped: 0,
                cur_num_persistent_scheduled: 0,
                cur_num_temporal_scheduled: 0,
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

    fn parse_payload(message: &[u8]) -> Payload {
        Payload::load(MockBlendMessage::payload(message).unwrap())
    }

    fn forward(&mut self, message: SimMessage, exclude_node: Option<NodeId>) {
        let message_hash = Self::sha256(&message.0);
        self.message_cache.cache_set(message_hash, ());

        let payload_id = Self::parse_payload(&message.0).id();
        for node_id in self
            .settings
            .connected_peers
            .iter()
            .filter(|&id| Some(*id) != exclude_node)
        {
            log!(
                "MessageEvent",
                MessageEvent {
                    payload_id: payload_id.clone(),
                    step_id: self.state.step_id,
                    node_id: self.id,
                    event_type: MessageEventType::NetworkSent {
                        to: *node_id,
                        message_hash
                    }
                }
            );
            self.network_interface
                .send_message(*node_id, message.clone())
        }
    }

    fn receive(&mut self) -> Vec<NetworkMessage<SimMessage>> {
        self.network_interface
            .receive_messages()
            .into_iter()
            // Retain only messages that have not been seen before
            .filter(|msg| {
                let message_hash = Self::sha256(&msg.payload().0);
                let duplicate = self.message_cache.cache_set(message_hash, ()).is_some();
                log!(
                    "MessageEvent",
                    MessageEvent {
                        payload_id: Self::parse_payload(&msg.payload().0).id(),
                        step_id: self.state.step_id,
                        node_id: self.id,
                        event_type: MessageEventType::NetworkReceived {
                            from: msg.from,
                            message_hash,
                            duplicate,
                        }
                    }
                );
                !duplicate
            })
            .collect()
    }

    fn sha256(message: &[u8]) -> Sha256Hash {
        let mut hasher = Sha256::new();
        hasher.update(message);
        hasher.finalize().into()
    }

    fn schedule_persistent_transmission(&mut self, message: SimMessage) {
        log!(
            "MessageEvent",
            MessageEvent {
                payload_id: Self::parse_payload(&message.0).id(),
                step_id: self.state.step_id,
                node_id: self.id,
                event_type: MessageEventType::PersistentTransmissionScheduled {
                    index: self.state.cur_num_persistent_scheduled
                }
            }
        );
        self.persistent_sender.send(message).unwrap();
        self.state.cur_num_persistent_scheduled += 1;
    }

    fn handle_incoming_message(&mut self, message: SimMessage) {
        match self.crypto_processor.unwrap_message(&message.0) {
            Ok((unwrapped_message, fully_unwrapped)) => {
                let temporal_message = if fully_unwrapped {
                    BlendOutgoingMessage::FullyUnwrapped(unwrapped_message)
                } else {
                    BlendOutgoingMessage::Outbound(unwrapped_message)
                };

                self.schedule_temporal_processor(temporal_message);
            }
            Err(e) => {
                tracing::debug!("Failed to unwrap message: {:?}", e);
            }
        }
    }

    fn schedule_temporal_processor(&mut self, message: BlendOutgoingMessage) {
        log!(
            "MessageEvent",
            MessageEvent {
                payload_id: match &message {
                    BlendOutgoingMessage::Outbound(msg) => Self::parse_payload(msg).id(),
                    BlendOutgoingMessage::FullyUnwrapped(payload) =>
                        Payload::load(payload.clone()).id(),
                },
                step_id: self.state.step_id,
                node_id: self.id,
                event_type: MessageEventType::TemporalProcessorScheduled {
                    index: self.state.cur_num_temporal_scheduled
                }
            }
        );
        self.temporal_sender.send(message).unwrap();
        self.state.cur_num_temporal_scheduled += 1;
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
                let message = self
                    .crypto_processor
                    .wrap_message(payload.as_bytes())
                    .unwrap();
                log!(
                    "MessageEvent",
                    MessageEvent {
                        payload_id: payload.id(),
                        step_id: self.state.step_id,
                        node_id: self.id,
                        event_type: MessageEventType::Created
                    }
                );
                self.schedule_persistent_transmission(SimMessage(message));
            }
        }

        // Handle incoming messages
        for network_message in self.receive() {
            if MockBlendMessage::is_drop_message(&network_message.payload().0) {
                continue;
            }

            self.forward(
                network_message.payload().clone(),
                Some(network_message.from),
            );
            self.handle_incoming_message(network_message.into_payload());
        }

        // Proceed temporal processor
        if let Poll::Ready(Some(msg)) =
            pin!(&mut self.temporal_processor_messages).poll_next(&mut cx)
        {
            log!(
                "MessageEvent",
                MessageEvent {
                    payload_id: match &msg {
                        BlendOutgoingMessage::Outbound(msg) => Self::parse_payload(msg).id(),
                        BlendOutgoingMessage::FullyUnwrapped(payload) =>
                            Payload::load(payload.clone()).id(),
                    },
                    step_id: self.state.step_id,
                    node_id: self.id,
                    event_type: MessageEventType::TemporalProcessorReleased
                }
            );
            self.state.cur_num_temporal_scheduled -= 1;

            // Proceed the message
            match msg {
                BlendOutgoingMessage::Outbound(message) => {
                    self.schedule_persistent_transmission(SimMessage(message));
                }
                BlendOutgoingMessage::FullyUnwrapped(payload) => {
                    let payload = Payload::load(payload);
                    log!(
                        "MessageEvent",
                        MessageEvent {
                            payload_id: payload.id(),
                            step_id: self.state.step_id,
                            node_id: self.id,
                            event_type: MessageEventType::FullyUnwrapped
                        }
                    );
                    self.state.num_messages_fully_unwrapped += 1;
                }
            }
        }

        // Generate a cover message probabilistically
        if let Poll::Ready(Some(_)) = pin!(&mut self.cover_traffic).poll_next(&mut cx) {
            let payload = Payload::new();
            let message = self
                .crypto_processor
                .wrap_message(payload.as_bytes())
                .unwrap();
            log!(
                "MessageEvent",
                MessageEvent {
                    payload_id: payload.id(),
                    step_id: self.state.step_id,
                    node_id: self.id,
                    event_type: MessageEventType::Created
                }
            );
            self.schedule_persistent_transmission(SimMessage(message));
        }

        // Proceed persistent transmission
        if let Poll::Ready(Some(msg)) =
            pin!(&mut self.persistent_transmission_messages).poll_next(&mut cx)
        {
            log!(
                "MessageEvent",
                MessageEvent {
                    payload_id: Self::parse_payload(&msg.0).id(),
                    step_id: self.state.step_id,
                    node_id: self.id,
                    event_type: MessageEventType::PersistentTransmissionReleased
                }
            );
            self.state.cur_num_persistent_scheduled -= 1;
            self.forward(msg, None);
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
