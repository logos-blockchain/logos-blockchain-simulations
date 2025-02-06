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
use message::{duration_as_millis, MessageEventType, MessageHistory, Payload, PayloadId};
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

#[derive(Debug, Clone, Serialize)]
pub struct BlendMessage {
    message: Vec<u8>,
    history: MessageHistory,
}

impl BlendMessage {
    fn new(message: Vec<u8>, node_id: NodeId, step_id: usize, step_time: Duration) -> Self {
        let mut history = MessageHistory::new();
        history.add(node_id, step_id, step_time, MessageEventType::Created);
        Self { message, history }
    }

    fn new_drop() -> Self {
        Self {
            message: Vec::new(),
            history: MessageHistory::new(),
        }
    }

    fn is_drop(&self) -> bool {
        self.message.is_empty()
    }
}

impl PayloadSize for BlendMessage {
    fn size_bytes(&self) -> u32 {
        2208
    }
}

struct BlendOutgoingMessageWithHistory {
    outgoing_message: BlendOutgoingMessage,
    history: MessageHistory,
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

type Sha256Hash = [u8; 32];

/// This node implementation only used for testing different streaming implementation purposes.
pub struct BlendNode {
    id: NodeId,
    state: BlendnodeState,
    settings: BlendnodeSettings,
    step_time: Duration,
    network_interface: InMemoryNetworkInterface<BlendMessage>,
    message_cache: TimedCache<Sha256Hash, ()>,

    data_msg_lottery_update_time_sender: channel::Sender<Duration>,
    data_msg_lottery_interval: Interval,
    data_msg_lottery: StakeLottery<ChaCha12Rng>,

    persistent_sender: channel::Sender<BlendMessage>,
    persistent_update_time_sender: channel::Sender<Duration>,
    persistent_transmission_messages:
        PersistentTransmissionStream<CrossbeamReceiverStream<BlendMessage>, ChaCha12Rng, Interval>,

    crypto_processor: CryptographicProcessor<NodeId, ChaCha12Rng, MockBlendMessage>,
    temporal_sender: channel::Sender<BlendOutgoingMessageWithHistory>,
    temporal_update_time_sender: channel::Sender<Duration>,
    temporal_processor_messages:
        TemporalStream<CrossbeamReceiverStream<BlendOutgoingMessageWithHistory>, TemporalScheduler>,

    epoch_update_sender: channel::Sender<Duration>,
    slot_update_sender: channel::Sender<Duration>,
    cover_traffic: CoverTraffic<Epoch, Slot, MockBlendMessage>,
}

impl BlendNode {
    pub fn new(
        id: NodeId,
        settings: BlendnodeSettings,
        step_time: Duration,
        network_interface: InMemoryNetworkInterface<BlendMessage>,
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
        let (persistent_sender, persistent_receiver) = channel::unbounded::<BlendMessage>();
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
                BlendMessage::new_drop(),
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
            step_time,
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

    fn forward(&mut self, message: BlendMessage, exclude_node: Option<NodeId>) {
        for node_id in self
            .settings
            .connected_peers
            .iter()
            .filter(|&id| Some(*id) != exclude_node)
        {
            let mut message = message.clone();
            self.record_network_sent_event(&mut message.history, *node_id);
            self.network_interface.send_message(*node_id, message)
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

    fn schedule_persistent_transmission(&mut self, mut message: BlendMessage) {
        self.record_persistent_scheduled_event(&mut message.history);
        self.persistent_sender.send(message).unwrap();
        self.state.cur_num_persistent_scheduled += 1;
    }

    fn handle_incoming_message(&mut self, message: BlendMessage) {
        match self.crypto_processor.unwrap_message(&message.message) {
            Ok((unwrapped_message, fully_unwrapped)) => {
                let temporal_message = if fully_unwrapped {
                    BlendOutgoingMessage::FullyUnwrapped(unwrapped_message)
                } else {
                    BlendOutgoingMessage::Outbound(unwrapped_message)
                };

                self.schedule_temporal_processor(BlendOutgoingMessageWithHistory {
                    outgoing_message: temporal_message,
                    history: message.history,
                });
            }
            Err(e) => {
                tracing::debug!("Failed to unwrap message: {:?}", e);
            }
        }
    }

    fn schedule_temporal_processor(&mut self, mut message: BlendOutgoingMessageWithHistory) {
        self.record_temporal_scheduled_event(&mut message.history);
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

    fn log_message_fully_unwrapped(&self, payload: &Payload, history: MessageHistory) {
        let total_duration = history.total_duration();
        log!(
            "MessageFullyUnwrapped",
            MessageWithHistoryLog {
                message: MessageLog {
                    payload_id: payload.id(),
                    step_id: self.state.step_id,
                    node_id: self.id.index(),
                },
                history,
                total_duration,
            }
        );
    }

    fn new_blend_message(&self, message: Vec<u8>) -> BlendMessage {
        BlendMessage::new(message, self.id, self.state.step_id, self.step_time)
    }

    fn record_network_sent_event(&self, history: &mut MessageHistory, to: NodeId) {
        self.record_message_event(history, MessageEventType::NetworkSent { to });
    }

    fn record_network_received_event(&self, history: &mut MessageHistory, from: NodeId) {
        assert_eq!(
            history.last_event_type(),
            Some(&MessageEventType::NetworkSent { to: self.id })
        );
        self.record_message_event(history, MessageEventType::NetworkReceived { from });
    }

    fn record_persistent_scheduled_event(&self, history: &mut MessageHistory) {
        self.record_message_event(
            history,
            MessageEventType::PersistentTransmissionScheduled {
                index: self.state.cur_num_persistent_scheduled,
            },
        );
    }

    fn record_persistent_released_event(&self, history: &mut MessageHistory) {
        assert!(matches!(
            history.last_event_type(),
            Some(MessageEventType::PersistentTransmissionScheduled { .. })
        ));
        self.record_message_event(history, MessageEventType::PersistentTransmissionReleased);
    }

    fn record_temporal_scheduled_event(&self, history: &mut MessageHistory) {
        self.record_message_event(
            history,
            MessageEventType::TemporalProcessorScheduled {
                index: self.state.cur_num_temporal_scheduled,
            },
        );
    }

    fn record_temporal_released_event(&self, history: &mut MessageHistory) {
        assert!(matches!(
            history.last_event_type(),
            Some(MessageEventType::TemporalProcessorScheduled { .. })
        ));
        self.record_message_event(history, MessageEventType::TemporalProcessorReleased);
    }

    fn record_message_event(&self, history: &mut MessageHistory, event_type: MessageEventType) {
        history.add(self.id, self.state.step_id, self.step_time, event_type);
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
                self.schedule_persistent_transmission(self.new_blend_message(message));
            }
        }

        // Handle incoming messages
        for mut network_message in self.receive() {
            self.record_network_received_event(
                &mut network_message.payload.history,
                network_message.from,
            );

            if network_message.payload().is_drop() {
                continue;
            }

            self.forward(
                network_message.payload().clone(),
                Some(network_message.from),
            );
            self.handle_incoming_message(network_message.into_payload());
        }

        // Proceed temporal processor
        if let Poll::Ready(Some(mut outgoing_msg_with_history)) =
            pin!(&mut self.temporal_processor_messages).poll_next(&mut cx)
        {
            self.record_temporal_released_event(&mut outgoing_msg_with_history.history);
            self.state.cur_num_temporal_scheduled -= 1;

            // Proceed the message
            match outgoing_msg_with_history.outgoing_message {
                BlendOutgoingMessage::Outbound(message) => {
                    self.schedule_persistent_transmission(BlendMessage {
                        message,
                        history: outgoing_msg_with_history.history,
                    });
                }
                BlendOutgoingMessage::FullyUnwrapped(payload) => {
                    let payload = Payload::load(payload);
                    self.log_message_fully_unwrapped(&payload, outgoing_msg_with_history.history);
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
            self.schedule_persistent_transmission(self.new_blend_message(message));
        }

        // Proceed persistent transmission
        if let Poll::Ready(Some(mut msg)) =
            pin!(&mut self.persistent_transmission_messages).poll_next(&mut cx)
        {
            self.record_persistent_released_event(&mut msg.history);
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

#[derive(Debug, Serialize)]
struct MessageLog {
    payload_id: PayloadId,
    step_id: usize,
    node_id: usize,
}

#[derive(Debug, Serialize)]
struct MessageWithHistoryLog {
    message: MessageLog,
    history: MessageHistory,
    #[serde(serialize_with = "duration_as_millis")]
    total_duration: Duration,
}
