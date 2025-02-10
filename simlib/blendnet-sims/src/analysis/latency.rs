use core::panic;
use std::{error::Error, ops::Mul, path::PathBuf, time::Duration};

use std::{
    collections::HashMap,
    fs::File,
    io::{BufRead, BufReader},
};

use netrunner::node::NodeId;
use polars::prelude::{AnyValue, NamedFrom, QuantileMethod, Scalar};
use polars::series::Series;
use serde::{Deserialize, Serialize};

use crate::node::blend::log::TopicLog;
use crate::node::blend::message::{MessageEvent, MessageEventType, PayloadId};

pub fn analyze_latency(log_file: PathBuf, step_duration: Duration) -> Result<(), Box<dyn Error>> {
    let output = Output {
        message: analyze_message_latency(log_file.clone(), step_duration)?,
        connection: analyze_connection_latency(log_file.clone(), step_duration)?,
    };
    println!("{}", serde_json::to_string(&output)?);
    Ok(())
}

#[derive(Serialize, Deserialize, Debug)]
struct Output {
    message: MessageLatency,
    connection: ConnectionLatency,
}

fn analyze_message_latency(
    log_file: PathBuf,
    step_duration: Duration,
) -> Result<MessageLatency, Box<dyn Error>> {
    let file = File::open(log_file)?;
    let reader = BufReader::new(file);

    let mut messages: HashMap<PayloadId, usize> = HashMap::new();
    let mut latencies_ms: Vec<i64> = Vec::new();
    let mut latency_to_message: HashMap<i64, PayloadId> = HashMap::new();

    for line in reader.lines() {
        let line = line?;
        if let Ok(topic_log) = serde_json::from_str::<TopicLog<MessageEvent>>(&line) {
            assert_eq!(topic_log.topic, "MessageEvent");
            let event = topic_log.message;
            match event.event_type {
                MessageEventType::Created => {
                    assert_eq!(messages.insert(event.payload_id, event.step_id), None);
                }
                MessageEventType::FullyUnwrapped => match messages.remove(&event.payload_id) {
                    Some(created_step_id) => {
                        let latency = step_duration
                            .mul((event.step_id - created_step_id).try_into().unwrap())
                            .as_millis()
                            .try_into()
                            .unwrap();
                        latencies_ms.push(latency);
                        latency_to_message.insert(latency, event.payload_id);
                    }
                    None => {
                        panic!(
                            "FullyUnwrapped event without Created event: {}",
                            event.payload_id
                        );
                    }
                },
                _ => {
                    continue;
                }
            }
        }
    }

    let series = Series::new("latencies".into(), latencies_ms);
    Ok(MessageLatency::new(&series, &latency_to_message))
}

#[derive(Serialize, Deserialize, Debug)]
struct MessageLatency {
    count: usize,
    min: i64,
    min_payload_id: PayloadId,
    q1: f64,
    avg: f64,
    med: f64,
    q3: f64,
    max: i64,
    max_payload_id: PayloadId,
}

impl MessageLatency {
    fn new(series: &Series, latency_to_message: &HashMap<i64, PayloadId>) -> Self {
        let min = series.min::<i64>().unwrap().unwrap();
        let min_payload_id = latency_to_message.get(&min).unwrap().clone();
        let max = series.max::<i64>().unwrap().unwrap();
        let max_payload_id = latency_to_message.get(&max).unwrap().clone();
        Self {
            count: series.len(),
            min,
            min_payload_id,
            q1: quantile(series, 0.25),
            avg: series.mean().unwrap(),
            med: series.median().unwrap(),
            q3: quantile(series, 0.75),
            max,
            max_payload_id,
        }
    }
}

fn analyze_connection_latency(
    log_file: PathBuf,
    step_duration: Duration,
) -> Result<ConnectionLatency, Box<dyn Error>> {
    let file = File::open(log_file)?;
    let reader = BufReader::new(file);

    let mut sent_events: HashMap<(PayloadId, NodeId, NodeId), usize> = HashMap::new();
    let mut latencies_ms: Vec<i64> = Vec::new();

    for line in reader.lines() {
        let line = line?;
        if let Ok(topic_log) = serde_json::from_str::<TopicLog<MessageEvent>>(&line) {
            assert_eq!(topic_log.topic, "MessageEvent");
            let event = topic_log.message;
            match event.event_type {
                MessageEventType::NetworkSent { to } => {
                    sent_events
                        .entry((event.payload_id, event.node_id, to))
                        .or_insert(event.step_id);
                }
                MessageEventType::NetworkReceived { from } => {
                    if let Some(sent_step_id) =
                        sent_events.remove(&(event.payload_id, from, event.node_id))
                    {
                        let latency = step_duration
                            .mul((event.step_id - sent_step_id).try_into().unwrap())
                            .as_millis()
                            .try_into()
                            .unwrap();
                        latencies_ms.push(latency);
                    }
                }
                _ => {
                    continue;
                }
            }
        }
    }

    let series = Series::new("latencies".into(), latencies_ms);
    Ok(ConnectionLatency::new(&series))
}

#[derive(Serialize, Deserialize, Debug)]
struct ConnectionLatency {
    count: usize,
    min: i64,
    q1: f64,
    avg: f64,
    med: f64,
    q3: f64,
    max: i64,
}

impl ConnectionLatency {
    fn new(series: &Series) -> Self {
        Self {
            count: series.len(),
            min: series.min::<i64>().unwrap().unwrap(),
            q1: quantile(series, 0.25),
            avg: series.mean().unwrap(),
            med: series.median().unwrap(),
            q3: quantile(series, 0.75),
            max: series.max::<i64>().unwrap().unwrap(),
        }
    }
}

fn quantile(series: &Series, quantile: f64) -> f64 {
    f64_from_scalar(
        &series
            .quantile_reduce(quantile, QuantileMethod::Linear)
            .unwrap(),
    )
}

fn f64_from_scalar(scalar: &Scalar) -> f64 {
    match scalar.value() {
        AnyValue::Float64(value) => *value,
        _ => panic!("Expected f64"),
    }
}
