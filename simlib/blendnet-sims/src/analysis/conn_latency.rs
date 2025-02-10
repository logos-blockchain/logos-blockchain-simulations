use std::{error::Error, ops::Mul, path::PathBuf, time::Duration};

use std::{
    collections::HashMap,
    fs::File,
    io::{BufRead, BufReader},
};

use netrunner::node::NodeId;
use polars::prelude::NamedFrom;
use polars::series::Series;
use serde::{Deserialize, Serialize};

use crate::node::blend::log::TopicLog;
use crate::node::blend::message::{MessageEvent, MessageEventType, PayloadId};

use super::message_latency::quantile;

pub fn analyze_connection_latency(
    log_file: PathBuf,
    step_duration: Duration,
) -> Result<(), Box<dyn Error>> {
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
    let series = Output::new(&series);
    println!("{}", serde_json::to_string(&series).unwrap());

    Ok(())
}

#[derive(Serialize, Deserialize, Debug)]
struct Output {
    count: usize,
    min: i64,
    q1: f64,
    avg: f64,
    med: f64,
    q3: f64,
    max: i64,
}

impl Output {
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
