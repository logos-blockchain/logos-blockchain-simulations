use nomos_tracing::{
    logging::local::{create_file_layer, create_writer_layer},
    metrics::otlp::{create_otlp_metrics_layer, OtlpMetricsConfig},
};
use std::{path::PathBuf, str::FromStr};
use tracing::{level_filters::LevelFilter, subscriber::DefaultGuard, Level};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Default, Copy, Clone)]
pub enum LogFormat {
    #[default]
    Plain,
    Json,
}

impl FromStr for LogFormat {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "json" => Ok(LogFormat::Json),
            "plain" => Ok(LogFormat::Plain),
            _ => Err(anyhow::anyhow!("Unknown log format")),
        }
    }
}

#[derive(Default, Clone)]
pub enum LogOutput {
    #[default]
    StdOut,
    StdErr,
    File(PathBuf),
}

impl FromStr for LogOutput {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "stdout" => Ok(Self::StdOut),
            "stderr" => Ok(Self::StdErr),
            path => Ok(Self::File(PathBuf::from(path))),
        }
    }
}

pub fn config_tracing(
    _fmt: LogFormat,
    log_to: &LogOutput,
    with_metrics: bool,
) -> Option<(WorkerGuard, DefaultGuard)> {
    let mut layers: Vec<Box<dyn tracing_subscriber::Layer<_> + Send + Sync>> = vec![];

    let (log_layer, guard) = match log_to {
        LogOutput::StdOut => create_writer_layer(std::io::stdout()),
        LogOutput::StdErr => create_writer_layer(std::io::stderr()),
        LogOutput::File(path) => create_file_layer(nomos_tracing::logging::local::FileConfig {
            directory: path.parent().unwrap().to_owned(),
            prefix: None,
        }),
    };

    layers.push(Box::new(log_layer));

    if with_metrics {
        let metrics_layer = create_otlp_metrics_layer(OtlpMetricsConfig {
            endpoint: "http://127.0.0.1:9090/api/v1/otlp/v1/metrics"
                .try_into()
                .unwrap(),
            host_identifier: "network_simulator".to_string(),
        })
        .unwrap();
        layers.push(Box::new(metrics_layer));
    }

    let a = tracing_subscriber::registry()
        .with(LevelFilter::from(Level::INFO))
        .with(layers)
        .set_default();

    Some((guard, a))
}
