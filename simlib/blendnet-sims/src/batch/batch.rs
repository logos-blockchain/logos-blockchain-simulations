use std::{fs, path::PathBuf, time::Duration};

use clap::Parser;
use serde::Deserialize;

use crate::{load_json_from_file, settings::SimSettings, SimulationApp};

#[derive(Parser)]
pub struct BatchApp {
    #[clap(long, short)]
    paramset_file: PathBuf,
    #[clap(long, short)]
    default_config_file: PathBuf,
    #[clap(long, short)]
    outdir: PathBuf,
}

impl BatchApp {
    pub fn run(&self) -> anyhow::Result<()> {
        self.prepare_config_files()?
            .into_iter()
            .try_for_each(|(config_path, log_path)| self.run_simulation(config_path, log_path))?;

        Ok(())
    }

    fn prepare_config_files(&self) -> anyhow::Result<Vec<(PathBuf, PathBuf)>> {
        let default_settings: SimSettings = load_json_from_file(&self.default_config_file)?;
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(true)
            .delimiter(b',')
            .from_path(&self.paramset_file)?;

        fs::create_dir_all(self.outdir.clone())?;

        let mut config_and_log_paths = Vec::new();
        for (i, record) in reader.deserialize().enumerate() {
            let paramset: ParamSetRecord = record?;
            let mut settings = default_settings.clone();
            paramset.apply_to(&mut settings);

            let dir_path = self.outdir.join(format!("sim-{}", i));
            fs::create_dir_all(dir_path.clone())?;

            let config_path = dir_path.join("config.json");
            serde_json::to_writer_pretty(fs::File::create(config_path.clone())?, &settings)?;
            println!("Wrote {}", config_path.display());

            config_and_log_paths.push((config_path, dir_path.join("out.log")));
        }
        Ok(config_and_log_paths)
    }

    fn run_simulation(&self, config_path: PathBuf, log_path: PathBuf) -> anyhow::Result<()> {
        println!(
            "Running simulation with config file {}",
            config_path.display()
        );
        let app = SimulationApp {
            input_settings: config_path,
            stream_type: None,
            log_format: crate::log::LogFormat::Plain,
            log_to: crate::log::LogOutput::File(log_path.clone()),
            no_netcap: false,
            with_metrics: false,
        };

        let maybe_guard = crate::log::config_tracing(app.log_format, &app.log_to, app.with_metrics);
        let result = app.run();
        if result.is_ok() {
            println!(
                "Simulation finished successfully, logs written to {}",
                log_path.display()
            );
        }
        drop(maybe_guard);
        result
    }
}

#[derive(Debug, Deserialize)]
struct ParamSetRecord {
    network_size: usize,
    peering_degree: usize,
    blend_hops: usize,
    cover_slots_per_epoch: usize,
    cover_slot_duration: usize,
    max_temporal_delay: usize,
}

impl ParamSetRecord {
    fn apply_to(&self, settings: &mut SimSettings) {
        settings.simulation_settings.node_count = self.network_size;
        settings.connected_peers_count = self.peering_degree;
        settings.number_of_hops = self.blend_hops;
        settings.number_of_blend_layers = self.blend_hops;
        settings.epoch_duration = Duration::from_secs(
            (self.cover_slots_per_epoch * self.cover_slot_duration)
                .try_into()
                .unwrap(),
        );
        settings.slots_per_epoch = self.cover_slots_per_epoch;
        settings.slot_duration = Duration::from_secs(self.cover_slot_duration.try_into().unwrap());
        settings.max_delay_seconds = self.max_temporal_delay.try_into().unwrap();
    }
}
