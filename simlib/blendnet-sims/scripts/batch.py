# !/usr/bin/env python
import argparse
import csv
import json
import os
import subprocess
from collections import OrderedDict

import latency
import mixlog


def bandwidth_result(log_path: str, step_duration_ms: int) -> dict[str, float]:
    max_step_id = 0
    for topic, json_msg in mixlog.get_input_stream(log_path):
        if topic == "MessageFullyUnwrapped":
            max_step_id = max(max_step_id, json_msg["message"]["step_id"])

    with open(log_path, "r") as file:
        for line in file:
            if "total_outbound_bandwidth" in line:
                line = line[line.find("{") :]
                line = line.replace("{ ", '{"')
                line = line.replace(": ", '": ')
                line = line.replace(", ", ', "')
                record = json.loads(line)

                elapsed = (max_step_id * step_duration_ms) / 1000.0
                return {
                    "min": float(record["min_node_total_bandwidth"]) / elapsed,
                    "avg": float(record["avg_node_total_bandwidth"]) / elapsed,
                    "max": float(record["max_node_total_bandwidth"]) / elapsed,
                }

    raise Exception("No bandwidth data found in log file")


def topology_result(log_path: str) -> dict[str, int]:
    for topic, json_msg in mixlog.get_input_stream(log_path):
        if topic == "Topology":
            return json_msg
    raise Exception("No topology found in log file")


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Log analysis for nomos-simulations.")
    parser.add_argument(
        "--step-duration",
        type=int,
        help="Duration (in ms) of each step in the simulation.",
    )
    parser.add_argument(
        "--params-file",
        type=str,
        help="A CSV file that contains all parameter sets",
    )
    parser.add_argument(
        "--orig-config-file",
        type=str,
        help="An original blendnet config JSON file that will be modified as specified in `params_file`",
    )
    parser.add_argument(
        "--outdir",
        type=str,
        help="A output directory to be created",
    )
    parser.add_argument(
        "--skip-run",
        action="store_true",
        help="Skip running the simulations and only analyze the logs",
    )
    return parser


if __name__ == "__main__":
    argument_parser = build_argument_parser()
    args = argument_parser.parse_args()

    # Read the params CSV file
    param_sets = []
    param_set_header = []
    with open(args.params_file, mode="r") as csvfile:
        param_set_header = csvfile.readline().strip().split(",")
        csvfile.seek(0)  # Reset file pointer to the beginning after reading the header
        reader = csv.DictReader(csvfile, delimiter=",")
        param_sets = list(reader)

    # Read the original blendnet json config file
    with open(args.orig_config_file, "r") as jsonfile:
        original_json = json.load(jsonfile)

    # Directory to save modified JSON files
    modified_configs_dir = os.path.join(args.outdir, "modified_configs")
    os.makedirs(modified_configs_dir, exist_ok=True)

    # Modify and save JSON files for each row in CSV
    config_paths = []
    for idx, param_set in enumerate(param_sets):
        output_path = os.path.join(modified_configs_dir, f"{idx}.json")
        config_paths.append(output_path)

        if args.skip_run:
            continue

        modified_json = OrderedDict(original_json)  # Preserve original field order

        # Apply modifications
        modified_json["network_settings"]["regions"]["north america west"] = 0.06
        modified_json["network_settings"]["regions"]["north america east"] = 0.15
        modified_json["network_settings"]["regions"]["north america central"] = 0.02
        modified_json["network_settings"]["regions"]["europe"] = 0.47
        modified_json["network_settings"]["regions"]["northern europe"] = 0.10
        modified_json["network_settings"]["regions"]["east asia"] = 0.10
        modified_json["network_settings"]["regions"]["southeast asia"] = 0.07
        modified_json["network_settings"]["regions"]["australia"] = 0.03
        modified_json["step_time"] = f"{args.step_duration}ms"
        modified_json["node_count"] = int(param_set["network_size"])
        modified_json["wards"][0]["sum"] = 1000
        modified_json["connected_peers_count"] = int(param_set["peering_degree"])
        modified_json["data_message_lottery_interval"] = "20s"
        modified_json["stake_proportion"] = 0.0
        modified_json["persistent_transmission"]["max_emission_frequency"] = 1.0
        modified_json["persistent_transmission"]["drop_message_probability"] = 0.0
        modified_json["epoch_duration"] = (
            f"{int(param_set['cover_slots_per_epoch']) * int(param_set['cover_slot_duration'])}s"
        )
        modified_json["slots_per_epoch"] = int(param_set["cover_slots_per_epoch"])
        modified_json["slot_duration"] = f"{param_set['cover_slot_duration']}s"
        modified_json["max_delay_seconds"] = int(param_set["max_temporal_delay"])
        modified_json["number_of_hops"] = int(param_set["blend_hops"])
        modified_json["number_of_blend_layers"] = int(param_set["blend_hops"])

        # Save modified JSON
        with open(output_path, "w") as outfile:
            json.dump(modified_json, outfile, indent=2)
            print("Saved modified JSON to:", output_path)

    # Directory to save logs
    log_dir = os.path.join(args.outdir, "logs")
    os.makedirs(log_dir, exist_ok=True)

    log_paths = []
    for idx, config_path in enumerate(config_paths):
        log_path = f"{log_dir}/{idx}.log"
        log_paths.append(log_path)

        if args.skip_run:
            continue

        with open(log_path, "w") as log_file:
            print(
                f"Running simulation-{idx}: {log_file.name} with config: {config_path}"
            )
            subprocess.run(
                ["../../target/release/blendnet-sims", "--input-settings", config_path],
                stdout=log_file,
            )
            print(f"Simulation-{idx} completed: {log_file.name}")

    print("Analyzing logs...")
    print("=================")

    with open(os.path.join(args.outdir, "output.csv"), "w", newline="") as file:
        print(f"Writing results to: {file.name}")
        csv_writer = csv.writer(file)
        csv_writer.writerow(
            param_set_header
            + [
                "network_diameter",
                "msg_count",
                "min_latency_sec",
                "avg_latency_sec",
                "median_latency_sec",
                "max_latency_sec",
                "min_latency_msg_id",
                "min_latency_msg_persistent_latency_sec",
                "min_latency_msg_persistent_index",
                "min_latency_msg_temporal_latency_sec",
                "min_latency_msg_temporal_index",
                "max_latency_msg_id",
                "max_latency_msg_persistent_latency_sec",
                "max_latency_msg_persistent_index",
                "max_latency_msg_temporal_latency_sec",
                "max_latency_msg_temporal_index",
                "min_conn_latency_sec",
                "avg_conn_latency_sec",
                "med_conn_latency_sec",
                "max_conn_latency_sec",
                "min_bandwidth_kbps",
                "avg_bandwidth_kbps",
                "max_bandwidth_kbps",
            ]
        )

        for idx, log_path in enumerate(log_paths):
            csv_row = []
            csv_row.extend([param_sets[idx][key] for key in param_set_header])

            csv_row.append(topology_result(log_path)["diameter"])

            latency_analysis = latency.LatencyAnalysis.build(
                mixlog.get_input_stream(log_path)
            )
            csv_row.append(latency_analysis.total_messages)
            csv_row.append(float(latency_analysis.min_latency_ms) / 1000.0)
            csv_row.append(float(latency_analysis.avg_latency_ms) / 1000.0)
            csv_row.append(float(latency_analysis.median_latency_ms) / 1000.0)
            csv_row.append(float(latency_analysis.max_latency_ms) / 1000.0)
            csv_row.append(latency_analysis.min_latency_analysis.message_id)
            csv_row.append(
                ",".join(
                    map(
                        str,
                        [
                            ms / 1000.0
                            for ms in latency_analysis.min_latency_analysis.persistent_latencies_ms
                        ],
                    )
                )
            )
            csv_row.append(
                ",".join(
                    map(str, latency_analysis.min_latency_analysis.persistent_indices)
                )
            )
            csv_row.append(
                ",".join(
                    map(
                        str,
                        [
                            ms / 1000.0
                            for ms in latency_analysis.min_latency_analysis.temporal_latencies_ms
                        ],
                    )
                )
            )
            csv_row.append(
                ",".join(
                    map(str, latency_analysis.min_latency_analysis.temporal_indices)
                )
            )
            csv_row.append(latency_analysis.max_latency_analysis.message_id)
            csv_row.append(
                ",".join(
                    map(
                        str,
                        [
                            ms / 1000.0
                            for ms in latency_analysis.max_latency_analysis.persistent_latencies_ms
                        ],
                    )
                )
            )
            csv_row.append(
                ",".join(
                    map(str, latency_analysis.max_latency_analysis.persistent_indices)
                )
            )
            csv_row.append(
                ",".join(
                    map(
                        str,
                        [
                            ms / 1000.0
                            for ms in latency_analysis.max_latency_analysis.temporal_latencies_ms
                        ],
                    )
                )
            )
            csv_row.append(
                ",".join(
                    map(str, latency_analysis.max_latency_analysis.temporal_indices)
                )
            )
            csv_row.append(
                float(latency_analysis.conn_latency_analysis.min_ms) / 1000.0
            )
            csv_row.append(
                float(latency_analysis.conn_latency_analysis.avg_ms) / 1000.0
            )
            csv_row.append(
                float(latency_analysis.conn_latency_analysis.med_ms) / 1000.0
            )
            csv_row.append(
                float(latency_analysis.conn_latency_analysis.max_ms) / 1000.0
            )

            bandwidth_res = bandwidth_result(log_path, args.step_duration)
            csv_row.append(bandwidth_res["min"] * 8 / 1000.0)
            csv_row.append(bandwidth_res["avg"] * 8 / 1000.0)
            csv_row.append(bandwidth_res["max"] * 8 / 1000.0)

            csv_writer.writerow(csv_row)

        print(f"The outputs have been successfully written to {file.name}")
