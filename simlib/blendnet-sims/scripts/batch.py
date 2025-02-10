# !/usr/bin/env python
import argparse
import csv
import json
import os
import subprocess
import sys
from collections import OrderedDict

import mixlog

SIM_APP = "../../target/release/blendnet-sims"


def bandwidth_result(log_path: str, step_duration_ms: int) -> dict[str, float]:
    max_step_id = 0
    for topic, json_msg in mixlog.get_input_stream(log_path):
        if topic == "MessageEvent":
            max_step_id = max(max_step_id, json_msg["step_id"])

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
        if param_set["regions"] == "all":
            modified_json["network_settings"]["regions"]["north america west"] = 0.06
            modified_json["network_settings"]["regions"]["north america east"] = 0.15
            modified_json["network_settings"]["regions"]["north america central"] = 0.02
            modified_json["network_settings"]["regions"]["europe"] = 0.47
            modified_json["network_settings"]["regions"]["northern europe"] = 0.10
            modified_json["network_settings"]["regions"]["east asia"] = 0.10
            modified_json["network_settings"]["regions"]["southeast asia"] = 0.07
            modified_json["network_settings"]["regions"]["australia"] = 0.03
        elif param_set["regions"] == "eu":
            modified_json["network_settings"]["regions"]["north america west"] = 0.00
            modified_json["network_settings"]["regions"]["north america east"] = 0.00
            modified_json["network_settings"]["regions"]["north america central"] = 0.00
            modified_json["network_settings"]["regions"]["europe"] = 1.00
            modified_json["network_settings"]["regions"]["northern europe"] = 0.00
            modified_json["network_settings"]["regions"]["east asia"] = 0.00
            modified_json["network_settings"]["regions"]["southeast asia"] = 0.00
            modified_json["network_settings"]["regions"]["australia"] = 0.00
        else:
            sys.exit("Invalid region")

        modified_json["step_time"] = f"{args.step_duration}ms"
        modified_json["node_count"] = int(param_set["network_size"])
        modified_json["wards"][0]["sum"] = int(param_set["target_message_count"])
        modified_json["connected_peers_count"] = int(param_set["peering_degree"])
        modified_json["data_message_lottery_interval"] = "20s"
        modified_json["stake_proportion"] = 0.0
        modified_json["persistent_transmission"]["max_emission_frequency"] = float(
            param_set["max_emission_frequency"]
        )
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

        if args.skip_run or os.path.exists(log_path):
            print(f"Skipping simulation: {log_path}")
            continue

        with open(log_path, "w") as log_file:
            print(
                f"Running simulation-{idx}: {log_file.name} with config: {config_path}"
            )
            subprocess.run(
                [
                    SIM_APP,
                    "run",
                    "--input-settings",
                    config_path,
                ],
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
                "25th_latency_sec",
                "avg_latency_sec",
                "median_latency_sec",
                "75th_latency_sec",
                "max_latency_sec",
                "min_latency_msg_id",
                "max_latency_msg_id",
                "conn_latency_count",
                "min_conn_latency_sec",
                "25th_conn_latency_sec",
                "avg_conn_latency_sec",
                "med_conn_latency_sec",
                "75th_conn_latency_sec",
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

            result = subprocess.run(
                [
                    SIM_APP,
                    "analyze",
                    "latency",
                    "--log-file",
                    log_path,
                    "--step-duration",
                    f"{args.step_duration}ms",
                ],
                capture_output=True,
                text=True,
            )
            assert result.returncode == 0
            result = json.loads(result.stdout)

            csv_row.append(result["message"]["count"])
            csv_row.append(float(result["message"]["min"]) / 1000.0)
            csv_row.append(float(result["message"]["q1"]) / 1000.0)
            csv_row.append(float(result["message"]["avg"]) / 1000.0)
            csv_row.append(float(result["message"]["med"]) / 1000.0)
            csv_row.append(float(result["message"]["q3"]) / 1000.0)
            csv_row.append(float(result["message"]["max"]) / 1000.0)
            csv_row.append(result["message"]["min_payload_id"])
            csv_row.append(result["message"]["max_payload_id"])

            csv_row.append(result["connection"]["count"])
            csv_row.append(float(result["connection"]["min"]) / 1000.0)
            csv_row.append(float(result["connection"]["q1"]) / 1000.0)
            csv_row.append(float(result["connection"]["avg"]) / 1000.0)
            csv_row.append(float(result["connection"]["med"]) / 1000.0)
            csv_row.append(float(result["connection"]["q3"]) / 1000.0)
            csv_row.append(float(result["connection"]["max"]) / 1000.0)

            bandwidth_res = bandwidth_result(log_path, args.step_duration)
            csv_row.append(bandwidth_res["min"] * 8 / 1000.0)
            csv_row.append(bandwidth_res["avg"] * 8 / 1000.0)
            csv_row.append(bandwidth_res["max"] * 8 / 1000.0)

            csv_writer.writerow(csv_row)

        print(f"The outputs have been successfully written to {file.name}")
