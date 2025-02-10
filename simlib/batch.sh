#!/bin/bash

set -euxo pipefail

CONFIG_DIR="./blendnet-sims/config"
OUT_DIR="./out"

if [[ ! -d $CONFIG_DIR ]]; then
    echo "Config directory not found: $CONFIG_DIR"
    exit 1
fi

if [[ -d $OUT_DIR ]]; then
    echo "outdir already exists: $OUT_DIR"
    exit 1
fi

mkdir $OUT_DIR

for config_file in "$CONFIG_DIR"/*.json; do
    # Skip if no JSON files exist
    [[ -e "$config_file" ]] || continue

    # Extract filename without extension
    filename=$(basename -- "$config_file" .json)

    log_path=${OUT_DIR}/${filename}.log
    cargo run --release -- run --input-settings $config_file > $log_path

    step_duration=$(cat $config_file | jq '.step_time' | sed 's|"||g')
    latency_output_path=${OUT_DIR}/${filename}_latency.json
    cargo run --release -- analyze latency --log-file ${log_path} --step-duration ${step_duration} | jq > $latency_output_path

    min_payload_id=$(cat $latency_output_path | jq '.message.min_payload_id' | sed 's|"||g')
    min_history_path=${OUT_DIR}/${filename}_min_history.json
    cargo run --release -- analyze message-history --log-file ${log_path} --step-duration ${step_duration} --payload-id ${min_payload_id} | jq > $min_history_path
    max_payload_id=$(cat $latency_output_path | jq '.message.max_payload_id' | sed 's|"||g')
    max_history_path=${OUT_DIR}/${filename}_max_history.json
    cargo run --release -- analyze message-history --log-file ${log_path} --step-duration ${step_duration} --payload-id ${max_payload_id} | jq > $max_history_path
done
