# !/usr/bin/env python
import argparse
import json
from dataclasses import asdict, dataclass
from typing import Iterable

import mixlog
import pandas as pd


@dataclass
class LatencyAnalysis:
    total_messages: int
    min_latency_ms: int
    min_latency_analysis: "MessageLatencyAnalysis"
    max_latency_ms: int
    max_latency_analysis: "MessageLatencyAnalysis"
    avg_latency_ms: int
    median_latency_ms: int
    conn_latency_analysis: "ConnectionLatencyAnalysis"

    @classmethod
    def build(cls, input_stream: Iterable[tuple[str, dict]]) -> "LatencyAnalysis":
        latencies = []
        message_ids = []
        messages: dict[str, dict] = {}
        for topic, record in input_stream:
            if topic != "MessageFullyUnwrapped":
                continue
            latencies.append(record["total_duration"])
            message_id = record["message"]["payload_id"]
            message_ids.append(message_id)
            messages[message_id] = record

        latencies = pd.Series(latencies)
        message_ids = pd.Series(message_ids)

        return cls(
            total_messages=int(latencies.count()),
            min_latency_ms=int(latencies.min()),
            min_latency_analysis=MessageLatencyAnalysis.build(
                messages[str(message_ids[latencies.idxmin()])]
            ),
            max_latency_ms=int(latencies.max()),
            max_latency_analysis=MessageLatencyAnalysis.build(
                messages[str(message_ids[latencies.idxmax()])]
            ),
            avg_latency_ms=int(latencies.mean()),
            median_latency_ms=int(latencies.median()),
            conn_latency_analysis=ConnectionLatencyAnalysis.build(messages),
        )


@dataclass
class MessageLatencyAnalysis:
    message_id: str
    persistent_latencies_ms: list[int]
    persistent_indices: list[int]
    temporal_latencies_ms: list[int]
    temporal_indices: list[int]

    @classmethod
    def build(
        cls,
        message: dict,
    ) -> "MessageLatencyAnalysis":
        analysis = cls(message["message"]["payload_id"], [], [], [], [])

        for event in message["history"]:
            event_type = event["event_type"]
            if "PersistentTransmissionScheduled" in event_type:
                analysis.persistent_indices.append(
                    int(event_type["PersistentTransmissionScheduled"]["index"])
                )
            elif event_type == "PersistentTransmissionReleased":
                analysis.persistent_latencies_ms.append(event["duration_from_prev"])
            elif "TemporalProcessorScheduled" in event_type:
                analysis.temporal_indices.append(
                    int(event_type["TemporalProcessorScheduled"]["index"])
                )
            elif event_type == "TemporalProcessorReleased":
                analysis.temporal_latencies_ms.append(event["duration_from_prev"])

        return analysis


@dataclass
class ConnectionLatencyAnalysis:
    min_ms: int
    avg_ms: int
    med_ms: int
    max_ms: int

    @classmethod
    def build(
        cls,
        messages: dict[str, dict],
    ) -> "ConnectionLatencyAnalysis":
        latencies = []
        for message in messages.values():
            for event in message["history"]:
                if "NetworkReceived" in event["event_type"]:
                    latencies.append(event["duration_from_prev"])
        latencies = pd.Series(latencies)
        return cls(
            int(latencies.min()),
            int(latencies.mean()),
            int(latencies.median()),
            int(latencies.max()),
        )


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Log analysis for nomos-simulations.")
    parser.add_argument(
        "--step-duration",
        type=int,
        default=100,
        help="Duration (in ms) of each step in the simulation.",
    )
    parser.add_argument(
        "input_file",
        nargs="?",
        help="The file to parse. If not provided, input will be read from stdin.",
    )
    return parser


if __name__ == "__main__":
    argument_parser = build_argument_parser()
    arguments = argument_parser.parse_args()

    analysis = LatencyAnalysis.build(mixlog.get_input_stream(arguments.input_file))
    print(json.dumps(asdict(analysis), indent=2))
