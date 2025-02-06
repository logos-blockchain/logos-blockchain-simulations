import json
import sys
from collections.abc import Iterable
from typing import Optional

TOPIC_LOG_INDICATOR = '{"topic":'


def line_to_json_stream(record_stream: Iterable[str]) -> Iterable[tuple[str, dict]]:
    for record in record_stream:
        topic_idx = record.find(TOPIC_LOG_INDICATOR)
        if topic_idx == -1:
            continue

        # Split the line into 2 parts: topic and JSON message
        log = json.loads(record[topic_idx:].strip())
        yield (log["topic"], log["message"])


def get_pipe_stream() -> Iterable[str]:
    yield from sys.stdin


def get_file_stream(input_filename) -> Iterable[str]:
    with open(input_filename, "r") as file:
        yield from file


def get_input_stream(input_filename: Optional[str]) -> Iterable[tuple[str, dict]]:
    stream = (
        get_file_stream(input_filename)
        if input_filename is not None
        else get_pipe_stream()
    )
    return line_to_json_stream(stream)
