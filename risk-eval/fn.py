#!/usr/bin/env python3

import json
import typing
import logging

from call_next_func import post_threshold
from timestamp_for_logger import CustomFormatter
from tracer import TracerInitializer
from collision_detector import detect_collisions

# Set up Python logger. milliseconds are not supported by default
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S.%f'
)
logger = logging.getLogger(__name__)
for handler in logging.getLogger().handlers:  # Apply the custom formatter to the root logger
    handler.setFormatter(CustomFormatter(handler.formatter._fmt, handler.formatter.datefmt))

# Initialize the OpenTelemetry tracer
tracer = TracerInitializer("risk-eval").tracer

def fn(input: typing.Optional[str]) -> typing.Optional[str]:
    """
    input: A JSON string that represents a dictionary with trajectory set 'data' and 'meta' keys.
    output: calls the threshold function with the risk evaluation result
    """
    with tracer.start_span('parse_input', attributes={"invoke_count": Counter.increment_count()}):
        logger.info(f'[risk-eval fn] invoke count: {str(Counter.get_count())}')
        # Parse the JSON string into a Python list of dictionaries
        parsed_input = json.loads(input)
        logger.debug(f'[risk-eval fn] Parsed input: {parsed_input}')

        data = parsed_input.get('data', [])
        meta = parsed_input.get('meta', {})

    # Call the sample function with the parsed input
    with tracer.start_span('detect_collisions', attributes={"invoke_count": Counter.get_count()}):
        # TODO: get parameters from ENV
        result = detect_collisions(data, time_interval=1, num_steps=10, horizontal_separation=5, vertical_separation=300)
        logger.info(f'[risk-eval fn] Result of collision detection: {result}')

    # calls to :8000/threshold
    with tracer.start_span('post_threshold', attributes={"invoke_count": Counter.get_count()}):
        post_threshold(data, meta, result)

    return str(result)  # for debugging purposes


class Counter:
    count = None

    @staticmethod
    def get_count():
        if Counter.count is None:  # memoize
            Counter.count = 0
        return Counter.count

    @staticmethod
    def increment_count():
        if Counter.count is None:
            Counter.count = 0
        Counter.count += 1
        return Counter.count
