#!/usr/bin/env python3

import json
import typing
import logging

from call_next_func import post_mutate, post_release
from timestamp_for_logger import CustomFormatter
from tracer import TracerInitializer

threshold = 0.5  #TODO: import from ENV

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
tracer = TracerInitializer("threshold").tracer

def fn(input: typing.Optional[str]) -> typing.Optional[str]:
    """
    input: A JSON string that represents a dictionary with trajectory set 'data' and 'meta' (incl. 'risk' value)
    keys. output: calls the mutate function if the risk is above the threshold, otherwise based on 'origin' metadata,
    either calls the simulation function or does nothing
    """
    with tracer.start_span('parse_input', attributes={"invoke_count": Counter.increment_count()}):
        logger.info(f'[threshold fn] invoke count: {str(Counter.get_count())}')
        # Parse the JSON string into a Python list of dictionaries
        parsed_input = json.loads(input)
        logger.debug(f'[threshold fn] Parsed input: {parsed_input}')

        # data = parsed_input.get('data', [])
        meta = parsed_input.get('meta', {})

    # Check if 'origin' key exists in meta
    origin = meta.get('origin', None)
    if origin is None:
        logger.error(f'[threshold fn] No origin key found in meta')
        return 'No origin key found in meta'

    # Check if 'risk' key exists in data and if it's above or below 0.5
    risk = meta.get('risk', None)
    if risk is None:
        logger.error(f'[threshold fn] No risk key found in meta: {meta}')
        return 'No risk key found in data'

    with tracer.start_span('risk-val-check', attributes={"invoke_count": Counter.get_count(), "risk": risk}):
        if not 0.0 <= risk <= 1.0:
            logger.error(f'[threshold fn] Risk value is not between 0 and 1: {risk}')
            return 'Risk value is not between 0 and 1'

    risk_status = 'unsafe' if risk > threshold else 'safe'
    logger.info(f'[threshold fn] Risk is {risk_status}: {risk}')

    # branches
    with tracer.start_span('final_decision', attributes={"invoke_count": Counter.get_count(), "risk_status": risk_status, "origin": origin}):
        if risk_status == 'safe':
            if origin == 'self_report':
                logger.info("Do nothing. (safe and self_report)")
                return 'Do nothing (safe and self_report)'
            elif origin == 'system':
                logger.info("calling release. (safe and from system)")
                # TODO: call simulation instead (+update logs, search for "release" word)
                post_release(parsed_input)
                return 'calling release (safe and from system)'
            else:
                logger.error("origin is neither system nor self_report")
                return 'origin is neither system nor self_report'
        elif risk_status == 'unsafe':
            logger.info("calling mutate trajectories. (unsafe)")
            post_mutate(parsed_input)
            return 'calling mutate trajectories. (unsafe)'
        else:
            logger.error("unknown risk status")
            return 'unknown risk status'


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
