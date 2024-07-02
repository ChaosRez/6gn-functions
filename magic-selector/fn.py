#!/usr/bin/env python3

import json
import typing
import logging

from call_next_func import post_risk_eval
from timestamp_for_logger import CustomFormatter
from tracer import TracerInitializer

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
tracer = TracerInitializer("magic-selector").tracer

def fn(input: typing.Optional[str]) -> typing.Optional[str]:
    """
    input: A JSON string that represents a dictionary with trajectory set candidates 'data' and 'meta' keys.
    output: calls the risk evaluation function with the selected trajectory
    """
    with tracer.start_as_current_span('fn') as main_span:
        main_span.set_attribute("invoke_count", Counter.increment_count())
        main_span.set_attribute("input", input)
        logger.info(f'[magicselector fn] invoke count: {str(Counter.get_count())}')
        # Parse the JSON string into a Python list of dictionaries
        with tracer.start_as_current_span('parse_input'):
            parsed_input = json.loads(input)
            logger.debug(f'[magicselector fn] Parsed input: {parsed_input}')

            data = parsed_input.get('data', [])
            meta = parsed_input.get('meta', {})

        # select one of the candidate trajectories
        with tracer.start_as_current_span('select_trajectory'):
            new_data = data[0]  # TODO: implement a selection algorithm
            main_span.set_attribute("chosen_trajectory", new_data)
            logger.info(f'[magicselector fn] chosen trajectory set: {new_data}')

            # change origin of the data if it is 'self_report'
            meta['origin'] = "system"

        # call risk evaluation function
        with tracer.start_as_current_span('post_risk_eval') as post_risk_eval_span:
            try:
                r = post_risk_eval(new_data, meta)
                post_risk_eval_span.set_attribute("response_code", r.status_code)
            except Exception as e:
                logger.error(f'[magicselector fn] Error in post_risk_eval: {e}')
                post_risk_eval_span.set_attribute("error", True)
                post_risk_eval_span.set_attribute("error_details", e)

        return f"new trajectory: {new_data}"

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
