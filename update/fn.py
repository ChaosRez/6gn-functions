#!/usr/bin/env python3

import json
import typing
import logging

from call_next_func import post_trigger
from timestamp_for_logger import CustomFormatter
from tracer import TracerInitializer
from store_update import store_update
from json_encoder import JSONEncoder

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
tracer = TracerInitializer("update").tracer

def fn(input: typing.Optional[str]) -> typing.Optional[str]:
    """
    input: A JSON string of collection of new trajectories
    output: writes to the db, and may call trigger function
    """
    with tracer.start_span('parse_input', attributes={"invoke_count": Counter.increment_count()}):
        logger.info(f'[update fn] invoke count: {str(Counter.get_count())}')
        # Parse the JSON string into a Python list of dictionaries
        parsed_input = json.loads(input)
        logger.debug(f'[update fn] Parsed input: {parsed_input}')

        data = parsed_input.get('data', [])
        meta = parsed_input.get('meta', {})

    # Check the 'origin' in 'meta'
    origin = meta.get('origin', None)
    if origin is None:
        logger.error(f'[update fn] No origin key found in meta')
        return f'No origin key found in meta. dump: {meta}'

    # store the data and call the trigger function
    with tracer.start_span('post_trigger_n_store_updates', attributes={"invoke_count": Counter.get_count(), "origin": origin}):
        if origin == 'system':  # invoked by release()
            logger.info(f'[update fn] storing the released data. dump: {data}')
            store_update(data)  # NOTE: multiple trajectories can be released by the system
            logger.info(f'[update fn] will NOT call post_trigger() as it is from system')
        elif origin == 'self_report':  # invoked by ingest
            logger.info('[update fn] storing the reported data.')
            store_update(data)  # NOTE: usually only one trajectory is reported
            logger.info('[update fn] Calling post_trigger with data and meta')
            # json_serialiized_data = JSONEncoder().encode(data)  # after adding created_at as python timestamp
            post_trigger("", meta)
        else:
            logger.fatal(f'[update fn] Unknown origin: {origin}')
            return f'Unknown origin: {origin}'

    return str(data)


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
