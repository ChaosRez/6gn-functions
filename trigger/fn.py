#!/usr/bin/env python3

import json
import typing
import logging
import uuid

from call_next_func import post_risk_eval
from timestamp_for_logger import CustomFormatter
from tracer import TracerInitializer
from get_recent_trajectories import get_recent_trajectories
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
tracer = TracerInitializer("trigger").tracer

def fn(input: typing.Optional[str]) -> typing.Optional[str]:
    """
    input: gets a new trajectory. Invoked by the update function
    output: calls the risk-eval function with the recent trajectories from the db
    """
    with tracer.start_span('parse_input', attributes={"invoke_count": Counter.increment_count()}):
        logger.info(f'[trigger fn] invoke count: {str(Counter.get_count())}')
        # Parse the JSON string into a Python list of dictionaries
        parsed_input = json.loads(input)
        logger.debug(f'[trigger fn] Parsed input: {parsed_input}')

        # data = parsed_input.get('data', [])  # no data expected
        meta = parsed_input.get('meta', {})

    # Check the 'origin' in 'meta'
    origin = meta.get('origin', None)
    if origin is None:
        logger.error(f'[trigger fn] No origin key found in meta. dump: {meta}')
        return f'No origin key found in meta. dump: {meta}'

    # Check if 'origin' is 'self_report'
    if origin != 'self_report':
        logger.error(f'[trigger fn] Origin is not self_report. dump: {meta}')
        return f'Origin is not self_report. dump: {meta}'

    # Generate a unique request_id and add it to the meta dictionary
    with tracer.start_span('gen_req_uid', attributes={"invoke_count": Counter.get_count()}):
        request_id = str(uuid.uuid4())
        meta['request_id'] = request_id
        logger.info(f'[trigger fn] Generated request_id: {request_id}')

    # TODO: get the ttl from ENV
    # TODO: if db is slow, call it in parallel with previous code
    # Get recent trajectory from each uav (limited by ttl, in seconds)
    # includes the trajectory from the update (already in db)
    ttl = 100  # seconds
    with tracer.start_span('get_recent_trajectories', attributes={"invoke_count": Counter.get_count(), "ttl": ttl}):
        recent_trajectories = get_recent_trajectories(ttl)

    # Check if recent_trajectories is not empty, else call risk-eval function
    with tracer.start_span('post_risk_eval_if_any_traj', attributes={"invoke_count": Counter.get_count(), "recent_trajectories_size": len(recent_trajectories)}):
        if not recent_trajectories:
            logger.error(f'[trigger fn] No recent trajectories found')
            return f'No recent trajectories found'
        else:
            logger.info(
                f'[trigger fn] Found {len(recent_trajectories)} trajectories for uav_ids: {[trajectory["uav_id"] for trajectory in recent_trajectories]}')
            # call risk-eval function
            encoded_recent_trajectories = [JSONEncoder().default(trajectory) for trajectory in recent_trajectories]
            post_risk_eval(encoded_recent_trajectories, meta)
            return str(encoded_recent_trajectories)


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
