#!/usr/bin/env python3

import json
import typing
import logging

from call_next_func import post_magic_selector
from timestamp_for_logger import CustomFormatter
from tracer import TracerInitializer
from mutate import generate_mutations

# Set up Python logger. milliseconds are not supported by default
logging.basicConfig(
    level=logging.INFO,  ###################################
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S.%f'
)
logger = logging.getLogger(__name__)
for handler in logging.getLogger().handlers:  # Apply the custom formatter to the root logger
    handler.setFormatter(CustomFormatter(handler.formatter._fmt, handler.formatter.datefmt))

# Initialize the OpenTelemetry tracer
tracer = TracerInitializer("mutate").tracer

# FIXME: the output's 'direction' and 'speed' values can be long floats. make them int afterward?
def fn(input: typing.Optional[str]) -> typing.Optional[str]:
    """
    input: A JSON string that represents a dictionary with a trajectory set 'data' and 'meta' keys.
    output: calls the magic selector function with a collection mutated trajectories (candidates)
    """
    with tracer.start_span('parse_input', attributes={"invoke_count": Counter.increment_count()}):
        logger.info(f'[mutate fn] invoke count: {str(Counter.get_count())}')
        # Load abilities from JSON file
        with open('abilities.json', 'r') as f:
            abilities = json.load(f)
        logger.debug(f'[mutate fn] Abilities: {abilities}')

        # Parse the JSON string into a Python list of dictionaries
        parsed_input = json.loads(input)
        logger.debug(f'[mutate fn] Parsed input: {parsed_input}')

        data = parsed_input.get('data', [])
        meta = parsed_input.get('meta', {})

    # (check &) increment the number of mutations
    with tracer.start_span('mutate_count_process', attributes={"invoke_count": Counter.get_count(), "origin": meta.get('origin'), "mutations": meta.get('mutations')}):
        if 'mutations' in meta and meta['origin'] == 'system':  # previously mutated
            logger.info(f"[mutate fn] the trajectory was mutated {meta['mutations']} time(s).")
            meta['mutations'] += 1
        elif 'mutations' not in meta and meta['origin'] == 'self_report':  # first time being mutated
            logger.info("[mutate fn] first time mutating the trajectory.")
            meta['mutations'] = 1
        else:
            msg = f"[mutate fn] fatal! unexpected origin or mutations value. origin: {meta['origin']}"
            logger.fatal(msg)
            return msg  # guard clause

    # collection of mutate trajectories (candidates)
    with tracer.start_span('generate_mutations', attributes={"invoke_count": Counter.get_count()}):
        mutated_trajectories = generate_mutations(data, abilities)  # TODO: limit the mutated values' decimal precision
        # logger.debug(f'[mutate fn] Mutated trajectories: {mutated_trajectories}')

    # call next function with the selected
    with tracer.start_span('post_magic_selector', attributes={"invoke_count": Counter.get_count()}):
        post_magic_selector(mutated_trajectories, meta)

    return str({"data": mutated_trajectories})


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
