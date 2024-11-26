#!/usr/bin/env python3

import json
import typing
import logging

from call_next_func import post_collision_detector
from timestamp_for_logger import CustomFormatter
from tracer import TracerInitializer
from mutate import generate_mutations, dec_speed_of_lower_collider

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

# Load abilities from JSON file
with open('abilities.json', 'r') as f:
    abilities = json.load(f)
logger.debug(f'[mutate fn] Abilities: {abilities}')

MAX_MUTATIONS = 100  # TODO: get from ENV


# FIXME: the output's 'direction' and 'speed' values can be long floats. make them int afterward?
def fn(input: typing.Optional[str], headers: typing.Optional[typing.Dict[str, str]]) -> typing.Optional[str]:
    """
    input: A JSON string that represents a dictionary with a trajectory set 'data' and 'meta' keys.
    output: calls the magic selector function with a collection of mutated trajectories set (candidates)
    """
    with tracer.start_as_current_span('fn') as main_span:
        main_span.set_attribute("invoke_count", Counter.increment_count())
        main_span.set_attribute("input", input)
        logger.info(f'[mutate fn] invoke count: {str(Counter.get_count())}')

        # Parse the JSON string into a Python list of dictionaries
        with tracer.start_as_current_span('parse_input'):
            parsed_input = json.loads(input)
            logger.debug(f'[mutate fn] Parsed input: {parsed_input}')

            data = parsed_input.get('data', [])
            meta = parsed_input.get('meta', {})

        # (check &) increment the number of mutations
        with tracer.start_as_current_span('process_mutate_count', attributes={"origin": meta.get('origin'),
                                                                              "mutations": meta.get('mutations', None)}) as process_mutate_count_span:
            if 'mutations' in meta and meta['origin'] == 'system':  # previously mutated
                logger.info(f"[mutate fn] the trajectory was mutated {meta['mutations']} time(s).")
                if meta['mutations'] > MAX_MUTATIONS:
                    logger.warning(
                        f"[mutate fn] the trajectory has been mutated more than {MAX_MUTATIONS} times. Aborting the request.")
                    return f"Trajectory mutated more than {MAX_MUTATIONS} times. Aborting."
                meta['mutations'] += 1
            elif 'mutations' not in meta and meta['origin'] == 'self_report':  # first time being mutated
                logger.info("[mutate fn] first time mutating the trajectory.")
                meta['mutations'] = 1
            else:
                msg = f"[mutate fn] fatal! unexpected origin or mutations value. origin: {meta['origin']}"
                process_mutate_count_span.set_attribute("error", True)
                logger.fatal(msg)
                return msg  # guard clause

        # collection of mutate trajectories (candidates)
        with tracer.start_as_current_span('case1_mutation') as case1_span:
            success, mutated_trajectory_set = dec_speed_of_lower_collider(data, abilities) # Case 1
            if not success:
                case1_span.set_attribute("error", True)
                case1_span.set_attribute("error_details", mutated_trajectory_set)
                return json.dumps({"error": mutated_trajectory_set})   # mutated_trajectory_set is just a string message here
            # logger.debug(f'[mutate fn] Mutated trajectories: {mutated_trajectory_set}')
            #TODO case 2 , 3

        # call next function with the selected
        with tracer.start_as_current_span('post_collision_detector') as post_collision_detector_span:
            # change origin of the data if it is 'self_report'
            meta['origin'] = "system"
            try:
                r = post_collision_detector(mutated_trajectory_set, meta)
                post_collision_detector_span.set_attribute("response_code", r.status_code)
            except Exception as e:
                logger.error(f'[mutate fn] Error in post_collision_detector: {e}')
                post_collision_detector_span.set_attribute("error", True)
                post_collision_detector_span.set_attribute("error_details", e)

        return str({"data": mutated_trajectory_set})


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
