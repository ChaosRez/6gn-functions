#!/usr/bin/env python3

import json
import typing
import logging
from confluent_kafka import Producer

from call_next_func import post_update
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
tracer = TracerInitializer("release").tracer

# Set up Kafka producer
host = "172.17.0.1"  #TODO: import from ENV
p = Producer({'bootstrap.servers': f'{host}:9092'})


def fn(input: typing.Optional[str]) -> typing.Optional[str]:
    """
    input: trajectories needed to be released and update
    output: publishes the data to the '/release' topic, and calls the update function
    """
    with tracer.start_as_current_span('fn') as main_span:
        main_span.set_attribute("invoke_count", Counter.increment_count())
        main_span.set_attribute("input", input)
        logger.info(f'[release fn] invoke count: {str(Counter.get_count())}')
        # Parse the JSON string into a Python list of dictionaries
        with tracer.start_as_current_span('parse_input'):
            parsed_input = json.loads(input)
            logger.debug(f'[release fn] Parsed input: {parsed_input}')

            data = parsed_input.get('data', [])
            meta = parsed_input.get('meta', {})

        # Select elements where 'origin' is 'mutated'
        with tracer.start_as_current_span('filter_mutated_elems'):
            mutated_data = list(filter(lambda item: item.get('origin', None) == 'mutate', data))
            logger.debug(f'[release fn] Mutated data to release: {mutated_data}')

        # Publish the trajectories to the 'release' topic
        with tracer.start_as_current_span('publish_w_delivery_report'):
            p.produce('releases', json.dumps(mutated_data), callback=delivery_report)
            logger.info(f'[release fn] Published mutated_data to releases topic: {mutated_data}')

        # call update function
        with tracer.start_as_current_span('post_update') as post_update_span:
            try:
                r = post_update(mutated_data, meta)
                post_update_span.set_attribute("response_code", r.status_code)
            except Exception as e:
                logger.error(f'[release fn] Error in post_update: {e}')
                post_update_span.set_attribute("error", True)
                post_update_span.set_attribute("error_details", e)

        # Wait for any outstanding messages to be delivered and delivery reports to be received.
        with tracer.start_as_current_span('flush'):  # TODO: can be async or in parallel somehow
            p.flush()  # blocking
            logger.info(f'[release fn] publish confirmation. meta dump: {meta}')

        return str("release func. check logs for details")


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        logger.error(f'Message delivery failed: {err}\n dumping message: {msg.value()}')
    else:
        logger.info(f'Message delivered to {msg.topic()} [{msg.partition()}]')


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
