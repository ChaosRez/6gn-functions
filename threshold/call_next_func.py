import requests
import logging

host = "172.17.0.1"
# host = "host.docker.internal"

# Set up Python logger
logger = logging.getLogger(__name__)


def post_mutate(input):
    url = f"http://{host}:8000/mutate"
    headers = {
        "Content-Type": "application/json",
        "X-tinyFaaS-Async": "true"}  # tinyfaas will return a 202 response

    payload = input
    logger.debug(f'[threshold fn] calling mutate function on {url} with payload: {payload}')
    response = requests.post(url, headers=headers, json=payload)
    if response.status_code != 202:  # async call
        logger.error(f'[threshold fn] Error calling mutate function ({response.status_code}): {response.text}')
    else:
        logger.info(f'[threshold fn] ({response.status_code}) Response from mutate function: {response.text}')
    return response


def post_release(input):
    url = f"http://{host}:8000/release"
    headers = {
        "Content-Type": "application/json",
        "X-tinyFaaS-Async": "true"}  # tinyfaas will return a 202 response

    payload = input
    logger.debug(f'[threshold fn] calling release function on {url} with payload: {payload}')
    response = requests.post(url, headers=headers, json=payload)
    if response.status_code != 202:  # async call
        logger.error(f'[threshold fn] Error calling release function ({response.status_code}): {response.text}')
    else:
        logger.info(f'[threshold fn] ({response.status_code}) Response from release function: {response.text}')
    return response
