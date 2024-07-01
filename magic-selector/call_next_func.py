import requests
import logging

host = "172.17.0.1"
# host = "host.docker.internal"

# Set up Python logger
# logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def post_risk_eval(data, meta):
    url = f"http://{host}:8000/riskeval"
    headers = {
        "Content-Type": "application/json",
        "X-tinyFaaS-Async": "true"}  # tinyfaas will return a 202 response

    payload = {
        "data": data,
        "meta": meta
    }
    logger.debug(f'[magicselector fn] calling riskeval function on {url} with payload: {payload}')
    response = requests.post(url, headers=headers, json=payload)
    if response.status_code != 202:  # async call
        logger.error(f'[magicselector fn] Error calling riskeval function ({response.status_code}): {response.text}')
    else:
        logger.info(f'[magicselector fn] ({response.status_code}) Response from riskeval function: {response.text}')
    return response
