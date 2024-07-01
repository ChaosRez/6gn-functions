import requests
import logging

from json_encoder import JSONEncoder

host = "172.17.0.1"
# host = "host.docker.internal"

# Set up Python logger
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
    logger.debug(f'[trigger fn] calling riskeval function on {url} with payload: {payload}')
    response = requests.post(url, headers=headers, data=JSONEncoder().encode(payload))
    if response.status_code != 202:  # async call
        logger.error(f'[trigger fn] Error calling riskeval function ({response.status_code}): {response.text}')
    else:
        logger.info(f'[trigger fn] ({response.status_code}) Response from riskeval function: {response.text}')
    return response
