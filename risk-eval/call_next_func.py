import requests
import logging

host = "172.17.0.1"
# host = "host.docker.internal"

# Set up Python logger
# logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def post_threshold(data, meta, result):
    url = f"http://{host}:8000/threshold"
    headers = {
        "Content-Type": "application/json",
        "X-tinyFaaS-Async": "true"}  # tinyfaas will return a 202 response

    payload = {
        "data": data,
        "meta": {
            **meta,
            "risk": result
        }
    }
    logger.debug(f'[risk-eval fn] calling threshold function on {url} with payload: {payload}')
    response = requests.post(url, headers=headers, json=payload)
    if response.status_code != 202:
        logger.error(f'[risk-eval fn] Error calling threshold function ({response.status_code}): {response.text}')
    else:
        logger.info(f'[risk-eval fn] ({response.status_code}) Response from threshold function: {response.text}')
    return response
