from typing import Dict, List
import requests
import json

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer


MODEL_NAME = "303-WIT-303"
url = "http://sientia-model-api.sientia-model-api:8080/transform"


def format_payload(data, model_name):
    formatted_data = {
        "data": {key: [entry[key] for entry in data] for key in data[0]},
        "model_name": model_name
    }

    return formatted_data


@transformer
def transform(messages: List[Dict], *args, **kwargs):
    """
    Template code for a transformer block.

    Args:
        messages: List of messages in the stream.

    Returns:
        Transformed messages
    """
    # Specify your transformation logic here
    formatted_data = format_payload(messages, MODEL_NAME)
    timestamp = formatted_data['data']['timestamp'][0]

    response = requests.post(url, json=formatted_data)
    response_data = response.json()
    response_data['timestamp'] = timestamp

    return response_data
