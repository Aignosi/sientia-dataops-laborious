from typing import Dict, List
import requests
import json
import pandas as pd

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer


MODEL_NAME = "303-WIT-303"
url = "http://sientia-model-api.sientia-model-api:8080/predict"


@transformer
def transform(response_data, *args, **kwargs):
    """
    Template code for a transformer block.

    Args:
        messages: List of messages in the stream.

    Returns:
        Transformed messages
    """
    data = response_data[0]
    timestamp = data['timestamp']

    formatted_data = {
        "data": data["transformed_data"],
        "model_name": MODEL_NAME
    }

    response = requests.post(url, json=formatted_data)

    response_data = response.json()
    response_data['timestamp'] = timestamp
    response_data = pd.DataFrame(response_data)
    return response_data
