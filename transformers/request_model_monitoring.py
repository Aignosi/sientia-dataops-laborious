import requests
import json
import pandas as pd

MODEL_NAME = "303-WIT-303"
url = "http://sientia-model-api.sientia-model-api:8080/model_monitoring"


if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


def format_payload(data_frame, model_name):
    formatted_data = {
        "current_data": {column: data_frame[column].tolist() for column in data_frame.columns},
        "model_name": model_name
    }
    return formatted_data




@transformer
def transform(merged_df, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your transformation logic here

    formatted_data = format_payload(merged_df, MODEL_NAME)
    response = requests.post(url, json=formatted_data)
    response_data = response.json()

    return response_data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
