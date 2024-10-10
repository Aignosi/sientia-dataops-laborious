if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

from datetime import datetime
@transformer
def transform(*args, **kwargs):
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
    data = args[0][['timestamp','close']]
    data.rename(columns={'close':'brent'},inplace=True)
    data['petr4']  = args[1]['close']
    data['dolar']  = args[2]['close']
    data['timestamp'] = data['timestamp'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d'))


    return data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
