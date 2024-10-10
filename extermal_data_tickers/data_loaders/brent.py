import yfinance as yf
import json

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


ticker = "BZ=F"
period = '5y'

@data_loader
def load_data(*args, **kwargs):
    """
    Template code for loading data from any source.

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """

    USD_BRL = yf.Ticker(ticker)
    df = USD_BRL.history(period=period)
    df.reset_index(inplace=True)
    df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')
    df.columns = [col.lower().replace(' ', '_') for col in df.columns]
    #rename date to timestamp
    df.rename(columns={'date':'timestamp'}, inplace=True)

    return df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'