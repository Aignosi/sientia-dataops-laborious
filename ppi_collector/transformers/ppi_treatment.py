if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
import pandas as pd
import numpy as np


@transformer
def transform(df, *args, **kwargs):
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
    df['start_date'], df['end_date'] = df['Data'].str.split(r' a | A ').str
    df['start_date'] = pd.to_datetime(df['start_date'], format='%d/%m/%Y')
    df['end_date'] = pd.to_datetime(df['end_date'], format='%d/%m/%Y')

    # Step 2: Create a daily DataFrame by expanding the date range
    daily_rows = []

    for i, row in df.iterrows():
        # Create a date range between start and end dates
        date_range = pd.date_range(start=row['start_date'], end=row['end_date'])
        
        # For each date in the range, create a row with interpolated values
        for j, date in enumerate(date_range):
            daily_row = row.copy()
            daily_row['timestamp'] = date
            for column in df.columns:
                start_value = df.loc[i, column]
                
                daily_row[column] = start_value
            daily_rows.append(daily_row)

    # Step 3: Convert the list of daily rows back to a DataFrame
    daily_df = pd.DataFrame(daily_rows)

    # Remove unnecessary columns
    daily_df = daily_df.drop(columns=['start_date', 'end_date'])
    daily_df["Data"] = daily_df["timestamp"]
    daily_df.drop("timestamp", axis=1,inplace=True)
    daily_df.rename(columns={"Data":"timestamp"},inplace=True)

    # Reset the index for clarity
    daily_df.reset_index(drop=True, inplace=True)


    daily_df.columns = [x.strip() for x in daily_df.columns] #clean spaces from name 
    return daily_df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
