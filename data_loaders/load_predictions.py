import prestodb
import pandas as pd
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


target = "303-WIT-230(VALUE)"

host = "sientia-presto.presto.svc.cluster.local"
port = 8080

TABLE_REAL = "sam00123"
TABLE_PRED = "prediction_303_WIT_303"


conn = prestodb.dbapi.connect(
    host=host,
    port=port,
    user='laborious',
    catalog='laborious',
    schema='laborious',
)


@data_loader
def load_data(*args, **kwargs):
    """
    Template code for loading data from any source.

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """

    # Use Presto SQL to perform the join
    query = f"""
        SELECT real.*, pred.prediction
        FROM pinot.default.{TABLE_REAL} real
        JOIN pinot.default.{TABLE_PRED} pred
        ON real.timestamp = pred.timestamp
        LIMIT 10
    """

    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    columns = [desc[0] for desc in cur.description]
    merged_df = pd.DataFrame(rows, columns=columns)

    merged_df.columns = [col.upper() if col not in [
        'timestamp', 'prediction'] else col for col in merged_df.columns]
    merged_df.rename(columns={
                     target: "target", "03CV022/CORRENTE_N_M1_PV(VALUE)": "03CV022/CORRENTE_N_M1_PV(Value)"}, inplace=True)
    merged_df.drop(columns='timestamp', inplace=True)
    return merged_df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
