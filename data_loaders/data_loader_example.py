
from kafka import KafkaProducer
import json
import pandas as pd
from datetime import datetime
import random

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


# Configurações do Kafka
# Substitua pelo endereço do seu broker Kafka
bootstrap_servers = 'sam-00123-kafka-brokers.strimzi-kafka.svc.cluster.local:9092'
topic_name = 'raw.industry.1'  # Substitua pelo nome do seu tópico

# Número de linhas no dataset
num_rows = 100

# Criando o DataFrame


@data_loader
def load_data(*args, **kwargs):
    """
    Template code for loading data from any source.

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your data loading logic here
    data = {
        "03CV022/CORRENTE_N_M1_PV(Value)": [random.uniform(0, 350) for _ in range(num_rows)],
        "303-WIT-230(Value)": [random.uniform(0, 3500) for _ in range(num_rows)],
        "timestamp": [datetime.now().strftime('%Y-%m-%d %H:%M:%S') for _ in range(num_rows)]
    }

    df = pd.DataFrame(data)

    # Convertendo o DataFrame para formato JSON
    json_data = df.to_json(orient='records', lines=True)

    # Inicializando o produtor Kafka
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    # Enviando dados para o tópico
    for index, row in df.iterrows():
        producer.send(topic_name, value=row.to_dict())

    # Fechando o produtor Kafka
    producer.close()
    return json_data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
