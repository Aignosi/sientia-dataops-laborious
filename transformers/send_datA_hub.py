from kafka import KafkaProducer
import json
import pandas as pd
from typing import Dict, List


# Configurações do Kafka
# Substitua pelo endereço do seu broker Kafka
bootstrap_servers = 'sam-00123-kafka-brokers.strimzi-kafka.svc.cluster.local:9092'
topic_name = 'industry.predictions'  # Substitua pelo nome do seu tópico


if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer


@transformer
def transform(response_data, *args, **kwargs):
    """
    Template code for a transformer block.

    Args:
        messages: List of messages in the stream.

    Returns:
        Transformed messages
    """

    response_data = response_data[0]

    # Inicializando o produtor Kafka
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    # Specify your transformation logic here
    # Enviando dados para o tópico
    for index, row in response_data.iterrows():
        producer.send(topic_name, value=row.to_dict())

    # Fechando o produtor Kafka
    producer.close()

    return None
