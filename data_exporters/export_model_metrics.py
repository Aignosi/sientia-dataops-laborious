from kafka import KafkaProducer
import json
import pandas as pd
from datetime import datetime

bootstrap_servers = 'sam-00123-kafka-brokers.strimzi-kafka.svc.cluster.local:9092'
topic_name = 'model.monitoring'  # Substitua pelo nome do seu tópico


if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data(response_data, *args, **kwargs):
    """
    Exports data to some source.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Output (optional):
        Optionally return any object and it'll be logged and
        displayed when inspecting the block run.
    """
    model_metrics = response_data['regression']
    
    model_metrics['timestamp'] = datetime.now().strftime('%Y/%m/%d %H:%M:%S')

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    # Enviando dados para o tópico

    producer.send(topic_name, value=model_metrics)

    # Fechando o produtor Kafka
    producer.close()