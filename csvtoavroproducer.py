from confluent_kafka import avro
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
import pandas as pd
import json
from collections import defaultdict
import argparse

def main(config_file, csv_file, value_schema_file, target_topic, key_field):

    avro_producer = make_producer(config_file, value_schema_file)
    
    df = pd.read_csv(csv_file)

# Produce messages
    for index, row in df.iterrows():
        key = str(row[key_field])  # Key as string
        value = row.to_dict()  # Convert row to dict for Avro serialization
        avro_producer.produce(topic=target_topic, key=key, value=value, on_delivery=delivery_report)
        avro_producer.flush()

def make_producer(config_file, schema_file):

    if schema_file:
      with open(schema_file, 'r') as file:
        schema_str = file.read()

    with open(config_file, 'r') as file:
      conf = json.load(file)

    schema_registry_conf = {k.replace('schema.registry.', ''): v for k, v in conf.items() if 'schema.registry' in k}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    value_serializer = AvroSerializer(schema_str=schema_str,
                                     schema_registry_client=schema_registry_client)
    
    conf = {k: v for k, v in conf.items() if 'schema.registry' not in k}
    conf['key.serializer'] = StringSerializer('utf_8')
    conf['value.serializer'] = value_serializer

    producer = SerializingProducer(conf)
    return producer

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()}:{msg.partition()}:{msg.offset()}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='publish CSV file lines as Avro messages')
    parser.add_argument('--config', type=str, help='path to configuration file (broker & SR)')
    parser.add_argument('--topic', type=str, help='the topic to produce to')
    parser.add_argument('--csv_file', type=str, help='the csv file to read data from')
    parser.add_argument('--schema_file', type=str, help='the avro file to read value schema from')
    parser.add_argument('--key_field', type=str, help='the field to be used as the string key')
    args = parser.parse_args()
    main(args.config, args.csv_file, args.schema_file, args.topic, args.key_field)

