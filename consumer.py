from kafka import KafkaConsumer
from configs import kafka_config
import json

consumer = KafkaConsumer(
    'hanna_dunska_output_topic',
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    key_deserializer=lambda k: k.decode('utf-8'),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='alert_reader'
)

print("Listening for alerts...\n")
for msg in consumer:
    print(f"[{msg.partition}] Alert code {msg.key}:\n{msg.value}\n")
