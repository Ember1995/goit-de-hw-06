from kafka.admin import KafkaAdminClient, NewTopic
from configs import kafka_config

admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password']
)

my_name = "hanna_dunska"
topics = [
    NewTopic(name=f'{my_name}_input_topic', num_partitions=2, replication_factor=1),
    NewTopic(name=f'{my_name}_output_topic', num_partitions=2, replication_factor=1)
]

try:
    admin_client.create_topics(new_topics=topics, validate_only=False)
    print("Topics created successfully.")
except Exception as e:
    print(f"Error creating topics: {e}")
finally:
    admin_client.close()
