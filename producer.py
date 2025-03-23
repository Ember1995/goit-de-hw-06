from kafka import KafkaProducer
from configs import kafka_config
import json, uuid, time, random

producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: str(k).encode('utf-8')
)

if __name__ == "__main__":
    topic = "hanna_dunska_input_topic"
    sensor_id = str(uuid.uuid4())

    for i in range(50):
        message = {
            "timestamp": time.time(),
            "temperature": random.randint(-300, 30),
            "humidity": random.randint(40, 60)
        }

        producer.send(topic, key=sensor_id, value=message)
        print(f"[{i}] Sent: {message}")
        time.sleep(2)

    producer.flush()
    producer.close()
