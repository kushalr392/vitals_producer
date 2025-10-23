import json
import os
import random
import time
from datetime import datetime

from kafka import KafkaProducer
from kafka.errors import KafkaError
from kafka.admin import KafkaAdminClient, NewTopic

# Load environment variables
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_OUTPUT_TOPIC = os.environ.get("KAFKA_OUTPUT_TOPIC", "vital-signs-topic")
SASL_USERNAME = os.environ.get("SASL_USERNAME", "user")
SASL_PASSWORD = os.environ.get("SASL_PASSWORD", "password")
INTERVAL_MS = int(os.environ.get("INTERVAL_MS", "1000"))  # milliseconds
DLQ_TOPIC = os.environ.get("KAFKA_DLQ_TOPIC", "vital-signs-dlq")
RETRY_COUNT = int(os.environ.get("RETRY_COUNT", "3"))
RETRY_BACKOFF_MS = int(os.environ.get("RETRY_BACKOFF_MS", "1000"))

def create_kafka_topic(bootstrap_servers, topic_name):
    """Creates a Kafka topic if it does not exist."""
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            client_id='vital-signs-producer-admin'
        )
        topic_list = admin_client.list_topics()
        if topic_name not in topic_list:
            topic = NewTopic(name=topic_name, num_partitions=1, replication_factor=1)  # Adjust partitions/replication as needed
            admin_client.create_topics(new_topics=[topic], validate_only=False)
            print(f"Topic '{topic_name}' created.")
        else:
            print(f"Topic '{topic_name}' already exists.")
    except Exception as e:
        print(f"Error creating topic: {e}")


def serialize_json(data):
    try:
        return json.dumps(data).encode('utf-8')
    except TypeError as e:
        print(f"Serialization error: {e}")
        return None


def generate_vital_signs():
    """Generates realistic vital signs data with occasional unrealistic heart rate/breath values."""
    body_temp = round(random.uniform(36.0, 39.0), 1)  # Celsius
    heart_rate = random.randint(60, 100)
    breaths_per_minute = random.randint(12, 20)
    systolic_pressure = random.randint(110, 140)
    diastolic_pressure = random.randint(70, 90)
    oxygen_saturation = random.randint(95, 100)
    blood_glucose = random.randint(70, 140)

    # Introduce occasional unrealistic values
    if random.random() < 0.05:  # 5% chance
        heart_rate = random.randint(150, 500)  # Very high heart rate
    if random.random() < 0.05:  # 5% chance
        breaths_per_minute = random.randint(30, 60) # Very high breath rate

    return {
        "timestamp": datetime.utcnow().isoformat(),
        "body_temp": body_temp,
        "heart_rate": heart_rate,
        "systolic_pressure": systolic_pressure,
        "diastolic_pressure": diastolic_pressure,
        "breaths_per_minute": breaths_per_minute,
        "oxygen_saturation": oxygen_saturation,
        "blood_glucose": blood_glucose
    }


def create_producer():
    """Creates a Kafka producer with SASL configuration."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        security_protocol='SASL_PLAINTEXT',
        sasl_mechanism='SCRAM-SHA-512',
        sasl_plain_username=SASL_USERNAME,
        sasl_plain_password=SASL_PASSWORD,
        value_serializer=serialize_json,
        api_version=(0, 11, 5) # Added to resolve broker compatibility issues
    )


def send_to_kafka(producer, topic, message, retry_count=RETRY_COUNT, retry_backoff_ms=RETRY_BACKOFF_MS):
    """Sends a message to Kafka with retry logic and dead-letter queue."""
    for attempt in range(retry_count + 1):
        try:
            producer.send(topic, message).get(timeout=10)  # Adjust timeout as needed
            print(f"Message sent to topic {topic}: {message}")
            return True
        except KafkaError as e:
            print(f"Attempt {attempt + 1} failed to send message: {e}")
            if attempt < retry_count:
                time.sleep(retry_backoff_ms / 1000)  # Backoff
            else:
                print(f"Failed to send message after {retry_count} attempts. Sending to DLQ.")
                send_to_dlq(producer, message)
                return False
    return False


def send_to_dlq(producer, message):
    """Sends a message to the dead-letter queue."""
    try:
        producer.send(DLQ_TOPIC, message).get(timeout=10)
        print(f"Message sent to DLQ topic {DLQ_TOPIC}: {message}")
    except KafkaError as e:
        print(f"Failed to send message to DLQ: {e}")


def main():
    create_kafka_topic(KAFKA_BOOTSTRAP_SERVERS, KAFKA_OUTPUT_TOPIC)
    create_kafka_topic(KAFKA_BOOTSTRAP_SERVERS, DLQ_TOPIC)

    producer = create_producer()

    try:
        while True:
            vital_signs = generate_vital_signs()
            send_to_kafka(producer, KAFKA_OUTPUT_TOPIC, vital_signs)
            time.sleep(INTERVAL_MS / 1000)
    except KeyboardInterrupt:
        print("Shutting down producer...")
    finally:
        producer.close()


if __name__ == "__main__":
    main()