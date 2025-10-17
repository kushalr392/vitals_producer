import os
import json
import time
import random
import uuid
from kafka import KafkaProducer
from kafka.errors import KafkaError
from dotenv import load_dotenv
import logging
import sys
from time import sleep

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(stream=sys.stdout, level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Configuration from environment variables
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_USERNAME = os.getenv("SASL_USERNAME")
KAFKA_PASSWORD = os.getenv("SASL_PASSWORD")
OUTPUT_TOPIC = os.getenv("KAFKA_OUTPUT_TOPIC")
# DEAD_LETTER_TOPIC = os.getenv("DEAD_LETTER_TOPIC", "dead_letter_topic")  # Default dead-letter topic
INTERVAL_MS = int(os.getenv("INTERVAL_MS", "1000"))  # Default interval: 1000ms = 1 second
RETRY_MAX_ATTEMPTS = int(os.getenv("RETRY_MAX_ATTEMPTS", "3")) # Retry attempts
RETRY_BACKOFF_FACTOR = float(os.getenv("RETRY_BACKOFF_FACTOR", "2")) # Exponential backoff factor

# SASL Configuration
sasl_mechanism = 'PLAIN'
sasl_plain_username = KAFKA_USERNAME
sasl_plain_password = KAFKA_PASSWORD

# SSL Configuration (if needed, configure your truststore)
security_protocol = 'SASL_SSL'

# Create Kafka Producer
def create_kafka_producer(bootstrap_servers, sasl_mechanism, sasl_plain_username, sasl_plain_password, security_protocol):
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers.split(","),
        sasl_mechanism=sasl_mechanism,
        sasl_plain_username=sasl_plain_username,
        sasl_plain_password=sasl_plain_password,
        security_protocol=security_protocol,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        retries=RETRY_MAX_ATTEMPTS,
        api_version=(0, 11, 5)
    )
    return producer


# Generate random vital signs data
def generate_vitals():
    return {
        "patient_id": str(uuid.uuid4()),
        "body_temp": round(random.uniform(36.1, 38.3), 1),  # 36.1-38.3 Celsius (97-101 F)
        "heart_rate": random.randint(60, 100),  # BPM
        "systolic_pressure": random.randint(90, 140),  # mmHg
        "diastolic_pressure": random.randint(60, 90),  # mmHg
        "respiratory_rate": random.randint(12, 20),  # breaths per minute
        "oxygen_saturation": random.randint(95, 100),  # percentage
        "blood_glucose": random.randint(70, 140)  # mg/dL
    }

def send_to_kafka(producer, topic, message):
    try:
        producer.send(topic, value=message)
        logging.info(f"Sent message to topic {topic}: {message}")
        return True
    except KafkaError as e:
        logging.error(f"Failed to send message to topic {topic}: {e}")
        return False

# def send_to_dead_letter_queue(producer, message, exception):
#     try:
#         producer.send(DEAD_LETTER_TOPIC, value={"original_message": message, "error": str(exception)})
#         logging.warning(f"Sent message to dead-letter topic {DEAD_LETTER_TOPIC}")
#     except KafkaError as e:
#         logging.error(f"Failed to send message to dead-letter topic {DEAD_LETTER_TOPIC}: {e}")

def main():
    producer = create_kafka_producer(KAFKA_BOOTSTRAP_SERVERS, sasl_mechanism, sasl_plain_username, sasl_plain_password, security_protocol)

    try:
        while True:
            vitals_data = generate_vitals()
            attempt = 0
            while attempt < RETRY_MAX_ATTEMPTS:
                if send_to_kafka(producer, OUTPUT_TOPIC, vitals_data):
                    break
                else:
                    attempt += 1
                    sleep(RETRY_BACKOFF_FACTOR ** attempt)
            # else:
            #     logging.error(f"Failed to send message after {RETRY_MAX_ATTEMPTS} attempts. Sending to dead-letter queue.")
            #     send_to_dead_letter_queue(producer, vitals_data, "Max retries exceeded")

            time.sleep(INTERVAL_MS / 1000)  # Convert milliseconds to seconds

    except KeyboardInterrupt:
        logging.info("Shutting down producer...")
    finally:
        producer.close()

if __name__ == "__main__":
    main()
