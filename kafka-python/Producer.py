from kafka import KafkaProducer
from faker import Faker
import json
import time

# Initialize Faker for generating random PII data
faker = Faker()

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],  # Adjust this to your Kafka broker address
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serialize data to JSON format
)

# Kafka topic name
topic_name = 'pii_topic'

# Function to generate random PII data
def generate_pii_data():
    return {
        'name': faker.name(),
        'address': faker.address(),
        'email': faker.email(),
        'ssn': faker.ssn(),
        'phone_number': faker.phone_number(),
        'birthdate': faker.date_of_birth().isoformat()
    }

# Continuously generate and send PII data to the Kafka topic
try:
    while True:
        pii_data = generate_pii_data()
        producer.send(topic_name, value=pii_data)
        print(f"Sent data: {pii_data}")
        time.sleep(1)  # Adjust the sleep time as needed
except KeyboardInterrupt:
    print("Stopped sending data.")
finally:
    producer.close()
