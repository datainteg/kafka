from kafka import KafkaConsumer
import mysql.connector
import json

# Kafka consumer setup
consumer = KafkaConsumer(
    'pii_topic',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='pii_group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# MySQL connection setup
mydb = mysql.connector.connect(
    host="localhost",
    user="your_mysql_user",
    password="your_mysql_password",
    database="pii_data"
)

mycursor = mydb.cursor()

insert_query = """
INSERT INTO pii_table (name, address, email, ssn, phone_number, birthdate)
VALUES (%s, %s, %s, %s, %s, %s)
"""

# Consume messages from Kafka and insert into MySQL
try:
    for message in consumer:
        pii_data = message.value
        data_tuple = (
            pii_data['name'],
            pii_data['address'],
            pii_data['email'],
            pii_data['ssn'],
            pii_data['phone_number'],
            pii_data['birthdate']
        )
        mycursor.execute(insert_query, data_tuple)
        mydb.commit()
        print(f"Inserted data: {pii_data}")
except KeyboardInterrupt:
    print("Stopped consuming messages.")
finally:
    mycursor.close()
    mydb.close()
