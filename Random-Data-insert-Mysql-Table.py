import mysql.connector
import random
import string

# Function to generate random string
def random_string(length=10):
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(length))

# Function to generate random integer
def random_integer(min_value=0, max_value=100):
    return random.randint(min_value, max_value)

# Connect to the MySQL database
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="root",
    database="kafka"
)

cursor = conn.cursor()

# Create table if not exists
cursor.execute("""
    CREATE TABLE IF NOT EXISTS random_data (
        id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(255),
        age INT
    )
""")

# Insert random data
for _ in range(100):  # Insert 100 rows
    name = random_string()
    age = random_integer(18, 99)
    cursor.execute("INSERT INTO random_data (name, age) VALUES (%s, %s)", (name, age))

# Commit the transaction
conn.commit()

# Close the connection
cursor.close()
conn.close()
