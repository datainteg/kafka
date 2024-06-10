pip install kafka-python mysql-connector-python mysql-connector
pip3 install kafka-python mysql-connector-python mysql-connector

######### Create Mysql Table ##########

CREATE DATABASE pii_data;

USE pii_data;

CREATE TABLE pii_table (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255),
    address TEXT,
    email VARCHAR(255),
    ssn VARCHAR(255),
    phone_number VARCHAR(50),
    birthdate DATE
);
