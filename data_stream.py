import random
import time
import csv
from datetime import datetime

FILE_NAME = "transactions.csv"

users = ["U1001", "U1002", "U1003", "U1004"]
locations = ["Delhi", "Mumbai", "Bangalore", "Chennai"]
devices = ["Android", "iPhone", "Web"]

def generate_transaction():
    return [
        random.choice(users),
        random.randint(100, 50000),
        random.choice(locations),
        random.choice(devices),
        datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    ]

# Create file with header
with open(FILE_NAME, mode="w", newline="") as file:
    writer = csv.writer(file)
    writer.writerow(["user_id", "amount", "location", "device", "timestamp"])

# Simulate streaming
while True:
    transaction = generate_transaction()
    with open(FILE_NAME, mode="a", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(transaction)
    print("New Transaction:", transaction)
    time.sleep(2)