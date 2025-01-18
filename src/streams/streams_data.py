# file: generate_and_produce_data.py

import random
import uuid
import json
import time
from faker import Faker
from kafka import KafkaProducer

fake = Faker()
Faker.seed(42)

############################################
# Data Generation
############################################

def generate_users(num=50):
    users = []
    for _ in range(num):
        users.append({
            "user_id": str(uuid.uuid4()),
            "user_name": fake.name(),
            "signup_date": str(fake.date_this_decade())
        })
    return users

def generate_merchants(num=30):
    merchants = []
    categories = ["Electronics", "Books", "Grocery", "Fashion", "Fuel", "Restaurants"]
    risk_levels = ["LOW", "MEDIUM", "HIGH"]
    for _ in range(num):
        merchants.append({
            "merchant_id": str(uuid.uuid4()),
            "merchant_name": fake.company(),
            "category": random.choice(categories),
            "risk_level": random.choice(risk_levels)
        })
    return merchants

def generate_policies(num=10):
    policies = []
    for _ in range(num):
        pid = str(uuid.uuid4())
        details = {
            "region": random.choice(["US_ONLY", "EU_ONLY", "GLOBAL"]),
            "velocity_limit": random.randint(1, 5)
        }
        policies.append({
            "policy_id": pid,
            "policy_name": f"Policy-{pid[:8]}",
            "effective_date": str(fake.date_this_decade()),
            "policy_details": json.dumps(details)
        })
    return policies

def generate_transactions(users, merchants, policies, num=200):
    transactions = []
    currencies = ["USD", "EUR", "GBP", "CAD"]
    statuses = ["APPROVED", "DECLINED", "PENDING"]
    for _ in range(num):
        t_id = str(uuid.uuid4())
        user = random.choice(users)
        merch = random.choice(merchants)
        maybe_policy = random.choice(policies) if random.random() < 0.5 else None
        transactions.append({
            "transaction_id": t_id,
            "user_id": user["user_id"],
            "merchant_id": merch["merchant_id"],
            "timestamp": str(fake.date_time_this_year()),
            "amount": round(random.uniform(5, 1000), 2),
            "currency": random.choice(currencies),
            "status": random.choice(statuses),
            "policy_id": maybe_policy["policy_id"] if maybe_policy else None
        })
    return transactions

def generate_fraud_signals(transactions, ratio=0.6):
    signals = []
    count = int(len(transactions) * ratio)
    chosen_txs = random.sample(transactions, count)
    for tx in chosen_txs:
        signals.append({
            "fraud_id": str(uuid.uuid4()),
            "transaction_id": tx["transaction_id"],
            "model_version": f"v{random.randint(1,3)}",
            "fraud_score": round(random.uniform(0, 100), 2),
            "inference_time": str(fake.date_time_this_year()),
            "reason_code": random.choice(["RULE_1","RULE_2","RULE_3"])
        })
    return signals

############################################
# Kafka Producer
############################################

def get_producer():
    return KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

def produce_batch(producer, topic, records):
    for idx, record in enumerate(records):
        future = producer.send(topic, record)
        metadata = future.get(timeout=10)
        print(f"Topic={topic} | Sent record #{idx} partition={metadata.partition} offset={metadata.offset}")
        # optional small delay for demonstration
        # time.sleep(0.01)

def main():
    producer = get_producer()

    # 1) Generate Data
    users       = generate_users(num=50)
    merchants   = generate_merchants(num=30)
    policies    = generate_policies(num=10)
    transactions = generate_transactions(users, merchants, policies, num=200)
    frauds      = generate_fraud_signals(transactions, ratio=0.6)

    print(f"Users: {len(users)}, Merchants: {len(merchants)}, Policies: {len(policies)}")
    print(f"Transactions: {len(transactions)}, FraudSignals: {len(frauds)}")

    # 2) Produce each dataset to its own Kafka topic
    produce_batch(producer, "users_topic",       users)
    produce_batch(producer, "merchants_topic",   merchants)
    produce_batch(producer, "policies_topic",    policies)
    produce_batch(producer, "transactions_topic",transactions)
    produce_batch(producer, "fraud_signals_topic", frauds)

    producer.close()

if __name__ == "__main__":
    main()