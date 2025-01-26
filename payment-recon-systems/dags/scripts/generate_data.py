#!/usr/bin/env python3
"""
end_to_end_data_setup.py

Generates upstream data for a Payment Reconciliation scenario, ensuring all
relationships remain consistent. Demonstrates how some data is for streams
(Kafka), some for batch (Parquet files), and some comes from an API.

1. Merchants
2. Transactions -> Pushed to Kafka (streaming)
3. Aggregator Statements -> Stored locally as Parquet (batch)
4. Policy Data -> Stored locally as Parquet (batch)
5. ML Fraud Signals -> Sent to external API, then retrieved & saved to Parquet

Author: Mentorhub Lead Engineer
"""

import os
import time
import json
import uuid
import random
import logging
from datetime import datetime
from typing import List, Dict

# Faker for mock data
from faker import Faker

# Kafka Producer (kafka-python)
# pip install kafka-python
from kafka import KafkaProducer

# For API calls
# pip install requests
import requests

# PySpark to write batch data as Parquet
# pip install pyspark pyarrow
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType, FloatType,
                               BooleanType)

# --------------------------------------------------------------------------------
# Configuration: Change these to match your environment
# --------------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVER = "localhost:9092"       # Where your Kafka is running
KAFKA_TOPIC = "transactions_topic"              # Topic for real-time transactions
ML_SIGNAL_API_URL = "http://127.0.0.1:7000/ml_signals"  # Where ML signals API is hosted

# Output folder for local Parquet files (aggregator statements, policy data, ML signals)
LOCAL_OUTPUT_PATH = "/opt/airflow/data"  # Matches Docker volume
ML_SIGNAL_API_URL = "http://host.docker.internal:7000/ml_signals"  # Access host from container
KAFKA_BOOTSTRAP_SERVER = "host.docker.internal:9092"

# Control how many records we generate
NUM_MERCHANTS = 50
NUM_TRANSACTIONS = 1000
NUM_STATEMENTS = 300
NUM_POLICIES = 5
NUM_FRAUD_SIGNALS =30

# Probability that aggregator statements match a real transaction
MATCH_PROB = 0.8
# Probability that an ML fraud signal references a real transaction
MATCH_FRAUD_PROB = 0.9

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

fake = Faker()
Faker.seed(42)

# --------------------------------------------------------------------------------
# Data Generation Helpers
# --------------------------------------------------------------------------------

def generate_merchants(num_merchants: int) -> List[Dict]:
    """
    Generate a list of merchant records.
    Each merchant can be referenced by multiple transactions.
    """
    risk_levels = ["LOW", "MEDIUM", "HIGH"]
    merchants = []
    for _ in range(num_merchants):
        merchant_id = fake.bothify(text="MCH-####", letters="ABCDEFGHIJKLMNOPQRSTUVWXYZ")
        merchants.append({
            "merchant_id": merchant_id,
            "merchant_name": fake.company(),
            "risk_level": random.choice(risk_levels),
            "onboarding_date": fake.date_between(start_date="-2y", end_date="today").isoformat()
        })
    return merchants

def generate_transactions(num_tx: int, merchants: List[Dict]) -> List[Dict]:
    """
    Generate transactions referencing existing merchants.
    We'll push these to Kafka for streaming consumption.
    """
    if not merchants:
        raise ValueError("No merchants available for transaction referencing.")

    statuses = ["PENDING", "APPROVED", "DECLINED", "REFUNDED"]
    transactions = []
    for _ in range(num_tx):
        merchant = random.choice(merchants)
        record = {
            "transaction_id": str(uuid.uuid4()),
            "merchant_id": merchant["merchant_id"],
            "user_id": fake.bothify(text="USR-####", letters="ABCDEFGHIJKLMNOPQRSTUVWXYZ"),
            "amount": round(random.uniform(5.0, 1500.0), 2),
            "currency": random.choice(["USD", "EUR", "GBP", "JPY"]),
            "status": random.choice(statuses),
            "event_time": datetime.utcnow().isoformat()  # or use Faker's date_time
        }
        transactions.append(record)
    return transactions

def generate_aggregator_statements(num_statements: int, transactions: List[Dict]) -> List[Dict]:
    """
    Generate bank/aggregator statements referencing a subset of transaction IDs.
    ~80% matched to real transactions, ~20% unmatched to simulate discrepancies.
    """
    if not transactions:
        raise ValueError("No transactions available for aggregator referencing.")

    tx_ids = [tx["transaction_id"] for tx in transactions]
    statements = []
    for _ in range(num_statements):
        stmt_id = "STMT-" + str(uuid.uuid4())[:8]
        if random.random() < MATCH_PROB:
            # matched scenario
            tx_id = random.choice(tx_ids)
        else:
            # unmatched scenario
            tx_id = str(uuid.uuid4())

        posted_amount = round(random.uniform(5.0, 1500.0), 2)
        fee_charged = round(posted_amount * random.uniform(0.005, 0.03), 2)
        statements.append({
            "statement_id": stmt_id,
            "transaction_id": tx_id,
            "posting_date": fake.date_between(start_date="-30d", end_date="today").isoformat(),
            "amount_posted": posted_amount,
            "fee_charged": fee_charged
        })
    return statements

def generate_policy_data(num_policies: int) -> List[Dict]:
    """
    Generate policy data, e.g., KYC/AML rules. Typically used by compliance or risk checks.
    """
    policy_types = ["KYC", "AML", "REGIONAL_LIMIT", "BLACKLIST"]
    policies = []
    for _ in range(num_policies):
        p_id = "POL-" + str(uuid.uuid4())[:8]
        policies.append({
            "policy_id": p_id,
            "policy_type": random.choice(policy_types),
            "rule_description": fake.sentence(nb_words=8),
            "active_flag": random.choice([True, False])
        })
    return policies

def generate_and_send_fraud_signals(num_signals: int, transactions: List[Dict]) -> None:
    """
    Generate ML Fraud Signals referencing existing transactions ~90% of the time.
    Send them to the ML Signal API via POST requests.
    """
    if not transactions:
        raise ValueError("No transactions to reference for fraud signals.")

    tx_ids = [tx["transaction_id"] for tx in transactions]
    model_versions = ["model_v1", "model_v2", "model_v3"]

    logging.info("Sending ML fraud signals to API...")
    for _ in range(num_signals):
        if random.random() < MATCH_FRAUD_PROB:
            tx_id = random.choice(tx_ids)
        else:
            tx_id = str(uuid.uuid4())

        signal_payload = {
            "fraud_signal_id": str(uuid.uuid4()),
            "transaction_id": tx_id,
            "fraud_score": round(random.uniform(0, 1), 4),
            "model_version": random.choice(model_versions),
            "inference_time": datetime.utcnow().isoformat()
        }
        try:
            resp = requests.post(ML_SIGNAL_API_URL, json=signal_payload, timeout=3)
            if resp.status_code != 201:
                logging.error(f"Failed to POST ML Signal: {resp.text}")
        except Exception as e:
            logging.error(f"Error calling ML signal API: {e}")

# --------------------------------------------------------------------------------
# Kafka Producer
# --------------------------------------------------------------------------------

def push_transactions_to_kafka(transactions: List[Dict]) -> None:
    """
    Push mock transaction data to Kafka for streaming consumption.
    Each record is JSON-serialized, then posted to the specified Kafka topic.
    """
    if not transactions:
        logging.warning("No transactions to push to Kafka.")
        return

    logging.info(f"Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVER}, topic='{KAFKA_TOPIC}'...")
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    logging.info(f"Pushing {len(transactions)} transactions to topic '{KAFKA_TOPIC}'...")
    for tx in transactions:
        producer.send(KAFKA_TOPIC, value=tx)
        # Add a small sleep if you want to simulate streaming intervals
        # time.sleep(0.05)
    producer.flush()
    producer.close()
    logging.info("Finished pushing transactions to Kafka.")

# --------------------------------------------------------------------------------
# Spark Helpers (Write to Parquet)
# --------------------------------------------------------------------------------

def get_spark_session():
    return (
        SparkSession.builder
        .appName("EndToEndDataSetup")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )

def write_to_parquet(data: List[Dict], filename: str) -> None:
    """
    Writes the list of dictionaries to a local Parquet file using PySpark.
    """
    if not data:
        logging.warning(f"No data to write for {filename}. Skipping.")
        return

    spark = get_spark_session()
    df = spark.createDataFrame(data)
    full_path = os.path.join(LOCAL_OUTPUT_PATH, filename)

    df.write.mode("overwrite").parquet(full_path)
    count_out = df.count()
    logging.info(f"Wrote {count_out} records to {full_path}")
    spark.stop()

def fetch_ml_signals_and_store() -> None:
    """
    Retrieves ML Fraud Signals from the external API (GET /ml_signals),
    then writes them to a local Parquet file.
    """
    try:
        logging.info("Fetching ML fraud signals via GET...")
        resp = requests.get(ML_SIGNAL_API_URL, timeout=5)
        if resp.status_code == 200:
            ml_data = resp.json()
            if not ml_data:
                logging.warning("No ML signals returned from API.")
                return
            write_to_parquet(ml_data, "ml_fraud_signals.parquet")
        else:
            logging.error(f"API response error: {resp.text}")
    except Exception as e:
        logging.error(f"GET /ml_signals request error: {e}")

# --------------------------------------------------------------------------------
# Main Execution
# --------------------------------------------------------------------------------

def main():
    logging.info("=== Starting End-to-End Data Setup ===")

    # 1. Generate base data
    merchants = generate_merchants(NUM_MERCHANTS)
    transactions = generate_transactions(NUM_TRANSACTIONS, merchants)
    aggregator_statements = generate_aggregator_statements(NUM_STATEMENTS, transactions)
    policy_data = generate_policy_data(NUM_POLICIES)

    # 2. Push transactions to Kafka (for streaming pipeline)
    push_transactions_to_kafka(transactions)

    # 3. Send ML Fraud Signals to an external API
    generate_and_send_fraud_signals(NUM_FRAUD_SIGNALS, transactions)

    # 4. (Batch-Only) aggregator statements & policy data -> local Parquet
    os.makedirs(LOCAL_OUTPUT_PATH, exist_ok=True)

    write_to_parquet(merchants, "merchants.parquet")               # Might be used by batch or analytics
    write_to_parquet(aggregator_statements, "aggregator_statements.parquet")
    write_to_parquet(policy_data, "policy_data.parquet")

    # 5. Retrieve the ML signals we just posted, store them in Parquet
    fetch_ml_signals_and_store()

    logging.info("=== End-to-End Data Setup Complete ===")
    logging.info("Now you have:")
    logging.info("  - Transactions in Kafka (topic = transactions_topic)")
    logging.info(f"  - aggregator_statements.parquet, policy_data.parquet, merchants.parquet, ml_fraud_signals.parquet in {LOCAL_OUTPUT_PATH}")
    logging.info("Use your streaming pipeline to read from Kafka, and your batch pipeline to read these Parquet files.")

if __name__ == "__main__":
    main()
