# file: generate_data.py
#
# Generates random transactions, then calls "bulk_post_fraud_signals" and
# "bulk_post_risk_rules" to push data to the running fraud_api.py server.
# Also writes some local "batch" data as Parquet (users, merchants, policies, transactions).

import random
import uuid
import json
import requests
import datetime
from faker import Faker

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import Row

fake = Faker()
Faker.seed(42)

# Example schemas if you want to store them in Spark
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)

UserSchema = StructType([
    StructField("user_id", StringType(), nullable=False),
    StructField("user_name", StringType(), nullable=True),
    StructField("signup_date", StringType(), nullable=True),
])

MerchantSchema = StructType([
    StructField("merchant_id", StringType(), nullable=False),
    StructField("merchant_name", StringType(), nullable=True),
    StructField("category", StringType(), nullable=True),
    StructField("risk_level", StringType(), nullable=True)
])

PolicySchema = StructType([
    StructField("policy_id", StringType(), nullable=False),
    StructField("policy_name", StringType(), nullable=True),
    StructField("effective_date", StringType(), nullable=True),
    StructField("policy_details", StringType(), nullable=True)
])

TransactionSchema = StructType([
    StructField("transaction_id", StringType(), nullable=False),
    StructField("user_id", StringType(), nullable=True),
    StructField("merchant_id", StringType(), nullable=True),
    StructField("timestamp", StringType(), nullable=True),
    StructField("amount", DoubleType(), nullable=True),
    StructField("currency", StringType(), nullable=True),
    StructField("status", StringType(), nullable=True),
    StructField("policy_id", StringType(), nullable=True)
])

def generate_users(num=50):
    data = []
    for _ in range(num):
        data.append({
            "user_id": str(uuid.uuid4()),
            "user_name": fake.name(),
            "signup_date": str(fake.date_this_decade())
        })
    return data

def generate_merchants(num=30):
    merchant_categories = ["Electronics", "Books", "Grocery", "Fashion", "Fuel", "Restaurants"]
    risk_levels = ["LOW", "MEDIUM", "HIGH"]
    data = []
    for _ in range(num):
        data.append({
            "merchant_id": str(uuid.uuid4()),
            "merchant_name": fake.company(),
            "category": random.choice(merchant_categories),
            "risk_level": random.choice(risk_levels)
        })
    return data

def generate_policies(num=10):
    data = []
    for _ in range(num):
        policy_id = str(uuid.uuid4())
        details_json = {
            "region": random.choice(["US_ONLY", "EU_ONLY", "GLOBAL"]),
            "velocity_limit": random.randint(1, 5)
        }
        data.append({
            "policy_id": policy_id,
            "policy_name": f"Policy-{policy_id[:8]}",
            "effective_date": str(fake.date_this_decade()),
            "policy_details": json.dumps(details_json)
        })
    return data

def generate_transactions(users, merchants, policies, num=200):
    currencies = ["USD", "EUR", "GBP", "CAD"]
    statuses = ["APPROVED", "DECLINED", "PENDING"]
    data = []
    for _ in range(num):
        t_id = str(uuid.uuid4())
        user = random.choice(users)
        merch = random.choice(merchants)
        pol = random.choice(policies) if random.random() < 0.5 else None
        data.append({
            "transaction_id": t_id,
            "user_id": user["user_id"],
            "merchant_id": merch["merchant_id"],
            "timestamp": str(fake.date_time_this_year()),
            "amount": round(random.uniform(5, 1000), 2),
            "currency": random.choice(currencies),
            "status": random.choice(statuses),
            "policy_id": pol["policy_id"] if pol else None
        })
    return data

def bulk_post_fraud_signals(transaction_ids, api_url="http://localhost:5000/fraud_signals", batch_size=100):
    signals = []
    for tid in transaction_ids:
        signals.append({
            "fraud_id": str(uuid.uuid4()),
            "transaction_id": tid,
            "model_version": f"v{random.randint(1, 5)}",
            "fraud_score": round(random.uniform(0, 100), 2),
            "inference_time": str(datetime.datetime.utcnow()),
            "reason_code": random.choice(["RULE_1", "RULE_2", "RULE_3"])
        })

    for i in range(0, len(signals), batch_size):
        batch = signals[i : i + batch_size]
        try:
            resp = requests.post(api_url, json=batch)
            resp.raise_for_status()
            print(f"Posted a batch of {len(batch)} fraud signals.")
        except requests.RequestException as e:
            print(f"Error posting fraud signals: {e}")

def bulk_post_risk_rules(num_rules=500, api_url="http://localhost:5000/risk_rules", batch_size=100):
    severity_levels = ["LOW", "MEDIUM", "HIGH"]
    rules = []
    for _ in range(num_rules):
        rules.append({
            "rule_id": str(uuid.uuid4()),
            "rule_name": fake.bs(),
            "severity": random.choice(severity_levels),
            "created_at": str(datetime.datetime.utcnow())
        })

    for i in range(0, len(rules), batch_size):
        batch = rules[i : i + batch_size]
        try:
            resp = requests.post(api_url, json=batch)
            resp.raise_for_status()
            print(f"Posted a batch of {len(batch)} risk rules.")
        except requests.RequestException as e:
            print(f"Error posting risk rules: {e}")

def get_spark_session(app_name: str = "PaymentUDFPipeline"):
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )

def main():
    spark = get_spark_session("indexing")

    # 1) Generate local data
    users_data = generate_users(num=100)         # more volume
    merchants_data = generate_merchants(num=50)
    policies_data = generate_policies(num=20)
    transactions_data = generate_transactions(users_data, merchants_data, policies_data, num=300)

    # 2) Convert to DataFrame, save locally
    user_df = spark.createDataFrame([Row(**x) for x in users_data], schema=UserSchema)
    merchant_df = spark.createDataFrame([Row(**x) for x in merchants_data], schema=MerchantSchema)
    policy_df = spark.createDataFrame([Row(**x) for x in policies_data], schema=PolicySchema)
    transaction_df = spark.createDataFrame([Row(**x) for x in transactions_data], schema=TransactionSchema)

    user_df.write.mode("overwrite").parquet("data/users")
    merchant_df.write.mode("overwrite").parquet("data/merchants")
    policy_df.write.mode("overwrite").parquet("data/policy")
    transaction_df.write.mode("overwrite").parquet("data/transaction")

    # 3) Bulk POST to the API
    t_ids = [t["transaction_id"] for t in transactions_data]

    # Post 300 signals in batches of 100
    bulk_post_fraud_signals(
        transaction_ids=t_ids,
        api_url="http://localhost:6000/fraud_signals",
        batch_size=100
    )

    # Post 500 risk rules in batches of 100
    bulk_post_risk_rules(
        num_rules=500,
        api_url="http://localhost:6000/risk_rules",
        batch_size=100
    )

    spark.stop()

if __name__ == "__main__":
    main()