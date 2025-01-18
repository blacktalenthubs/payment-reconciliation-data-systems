Below is an **all-in-one Python script** that generates initial datasets for **all six entities**—User, Merchant, Transaction, Policy, FraudSignal, and RiskRule—matching the schemas defined previously. The script uses **Spark** to create DataFrames from synthetic records (generated via `random`/`string`/etc.), then writes each DataFrame to local Parquet files (or you can adjust paths to S3). This ensures each table’s structure aligns with the final schemas you are using in your Spark code (streaming/batch/final aggregator).

You can run this script once to produce the initial “dimension” and “fact” data needed for your pipeline.

---

## generate_initial_data.py

```python
"""
generate_initial_data.py

Generates synthetic data for the six entities:
  1) User
  2) Merchant
  3) Transaction
  4) Policy
  5) FraudSignal
  6) RiskRule

Then writes them to Parquet files using Spark.

Usage:
  python generate_initial_data.py

Adjust the 'OUTPUT_DIR' to your preferred location (local or S3).
"""

import os
import random
import string
import time
from datetime import datetime, timedelta

from pyspark.sql import SparkSession, Row
from pyspark.sql.types import *
from pyspark.sql import functions as F

# We assume these schemas must match exactly the ones in "schemas.py"
# Re-declare them here for convenience.
UserSchema = StructType([
    StructField("user_id", StringType(), nullable=False),
    StructField("user_name", StringType(), nullable=True),
    StructField("signup_date", StringType(), nullable=True)
])

MerchantSchema = StructType([
    StructField("merchant_id", StringType(), nullable=False),
    StructField("merchant_name", StringType(), nullable=True),
    StructField("category", StringType(), nullable=True),
    StructField("risk_level", StringType(), nullable=True)
])

TransactionSchema = StructType([
    StructField("transaction_id", StringType(), nullable=False),
    StructField("user_id", StringType(), nullable=False),
    StructField("merchant_id", StringType(), nullable=False),
    StructField("timestamp", StringType(), nullable=True),
    StructField("amount", DoubleType(), nullable=True),
    StructField("currency", StringType(), nullable=True),
    StructField("status", StringType(), nullable=True),
    StructField("policy_id", StringType(), nullable=True)
])

PolicySchema = StructType([
    StructField("policy_id", StringType(), nullable=False),
    StructField("policy_name", StringType(), nullable=True),
    StructField("effective_date", StringType(), nullable=True),
    StructField("policy_details", StringType(), nullable=True)
])

FraudSignalSchema = StructType([
    StructField("fraud_id", StringType(), nullable=False),
    StructField("transaction_id", StringType(), nullable=False),
    StructField("model_version", StringType(), nullable=True),
    StructField("fraud_score", DoubleType(), nullable=True),
    StructField("inference_time", StringType(), nullable=True),
    StructField("reason_code", StringType(), nullable=True)
])

RiskRuleSchema = StructType([
    StructField("rule_id", StringType(), nullable=False),
    StructField("rule_name", StringType(), nullable=True),
    StructField("severity", StringType(), nullable=True),
    StructField("created_at", StringType(), nullable=True)
])

# Output directory (local or S3). Adjust as needed.
OUTPUT_DIR = "local_data"  # or "s3://your-bucket/initial_data"

# Number of records to generate for each entity
NUM_USERS = 50
NUM_MERCHANTS = 30
NUM_POLICIES = 20
NUM_RISKRULES = 10
NUM_TRANSACTIONS = 200
NUM_FRAUDSIGNALS = 150


def random_string(length=8):
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=length))

def random_date_within(days_back=90):
    """
    Returns an ISO-format date/time string within the past <days_back> days.
    """
    now = datetime.utcnow()
    offset = random.randint(0, days_back*24*60*60)
    random_time = now - timedelta(seconds=offset)
    return random_time.isoformat()

def generate_users(n):
    """
    Generate user data with unique user_id.
    """
    users = []
    for _ in range(n):
        user_id = f"U-{random_string(6)}"
        user_name = f"User_{random_string(4)}"
        signup_date = random_date_within(365)
        users.append({
            "user_id": user_id,
            "user_name": user_name,
            "signup_date": signup_date
        })
    return users

def generate_merchants(n):
    """
    Generate merchant data with unique merchant_id.
    """
    categories = ["Electronics", "Clothing", "Food", "Software", "Services"]
    merchants = []
    for _ in range(n):
        merchant_id = f"M-{random_string(6)}"
        merchant_name = f"Merchant_{random_string(5)}"
        category = random.choice(categories)
        risk_level = random.choice(["LOW", "MEDIUM", "HIGH"])
        merchants.append({
            "merchant_id": merchant_id,
            "merchant_name": merchant_name,
            "category": category,
            "risk_level": risk_level
        })
    return merchants

def generate_policies(n):
    """
    Generate policy data with unique policy_id.
    """
    policies = []
    for _ in range(n):
        policy_id = f"P-{random_string(6)}"
        policy_name = f"Policy_{random_string(4)}"
        effective_date = random_date_within(180)
        # policy_details can be a JSON string, but we'll store a brief snippet
        detail_str = f'{{"velocity_limit": {random.randint(1, 100)}, "regions": ["US","CA"]}}'
        policies.append({
            "policy_id": policy_id,
            "policy_name": policy_name,
            "effective_date": effective_date,
            "policy_details": detail_str
        })
    return policies

def generate_riskrules(n):
    """
    Generate risk rules with unique rule_id, e.g., used by fraud reason codes.
    """
    severities = ["LOW", "MEDIUM", "HIGH", "CRITICAL"]
    riskrules = []
    for _ in range(n):
        rule_id = f"RULE-{random_string(4)}"
        rule_name = f"RiskRule_{random_string(3)}"
        severity = random.choice(severities)
        created_at = random_date_within(365)
        riskrules.append({
            "rule_id": rule_id,
            "rule_name": rule_name,
            "severity": severity,
            "created_at": created_at
        })
    return riskrules

def generate_transactions(n, user_ids, merchant_ids, policy_ids):
    """
    Generate transaction data referencing existing user, merchant, and (optionally) policy IDs.
    """
    statuses = ["AUTHORIZED", "DECLINED", "PENDING", "REFUNDED"]
    currencies = ["USD", "EUR", "GBP", "CAD", "AUD"]
    transactions = []
    for _ in range(n):
        transaction_id = f"T-{random_string(8)}"
        user_id = random.choice(user_ids)
        merchant_id = random.choice(merchant_ids)
        timestamp = random_date_within(60)
        amount = round(random.uniform(5.0, 500.0), 2)
        currency = random.choice(currencies)
        status = random.choice(statuses)
        # 30% chance to have a policy
        policy_id = random.choice(policy_ids) if random.random() < 0.3 else None

        transactions.append({
            "transaction_id": transaction_id,
            "user_id": user_id,
            "merchant_id": merchant_id,
            "timestamp": timestamp,
            "amount": amount,
            "currency": currency,
            "status": status,
            "policy_id": policy_id
        })
    return transactions

def generate_fraud_signals(n, transaction_ids, riskrule_ids):
    """
    Generate fraud signals referencing transaction_id, possibly referencing riskrule in reason_code
    """
    signals = []
    model_versions = ["v1.0", "v1.1", "v2.0"]
    for _ in range(n):
        fraud_id = f"F-{random_string(6)}"
        t_id = random.choice(transaction_ids)
        model_version = random.choice(model_versions)
        fraud_score = round(random.uniform(0.0, 1.0), 2)
        inference_time = random_date_within(60)
        # We'll store reason_code as a risk rule ID some of the time
        reason_code = random.choice(riskrule_ids) if random.random() < 0.5 else None

        signals.append({
            "fraud_id": fraud_id,
            "transaction_id": t_id,
            "model_version": model_version,
            "fraud_score": fraud_score,
            "inference_time": inference_time,
            "reason_code": reason_code
        })
    return signals

def main():
    spark = SparkSession.builder \
        .appName("GenerateInitialData") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # 1) Generate dimension data
    user_data = generate_users(NUM_USERS)
    merchant_data = generate_merchants(NUM_MERCHANTS)
    policy_data = generate_policies(NUM_POLICIES)
    riskrules_data = generate_riskrules(NUM_RISKRULES)

    # Convert dimension data to Spark DataFrames
    user_df = spark.createDataFrame(user_data, schema=UserSchema)
    merchant_df = spark.createDataFrame(merchant_data, schema=MerchantSchema)
    policy_df = spark.createDataFrame(policy_data, schema=PolicySchema)
    riskrule_df = spark.createDataFrame(riskrules_data, schema=RiskRuleSchema)

    # Write dimension tables as Parquet
    user_dim_path = os.path.join(OUTPUT_DIR, "user_dim.parquet")
    merchant_dim_path = os.path.join(OUTPUT_DIR, "merchant_dim.parquet")
    policy_dim_path = os.path.join(OUTPUT_DIR, "policy_dim.parquet")
    riskrule_dim_path = os.path.join(OUTPUT_DIR, "riskrule_dim.parquet")

    user_df.write.mode("overwrite").parquet(user_dim_path)
    merchant_df.write.mode("overwrite").parquet(merchant_dim_path)
    policy_df.write.mode("overwrite").parquet(policy_dim_path)
    riskrule_df.write.mode("overwrite").parquet(riskrule_dim_path)

    # 2) Generate transaction + fraud signals referencing dimension IDs
    # Collect dimension IDs for referencing
    user_ids = [row["user_id"] for row in user_data]
    merchant_ids = [row["merchant_id"] for row in merchant_data]
    policy_ids = [row["policy_id"] for row in policy_data]
    riskrule_ids = [row["rule_id"] for row in riskrules_data]

    transaction_data = generate_transactions(NUM_TRANSACTIONS, user_ids, merchant_ids, policy_ids)
    # transaction_ids = gather for fraud
    tx_ids = [t["transaction_id"] for t in transaction_data]

    fraud_data = generate_fraud_signals(NUM_FRAUDSIGNALS, tx_ids, riskrule_ids)

    # Create DataFrames
    tx_df = spark.createDataFrame(transaction_data, schema=TransactionSchema)
    fraud_df = spark.createDataFrame(fraud_data, schema=FraudSignalSchema)

    # Write as "historical" or raw data
    transactions_parquet_path = os.path.join(OUTPUT_DIR, "transactions.parquet")
    fraud_parquet_path = os.path.join(OUTPUT_DIR, "fraud_signals.parquet")

    tx_df.write.mode("overwrite").parquet(transactions_parquet_path)
    fraud_df.write.mode("overwrite").parquet(fraud_parquet_path)

    print("Initial data generation complete.")
    print(f"User dimension  -> {user_dim_path}")
    print(f"Merchant dimension -> {merchant_dim_path}")
    print(f"Policy dimension -> {policy_dim_path}")
    print(f"RiskRule dimension -> {riskrule_dim_path}")
    print(f"Transactions -> {transactions_parquet_path}")
    print(f"FraudSignals -> {fraud_parquet_path}")

    spark.stop()

if __name__ == "__main__":
    main()
```

---

## How to Use

1. **Install Dependencies**  
   Make sure you have a working Python environment with Spark (`pyspark`), for example:
   ```bash
   pip install pyspark
   ```

2. **Run the Script**  
   ```bash
   python generate_initial_data.py
   ```
   This starts a local Spark session, generates synthetic data for **User**, **Merchant**, **Policy**, **RiskRule**, **Transaction**, **FraudSignal**, and writes six Parquet datasets into the `OUTPUT_DIR` (`local_data` by default).

3. **Resulting Files**  
   - `local_data/user_dim.parquet`  
   - `local_data/merchant_dim.parquet`  
   - `local_data/policy_dim.parquet`  
   - `local_data/riskrule_dim.parquet`  
   - `local_data/transactions.parquet`  
   - `local_data/fraud_signals.parquet`  

   Each file has consistent columns aligned with the **final schemas** used in the streaming/batch/final aggregator Spark code.

---

## Notes

- You can change `OUTPUT_DIR` to a local path (default is `local_data`) or an S3 bucket path (e.g. `s3://my-bucket/initial_data`).  
- If you plan to run the **streaming_index.py** code with Kafka, you’ll still produce more real-time data by pushing these records into Kafka or reusing the same script in a streaming scenario.  
- The `NUM_*` constants let you easily scale record volumes to test performance.  
- Every entity’s references are valid:  
  - **Transaction** references an existing `user_id`, `merchant_id`, and optionally a `policy_id`.  
  - **FraudSignal** references an existing `transaction_id` and optionally a `riskrule_id` for `reason_code`.  
- If you want to produce larger dimension or transaction datasets, simply increase the constants or adapt the generation logic.  

With this script, you ensure each table is **fully consistent** with the final schemas and references. You can now directly feed `transactions.parquet` and `fraud_signals.parquet` to the **batch_index.py** code, or treat them as “historical data” in your pipeline. For dimension data, the same paths (`user_dim.parquet`, `merchant_dim.parquet`, etc.) match the references in your streaming or batch Spark jobs.