Below is an example **PySpark UDF library** that implements the **five** user-defined functions (UDFs) mentioned in our design. Each function is shown in two parts:

1. A **pure Python** function illustrating the core logic.  
2. A **PySpark UDF** definition that can be applied within a Spark job.

These UDFs are **placeholders** or **templates**—you should adjust the internal logic to match your exact business rules, data structures, and field formats.

---

```python
#!/usr/bin/env python3
"""
udf_library.py

PySpark UDF definitions for Payment Reconciliation Index System:
1) udf_calc_discrepancy(amount, amount_posted)
2) udf_flag_compliance(fraud_score, policy_flags)
3) udf_mask_sensitive(user_id)
4) udf_derive_status(cur_status, bank_status)
5) udf_kpi_calculation(merchant_id, daily_index)

Author: Your Name
Date: 2025-01-19
"""

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DoubleType, BooleanType, ArrayType, IntegerType

# ----------------------------------------------------------------------
# 1. udf_calc_discrepancy
# ----------------------------------------------------------------------

def calc_discrepancy_py(amount: float, amount_posted: float) -> float:
    """
    Calculates the difference between what we expected (amount)
    and what the bank posted (amount_posted).
    Example return: amount_posted - amount
    """
    if amount is None or amount_posted is None:
        return 0.0  # or handle None logic differently
    return round(amount_posted - amount, 2)

udf_calc_discrepancy = F.udf(calc_discrepancy_py, DoubleType())

# ----------------------------------------------------------------------
# 2. udf_flag_compliance
# ----------------------------------------------------------------------

def flag_compliance_py(fraud_score: float, policy_flags: str) -> str:
    """
    Determines a compliance/risk label based on fraud_score + policy flags.
    This is a simple example:
      - If fraud_score > 0.8 or if "BLACKLIST" is in policy_flags,
        return 'HIGH_RISK'
      - Else if "KYC" in policy_flags, return 'REQUIRES_KYC'
      - Otherwise, return 'OK'
    
    Adjust logic as needed, especially if policy_flags is stored as an array.
    """
    if fraud_score is None:
        fraud_score = 0.0
    # Example: policy_flags might be a comma-separated string or a JSON array
    # For simplicity, let's just treat it as a string search
    flags = policy_flags.upper() if policy_flags else ""
    
    if fraud_score > 0.8 or "BLACKLIST" in flags:
        return "HIGH_RISK"
    elif "KYC" in flags:
        return "REQUIRES_KYC"
    else:
        return "OK"

udf_flag_compliance = F.udf(flag_compliance_py, StringType())

# ----------------------------------------------------------------------
# 3. udf_mask_sensitive
# ----------------------------------------------------------------------

def mask_sensitive_py(user_id: str) -> str:
    """
    Masks or tokenizes a user_id to protect PII.
    Example: keep only first 2 characters, then mask the rest.
    If user_id = 'USR-1234', result might be 'US********'
    """
    if not user_id:
        return ""
    # Example: just a simple mask after first 2 characters
    return user_id[:2] + "*" * (len(user_id) - 2)

udf_mask_sensitive = F.udf(mask_sensitive_py, StringType())

# ----------------------------------------------------------------------
# 4. udf_derive_status
# ----------------------------------------------------------------------

def derive_status_py(cur_status: str, bank_status: str) -> str:
    """
    Resolves the final status of a transaction by blending the real-time status
    (cur_status) with aggregator or bank statements (bank_status).
    This is a placeholder for your business logic:
      - If bank_status == "MATCHED" and cur_status in ("APPROVED", "REFUNDED"), keep cur_status
      - If bank_status == "UNMATCHED", return "DISCREPANCY"
      - etc.
    
    Adjust as needed.
    """
    # Example approach:
    if not bank_status:
        # fallback to current
        return cur_status if cur_status else "UNKNOWN"
    
    bank_status_upper = bank_status.upper()
    
    if bank_status_upper == "MATCHED":
        # keep the current status if it exists
        return cur_status if cur_status else "MATCHED"
    elif bank_status_upper == "UNMATCHED":
        return "DISCREPANCY"
    else:
        # fallback
        return cur_status if cur_status else bank_status

udf_derive_status = F.udf(derive_status_py, StringType())

# ----------------------------------------------------------------------
# 5. udf_kpi_calculation
# ----------------------------------------------------------------------

def kpi_calculation_py(merchant_id: str, daily_value: float) -> float:
    """
    Computes a KPI for each merchant. Example: multiply daily_value by a factor
    or apply some logic to produce a KPI. In real usage, you might
    aggregate at a merchant/day level, sum amounts, etc.
    
    This is a placeholder example returning daily_value * a random multiplier.
    """
    if daily_value is None:
        daily_value = 0.0
    
    # For demonstration, let's just add 10% if merchant_id starts with 'MCH-A'
    if merchant_id and merchant_id.startswith("MCH-A"):
        return round(daily_value * 1.1, 2)
    else:
        return round(daily_value, 2)

udf_kpi_calculation = F.udf(kpi_calculation_py, DoubleType())

# ----------------------------------------------------------------------
# EXAMPLE USAGE in Spark:
# df = df.withColumn("discrepancy", udf_calc_discrepancy(F.col("amount"), F.col("amount_posted")))
# df = df.withColumn("risk_label", udf_flag_compliance(F.col("fraud_score"), F.col("policy_flags")))
# df = df.withColumn("masked_user", udf_mask_sensitive(F.col("user_id")))
# df = df.withColumn("final_status", udf_derive_status(F.col("cur_status"), F.col("bank_status")))
# df = df.withColumn("merchant_kpi", udf_kpi_calculation(F.col("merchant_id"), F.col("daily_value")))
# ----------------------------------------------------------------------
```

---

## **Notes & Customization**

1. **Data Types**  
   - In the examples above, we assume certain column types (e.g., `DoubleType` for amounts, `StringType` for policy flags).  
   - Adjust as needed based on your actual schemas.  
   - If `policy_flags` is stored as an **array** of strings, you might change the UDF signature or parse logic.

2. **Error/Null Handling**  
   - Each function includes a minimal check for `None` values. In production, consider more robust error handling or default values.

3. **Performance Considerations**  
   - PySpark UDFs are **serialized** functions, so performance might be lower than using native Spark SQL functions.  
   - If possible, you can implement these transformations using built-in Spark SQL expressions or Spark SQL’s `expr` function for better performance.  
   - Alternatively, consider **Pandas UDFs** if you need vectorized operations.

4. **Business Logic**  
   - The placeholders (e.g., `'HIGH_RISK'`, `'OK'`, `'DISCREPANCY'`) are purely examples. Adjust them according to your **business rules**, compliance, or aggregator statuses.

5. **Usage Example**  
   - In your Spark pipeline code, **import** these UDFs and apply them on the DataFrame columns:
     ```python
     from udf_library import (
         udf_calc_discrepancy,
         udf_flag_compliance,
         udf_mask_sensitive,
         udf_derive_status,
         udf_kpi_calculation
     )

     df_enriched = df.join(...).withColumn(
         "discrepancy",
         udf_calc_discrepancy(F.col("amount"), F.col("amount_posted"))
     ).withColumn(
         "risk_label",
         udf_flag_compliance(F.col("fraud_score"), F.col("policy_flags"))
     )
     ...
     ```

Below is a **single-file** Python script that **generates** all relevant mock data—Merchants, Transactions, Aggregator Statements, Policy Data, and ML Fraud Signals—while **maintaining referential integrity**. It also **pushes** Transactions to Kafka (for streaming) and **sends** Fraud Signals to an external API (for later retrieval). Finally, it **stores** local files (Parquet) for the batch-only datasets (e.g., Aggregator Statements, Policy Data) and the fetched ML signals, so that everything is consistent and can be used in your next-stage pipelines.

This example:

1. **Generates Merchants** (used by both streaming & batch).  
2. **Generates Transactions** referencing Merchants.  
3. **Produces** Transactions to Kafka (for the streaming pipeline).  
4. **Generates Aggregator Statements** referencing Transactions (for the batch pipeline).  
5. **Generates Policy Data** (for potential compliance checks).  
6. **Sends** ML Fraud Signals referencing Transaction IDs (some matched, some unmatched) to a separate API.  
7. **Retrieves** all ML Fraud Signals from that API and **stores** them locally.  
8. **Outputs** everything in Parquet to a local folder, except for Transactions which go **directly** to Kafka for real-time streaming consumption.

> **Notes**  
> - This script has **no** CLI argument parsing; you can simply run it.  
> - Adjust constants (like `NUM_TRANSACTIONS`, `NUM_STATEMENTS`) and the **Kafka** / **API** configurations at the top to fit your environment.  
> - Ensure you have a **Kafka broker** running and a **Flask API** for ML signals (or some alternative) accessible.

---

```python
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

Author: Your Name
Date: 2025-01-19
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
ML_SIGNAL_API_URL = "http://127.0.0.1:5000/ml_signals"  # Where ML signals API is hosted

# Output folder for local Parquet files (aggregator statements, policy data, ML signals)
LOCAL_OUTPUT_PATH = "./batch_parquet_data"

# Control how many records we generate
NUM_MERCHANTS = 10
NUM_TRANSACTIONS = 50
NUM_STATEMENTS = 30
NUM_POLICIES = 5
NUM_FRAUD_SIGNALS = 15

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
```

---

1. **Generate Merchants**  
   - Creates a list of 10 (by default) merchant records, each with an ID, name, risk level, and onboarding date.

2. **Generate Transactions**  
   - Creates 50 transactions referencing **valid merchant IDs**. These transactions are meant for your **streaming** pipeline.

3. **Push Transactions to Kafka**  
   - We connect to the broker at `KAFKA_BOOTSTRAP_SERVER` and **send** each transaction (as JSON) to `KAFKA_TOPIC`.  
   - The user can then run a **Spark Structured Streaming job** (or any Kafka consumer) to read these in **real-time**.

4. **Generate Aggregator Statements**  
   - Creates a set of statements referencing transactions **~80% of the time** (the rest unmatched to simulate discrepancies).  
   - These are purely for your **batch pipeline** (daily or hourly) to read from local Parquet.

5. **Generate Policy Data**  
   - Creates a few KYC/AML rules. This might be used in batch or streaming enrichments; for simplicity, we store them in local Parquet.

6. **Send ML Fraud Signals**  
   - We create 15 signals referencing existing transactions **~90% of the time**.  
   - **POST** them to the ML Signal API at `ML_SIGNAL_API_URL`. You must have a separate Flask service or microservice receiving them.

7. **Fetch ML Signals**  
   - Immediately **GET** all signals from the API and store them in Parquet locally (`ml_fraud_signals.parquet`).  
   - This means your batch job or analytics can read the same signals if needed.

8. **Output**  
   - Everything except the transactions is placed under `./batch_parquet_data` in Parquet.  
   - The transactions are in **Kafka**, waiting for your streaming pipeline to consume them.

---

## **What Happens Next?**

1. **Streaming Pipeline**  
   - A separate **Spark Streaming** (or Flink, etc.) job reads from `transactions_topic`.  
   - It might write out a partial “Streaming Index” (e.g., to S3 or another location) that includes the latest statuses or any quick computations.

2. **Batch Pipeline**  
   - Another Spark job runs daily, reading `./batch_parquet_data/aggregator_statements.parquet` (plus the partial streaming index) to do reconciliation.  
   - It might also read the newly fetched `ml_fraud_signals.parquet` if fraud data is relevant to the batch logic.  
   - Or it can read `policy_data.parquet` for compliance checks.

3. **Dashboard / Analytics**  
   - The “Final Aggregate Index” or other outputs can be used by BI tools (e.g., Power BI, Tableau) or finance systems.

With this single script, you have a **consistent** set of data that is:

- **Pushed to Kafka** (for streaming)  
- **Available as local Parquet** (for batch)  
- **Linked** (Transaction IDs match aggregator statements and ML signals with some random unmatched cases)  


Below is a **three-file** layout demonstrating how you might orchestrate a complete **Payment Reconciliation** flow:

1. **Streaming Pipeline**:  
   - Reads real-time transactions from Kafka.  
   - Writes a **partial streaming index** to local Parquet (simulating near real-time output).  

2. **Batch Reconciliation**:  
   - Reads **aggregator statements** (already generated/stored as Parquet).  
   - Reads the **partial streaming index** created by the streaming pipeline.  
   - Merges data, applies simple UDFs, and writes out a **Batch Index**.  

3. **Final Index Pipeline**:  
   - Reads the **Batch Index** and **Partial Streaming Index** (or other relevant data, e.g., ML signals).  
   - Produces a unified **Final Index** that could be used by finance, compliance, or dashboards.

> **Assumptions/Prerequisites**  
> - You have a **Kafka** broker running at `localhost:9092`.  
> - A **Kafka topic** called `transactions_topic` exists for streaming ingestion.  
> - You have aggregator statements already saved as Parquet (e.g., `aggregator_statements.parquet`) and possibly other data sets (e.g., policy_data, ML signals).  
> - **No command-line arguments** are used to keep things simple. Each file has constants at the top.

Below, you’ll find three separate files. In a real project, you would store each in its own `.py` file.

---

## **1) `streaming_pipeline.py`**

```python
#!/usr/bin/env python3
"""
streaming_pipeline.py

Reads transactions from a Kafka topic in near real-time,
enriches/filters as needed, and writes a "Partial Streaming Index"
as Parquet to local storage.

Usage:
  python streaming_pipeline.py

Prereqs:
- Kafka running at localhost:9092
- 'transactions_topic' exists

Author: Your Name
"""

import os
import json
import time
import logging

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType
)

# ------------------------------ #
#  CONFIGURATIONS & CONSTANTS    #
# ------------------------------ #
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "transactions_topic"

# Where we'll write the partial streaming index
STREAMING_INDEX_OUTPUT = "./output/partial_stream_index"

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

def main():
    logging.info("Starting Streaming Pipeline...")

    spark = (
        SparkSession.builder
        .appName("StreamingPipeline")
        .getOrCreate()
    )

    # 1) Read transactions from Kafka
    df_kafka = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    # 2) Parse the JSON from the 'value' field
    df_raw = df_kafka.selectExpr("CAST(value AS STRING) as json_str")

    # Define a schema for our transactions
    transaction_schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("merchant_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("amount", FloatType(), True),
        StructField("currency", StringType(), True),
        StructField("status", StringType(), True),
        StructField("event_time", StringType(), True)
    ])

    df_parsed = df_raw.select(
        F.from_json(F.col("json_str"), transaction_schema).alias("data")
    ).select("data.*")

    # 3) (Optional) Enrichment or filtering
    # For now, just add a processing timestamp
    df_enriched = df_parsed.withColumn("processed_at", F.current_timestamp())

    # 4) Write partial streaming index to Parquet (append mode)
    os.makedirs(STREAMING_INDEX_OUTPUT, exist_ok=True)

    query = (
        df_enriched
        .writeStream
        .format("parquet")
        .option("checkpointLocation", STREAMING_INDEX_OUTPUT + "/_checkpoints")
        .option("path", STREAMING_INDEX_OUTPUT)
        .trigger(processingTime="5 seconds")  # micro-batch every 5s
        .outputMode("append")
        .start()
    )

    logging.info(f"Streaming from topic '{KAFKA_TOPIC}' to '{STREAMING_INDEX_OUTPUT}'")

    query.awaitTermination()

if __name__ == "__main__":
    main()
```

### **How It Works**  

- **Spark Structured Streaming** continuously reads from `transactions_topic`.  
- Parses JSON messages into a DataFrame (`df_parsed`).  
- Writes a **partial stream index** every 5 seconds to `./output/partial_stream_index/`.  
- In a real environment, you might write to **S3** or HDFS.  

---

## **2) `batch_reconciliation.py`**

```python
#!/usr/bin/env python3
"""
batch.py

Reads:
  - Aggregator Statements (Parquet)
  - Partial Streaming Index (Parquet)

Joins them on transaction_id, applies basic reconciliation logic (e.g., discrepancy),
and writes a "Batch Index" for further analysis.

Usage:
  python batch.py

Author: Your Name
"""

import os
import logging
from datetime import datetime

from pyspark.sql import SparkSession, functions as F, types as T

# ------------------------------ #
#  CONFIGURATIONS & CONSTANTS    #
# ------------------------------ #
AGGREGATOR_STATEMENTS_PATH = "./batch_parquet_data/aggregator_statements.parquet"
PARTIAL_STREAM_INDEX_PATH = "./output/partial_stream_index"
BATCH_INDEX_OUTPUT = "./output/batch_index"

# Basic UDF for calculating discrepancy
def calc_discrepancy_py(amount, amount_posted):
    if amount is None or amount_posted is None:
        return 0.0
    return round(amount_posted - amount, 2)

# We'll register it as a Spark UDF
calc_discrepancy = F.udf(calc_discrepancy_py, T.FloatType())

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

def main():
    logging.info("Starting Batch Reconciliation...")

    spark = (
        SparkSession.builder
        .appName("BatchReconciliation")
        .getOrCreate()
    )

    # 1) Read aggregator statements
    logging.info(f"Reading aggregator statements from {AGGREGATOR_STATEMENTS_PATH}")
    df_statements = spark.read.parquet(AGGREGATOR_STATEMENTS_PATH)

    # 2) Read partial streaming index
    logging.info(f"Reading partial streaming index from {PARTIAL_STREAM_INDEX_PATH}")
    df_partial = spark.read.parquet(PARTIAL_STREAM_INDEX_PATH)

    # 3) Join on transaction_id
    #    We'll keep all partial stream records, left-joining aggregator
    df_joined = df_partial.alias("st").join(
        df_statements.alias("ag"),
        F.col("st.transaction_id") == F.col("ag.transaction_id"),
        how="left"
    )

    # 4) Calculate discrepancy
    #    partial stream's "amount" vs aggregator's "amount_posted"
    df_enriched = df_joined.withColumn(
        "discrepancy",
        calc_discrepancy(F.col("st.amount"), F.col("ag.amount_posted"))
    )

    # 5) Optionally define a "reconciliation_status"
    #    If aggregator is missing => "UNMATCHED", else "MATCHED"
    df_enriched = df_enriched.withColumn(
        "reconciliation_status",
        F.when(F.col("ag.statement_id").isNull(), F.lit("UNMATCHED"))
         .otherwise(F.lit("MATCHED"))
    )

    # 6) Add a batch_run_timestamp
    current_ts = datetime.utcnow().isoformat()
    df_final = df_enriched.withColumn("batch_run_timestamp", F.lit(current_ts))

    # 7) Select columns for the "Batch Index"
    df_batch_index = df_final.select(
        "st.transaction_id",
        "st.merchant_id",
        "st.user_id",
        "st.amount",
        "st.currency",
        "st.status",  # streaming pipeline's real-time status
        "ag.statement_id",
        "ag.posting_date",
        "ag.amount_posted",
        "ag.fee_charged",
        "discrepancy",
        "reconciliation_status",
        "batch_run_timestamp"
    )

    # 8) Write Batch Index
    os.makedirs(BATCH_INDEX_OUTPUT, exist_ok=True)
    logging.info(f"Writing Batch Index to {BATCH_INDEX_OUTPUT}")

    df_batch_index.write.mode("overwrite").parquet(BATCH_INDEX_OUTPUT)
    count_out = df_batch_index.count()

    logging.info(f"Wrote {count_out} records to the Batch Index.")
    spark.stop()

if __name__ == "__main__":
    main()
```

### **How It Works**  

- Reads **aggregator statements** (generated/stored as Parquet by your data-setup script).  
- Reads the **partial streaming index** from `./output/partial_stream_index` (produced by `streaming_pipeline.py`).  
- Joins on `transaction_id`, calculates a simple `discrepancy`, and sets `reconciliation_status`.  
- Writes a consolidated **Batch Index** to `./output/batch_index`.

---

## **3) `final_index_pipeline.py`**

```python
#!/usr/bin/env python3
"""
final_index_pipeline.py

Demonstrates how one might merge the Batch Index with other data 
(e.g., the partial streaming index or ML signals, policy data) 
to produce a "Final Index."

Usage:
  python final_index_pipeline.py

Author: Your Name
"""

import os
import logging
from datetime import datetime

from pyspark.sql import SparkSession, functions as F

# ------------------------------ #
#  CONFIGURATIONS & CONSTANTS    #
# ------------------------------ #
BATCH_INDEX_PATH = "./output/batch_index"
PARTIAL_STREAM_INDEX_PATH = "./output/partial_stream_index"
ML_SIGNALS_PATH = "./batch_parquet_data/ml_fraud_signals.parquet"  # optional
FINAL_INDEX_OUTPUT = "./output/final_index"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

def main():
    logging.info("Starting Final Index Pipeline...")

    spark = (
        SparkSession.builder
        .appName("FinalIndexPipeline")
        .getOrCreate()
    )

    # 1) Read the Batch Index
    logging.info(f"Reading Batch Index from {BATCH_INDEX_PATH}")
    df_batch_index = spark.read.parquet(BATCH_INDEX_PATH)

    # 2) Optionally read partial streaming index or ML signals
    logging.info(f"Reading partial streaming index from {PARTIAL_STREAM_INDEX_PATH}")
    df_partial = spark.read.parquet(PARTIAL_STREAM_INDEX_PATH)

    logging.info(f"Reading ML signals from {ML_SIGNALS_PATH}")
    df_ml_signals = spark.read.parquet(ML_SIGNALS_PATH)  # if it exists

    # 3) Example: Join the Batch Index with ML signals to add a "fraud_score" column
    df_final_join = df_batch_index.alias("b").join(
        df_ml_signals.alias("ml"),
        F.col("b.transaction_id") == F.col("ml.transaction_id"),
        how="left"
    )

    # 4) Possibly merge partial streaming's latest 'status' or 'processed_at'
    #    This might not be strictly necessary if your batch index already includes
    #    the streaming statuses, but let's do a demonstration
    df_with_stream = df_final_join.alias("fj").join(
        df_partial.alias("ps"),
        F.col("fj.transaction_id") == F.col("ps.transaction_id"),
        how="left"
    )

    # 5) Add final timestamp, rename columns as needed
    run_ts = datetime.utcnow().isoformat()
    df_final = df_with_stream.withColumn("final_index_ts", F.lit(run_ts))

    # 6) Select columns for the "Final Index"
    #    (This is a placeholder—choose whichever columns your business needs.)
    df_final_index = df_final.select(
        "b.transaction_id",
        "b.merchant_id",
        "b.user_id",
        "b.amount",
        "b.currency",
        "b.status",            # from batch index
        "b.discrepancy",
        "b.reconciliation_status",
        "ml.fraud_signal_id",
        "ml.fraud_score",
        "ml.inference_time",
        "ps.processed_at",     # partial streaming
        "final_index_ts"
    )

    # 7) Write out the final index
    os.makedirs(FINAL_INDEX_OUTPUT, exist_ok=True)
    logging.info(f"Writing Final Index to {FINAL_INDEX_OUTPUT}")
    df_final_index.write.mode("overwrite").parquet(FINAL_INDEX_OUTPUT)
    count_out = df_final_index.count()

    logging.info(f"Wrote {count_out} records to Final Index.")
    spark.stop()

if __name__ == "__main__":
    main()
```

### **How It Works**  

- Reads the **Batch Index** from `./output/batch_index`.  
- Reads the **Partial Streaming Index** and possibly **ML Fraud Signals** from `ml_fraud_signals.parquet`.  
- Joins them on `transaction_id` to add fraud scores, or to pick up any last-minute streaming updates.  
- Produces a “Final Index” that has **both** batch reconciliation results and **fraud** or streaming data columns.

---

## **Putting It All Together**

1. **Run or have running** the script that **generates** aggregator statements, policy data, and posts transactions to Kafka.  
2. **Start** the **Streaming Pipeline** (`streaming_pipeline.py`): 
   - It will consume from the Kafka topic and continuously produce a partial stream index to `./output/partial_stream_index/`.
3. **Periodically run** the **Batch Reconciliation** job (`batch_reconciliation.py`): 
   - It reads aggregator statements (from your local Parquet) and the partial streaming index, merges them, writes `./output/batch_index/`.
4. **Finally** run the **Final Index Pipeline** (`final_index_pipeline.py`): 
   - It merges the batch index with ML signals or the partial index again, producing `./output/final_index/`.

Each step is separated into its own file for clarity, reflecting typical real-world architectures where the streaming pipeline, batch pipeline, and final aggregator pipeline each have distinct schedules or triggers.  

> **Important**:  
> - In a production scenario, you’d typically run the **streaming job** continuously (e.g., on a Spark cluster or a container in EKS).  
> - The batch jobs might be scheduled via **Airflow**, **Cron**, or other orchestrators.  
> - The “final index” job might also run on a schedule or be triggered on specific events (e.g., after the batch index is updated).


