Below is a revised approach that emphasizes **clear schema design**, **entity-relationship diagrams**, and a **step-by-step architecture**. We'll skip external libraries like `argparse` for clarity, define at least six schemas with clear relationships, and outline the data flows. We’ll also assume a **local Kafka** setup for streaming (rather than MSK) to keep things straightforward. Finally, we'll provide **Spark code** that uses well-defined functions for the joins, producing two indexes and, eventually, a final aggregate.

---

## 1. Entity & Schema Design

We’ll define **six core entities** in our payment-fraud ecosystem. Each entity has a corresponding schema (in PySpark), and we’ll illustrate the relationships via an ERD (Entity Relationship Diagram). This design sets the foundation for consistent joins and data flows.

### 1.1 Entity Definitions & Relationships

1. **User**  
   - Represents a customer or account holder.  
   - Has basic attributes like user_id, user_name, signup_date.  
   - One user can have multiple transactions.

2. **Merchant**  
   - Represents a seller or service provider.  
   - Attributes include merchant_id, merchant_name, category, risk_level.  
   - A transaction references exactly one merchant_id.

3. **Transaction**  
   - Represents a payment event.  
   - Attributes include transaction_id, user_id, merchant_id, timestamp, amount, currency, status.  
   - One transaction can reference zero or one policy (e.g., if policy-based rules were enforced).

4. **Policy**  
   - Represents a set of rules or restrictions (e.g., region blocks, velocity limits).  
   - Attributes include policy_id, policy_name, effective_date, policy_details (JSON).  
   - A transaction can be associated with zero or one policy_id.

5. **FraudSignal**  
   - Represents ML-driven or rule-driven risk data about a specific transaction.  
   - Attributes include fraud_id, transaction_id, model_version, fraud_score, inference_time, reason_code.  
   - One transaction can have multiple fraud signals (e.g., different models or repeated checks).

6. **RiskRule**  
   - Represents an internal rule or threshold used by the system.  
   - Attributes include rule_id, rule_name, severity, created_at.  
   - Potentially used in fraud signals or policy evaluations.  
   - This entity can be mapped to a policy, or a fraud signal might reference a specific risk rule triggered.

### 1.2 Entity Relationship Diagram (ASCII)

```
   +-----------+                  +----------------+
   |   User    |                  |    Merchant    |
   |-----------|                  |----------------|
   | user_id (PK) <-------------- (1) ---------- (∞) transaction.merchant_id
   | user_name  |                 | merchant_id (PK)
   | signup_date|                 | merchant_name
   +-----------+                  | category
              \                  | risk_level
   (1)          \                +----------------+
                (∞)
               +----------------------+
               |    Transaction      |
               |---------------------|
               | transaction_id (PK) |
     (∞) -----> (∞)  user_id (FK)
               | merchant_id (FK)
               | policy_id (FK?)
               | timestamp
               | amount
               | currency
               | status
               +----------------------+
                          |
                          | (0..1)
                          v
                +----------------------+
                |       Policy        |
                |---------------------|
                | policy_id (PK)      |
                | policy_name         |
                | effective_date      |
                | policy_details (JSON)
                +----------------------+

               +----------------------+
               |    FraudSignal      |
               |---------------------|
               | fraud_id (PK)       |
(1) ----------> (∞) transaction_id (FK)
               | model_version
               | fraud_score
               | inference_time
               | reason_code
               +----------------------+
               
               +----------------------+
               |     RiskRule        |
               |---------------------|
               | rule_id (PK)        |
(1) ----------> (∞) maybe referenced in policy_details or fraud signals
               | rule_name
               | severity
               | created_at
               +----------------------+
```

Notes:
- A `Transaction` references `User`, `Merchant`, optionally `Policy`.  
- A `FraudSignal` references exactly one `Transaction`.  
- A `Policy` can reference multiple `RiskRule` definitions. (This can be an embedded relationship if policy_details includes risk rule logic.)

---

## 2. Data Flow & Architecture

We’ll keep the same general concept: **two indexes** (Streaming Index and Batch Index), then a **final aggregator**. But we’ll be more explicit about how we generate data for each entity, how it lands in Kafka or S3, and how Spark processes them.

### 2.1 Data Generation and Ingestion

- **Flask APIs**: Generate synthetic data (Users, Merchants, Transactions, Policies, FraudSignals, RiskRules).  
- **Batch Ingestion**: Python scripts fetch large exports (e.g., historical transactions, policies, etc.) and store them as Parquet in S3.  
- **Stream Ingestion**: Python scripts push near real-time events (Transactions, FraudSignals) into Kafka topics.  
- **Dimension Tables**: Entities like User, Merchant, RiskRule might be stored as Parquet “dimension” data in S3 for broadcast joins.

### 2.2 Spark Processing

1. **Streaming Index Job**  
   - Reads `Transaction` and `FraudSignal` from Kafka.  
   - Joins them on `transaction_id`.  
   - Broadcasts dimension data (`Merchant`, `User`) from S3.  
   - Outputs the “Streaming Index” as Parquet in S3.  

2. **Batch Index Job**  
   - Reads historical `Transaction`, `Policy`, `FraudSignal` from S3.  
   - Joins them with dimension data (`User`, `Merchant`, `RiskRule`).  
   - Outputs the “Batch Index” as Parquet in S3.  

3. **Final Aggregate Job**  
   - Reads both indexes from S3 and merges them (e.g., union, or a deduplicating join on `transaction_id`).  
   - Produces a single “Final Aggregate Index” in S3.

---

## 3. Schema Definitions (PySpark StructTypes)

Below are more precise StructTypes for the six entities. In practice, you’d store them in a separate `schemas.py` file.

```python
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType, IntegerType
)

UserSchema = StructType([
    StructField("user_id", StringType(), nullable=False),
    StructField("user_name", StringType(), nullable=True),
    StructField("signup_date", StringType(), nullable=True),  # or TimestampType
])

MerchantSchema = StructType([
    StructField("merchant_id", StringType(), nullable=False),
    StructField("merchant_name", StringType(), nullable=True),
    StructField("category", StringType(), nullable=True),
    StructField("risk_level", StringType(), nullable=True)
])

TransactionSchema = StructType([
    StructField("transaction_id", StringType(), nullable=False),
    StructField("user_id", StringType(), nullable=True),
    StructField("merchant_id", StringType(), nullable=True),
    StructField("timestamp", StringType(), nullable=True),  # or TimestampType
    StructField("amount", DoubleType(), nullable=True),
    StructField("currency", StringType(), nullable=True),
    StructField("status", StringType(), nullable=True),
    StructField("policy_id", StringType(), nullable=True)  # optional
])

PolicySchema = StructType([
    StructField("policy_id", StringType(), nullable=False),
    StructField("policy_name", StringType(), nullable=True),
    StructField("effective_date", StringType(), nullable=True),
    StructField("policy_details", StringType(), nullable=True)  # could store JSON
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
```

---

## 4. Code Implementation (Spark + Kafka + Parquet)

Here we present a **cleaner** code structure. We’ll create separate Python files for each job. We’ll store configuration in top-level constants or environment variables (not using `argparse`) to keep it simple.

### 4.1 streaming_index.py

```python
"""
streaming_index.py

Reads real-time Transaction and FraudSignal data from Kafka,
joins with dimension tables (User, Merchant) from S3,
and writes a "Streaming Index" in Parquet to S3.
"""

import os
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType
from schemas import TransactionSchema, FraudSignalSchema, UserSchema, MerchantSchema

# Config variables (replace with environment variables or a config file as needed)
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TRANSACTION_TOPIC = os.environ.get("TRANSACTION_TOPIC", "transactions_topic")
FRAUD_TOPIC = os.environ.get("FRAUD_TOPIC", "fraud_signals_topic")

USER_DIM_PATH = os.environ.get("USER_DIM_PATH", "s3://my-dim-bucket/user_dim/")
MERCHANT_DIM_PATH = os.environ.get("MERCHANT_DIM_PATH", "s3://my-dim-bucket/merchant_dim/")

STREAMING_INDEX_OUTPUT = os.environ.get("STREAMING_INDEX_OUTPUT", "s3://my-output/streaming_index/")
STREAMING_CHECKPOINT = os.environ.get("STREAMING_CHECKPOINT", "s3://my-checkpoints/streaming/")

def join_transaction_fraud(tx_df, fraud_df):
    """
    Joins transaction and fraud data on transaction_id (inner join).
    """
    return tx_df.join(fraud_df, on="transaction_id", how="left")

def join_user_dim(df, user_dim):
    """
    Joins with user dimension on user_id, if present.
    """
    return df.join(F.broadcast(user_dim), on="user_id", how="left")

def join_merchant_dim(df, merchant_dim):
    """
    Joins with merchant dimension on merchant_id.
    """
    return df.join(F.broadcast(merchant_dim), on="merchant_id", how="left")

def main():
    spark = SparkSession.builder.appName("StreamingIndexJob").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Read Transaction from Kafka
    tx_raw = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", TRANSACTION_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    tx_parsed = tx_raw.select(
        F.from_json(F.col("value").cast("string"), TransactionSchema).alias("tx")
    ).select("tx.*")

    # Read FraudSignals from Kafka
    fraud_raw = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", FRAUD_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    fraud_parsed = fraud_raw.select(
        F.from_json(F.col("value").cast("string"), FraudSignalSchema).alias("fs")
    ).select("fs.*")

    # Join transaction + fraud
    joined_tx_fraud = join_transaction_fraud(tx_parsed, fraud_parsed)

    # Load dimension data from S3 (Parquet)
    user_dim = spark.read.parquet(USER_DIM_PATH)  # schema: UserSchema
    merchant_dim = spark.read.parquet(MERCHANT_DIM_PATH)  # schema: MerchantSchema

    # Join user and merchant dims
    with_user = join_user_dim(joined_tx_fraud, user_dim)
    final_stream_df = join_merchant_dim(with_user, merchant_dim)

    # Write streaming index to Parquet
    query = final_stream_df.writeStream \
        .format("parquet") \
        .option("path", STREAMING_INDEX_OUTPUT) \
        .option("checkpointLocation", STREAMING_CHECKPOINT) \
        .outputMode("append") \
        .trigger(processingTime="15 seconds") \
        .start()

    query.awaitTermination()
    spark.stop()

if __name__ == "__main__":
    main()
```

**Key Points**  
- We define small functions (`join_transaction_fraud`, `join_user_dim`, `join_merchant_dim`) to make the join logic explicit.  
- We read from Kafka (transactions & fraud).  
- We read dimension tables from S3.  
- We produce a “Streaming Index” in Parquet.

---

### 4.2 batch_index.py

```python
"""
batch_index.py

Reads historical Transaction, Policy, FraudSignal data from S3,
joins with dimension tables (User, Merchant, RiskRule),
and writes a "Batch Index" in Parquet to S3 (overwrite or partition).
"""

import os
from pyspark.sql import SparkSession, functions as F
from schemas import TransactionSchema, PolicySchema, FraudSignalSchema, UserSchema, MerchantSchema, RiskRuleSchema

# Config placeholders
TRANSACTION_PATH = os.environ.get("TRANSACTION_PATH", "s3://my-raw/historical/transactions/")
POLICY_PATH = os.environ.get("POLICY_PATH", "s3://my-raw/historical/policies/")
FRAUD_PATH = os.environ.get("FRAUD_PATH", "s3://my-raw/historical/fraud_signals/")

USER_DIM_PATH = os.environ.get("USER_DIM_PATH", "s3://my-dim-bucket/user_dim/")
MERCHANT_DIM_PATH = os.environ.get("MERCHANT_DIM_PATH", "s3://my-dim-bucket/merchant_dim/")
RISKRULE_DIM_PATH = os.environ.get("RISKRULE_DIM_PATH", "s3://my-dim-bucket/riskrule_dim/")

BATCH_INDEX_OUTPUT = os.environ.get("BATCH_INDEX_OUTPUT", "s3://my-output/batch_index/")

def join_transaction_policy(tx_df, policy_df):
    """
    For demonstration, assume each transaction might have policy_id.
    We'll do a left join on policy_id.
    """
    return tx_df.join(policy_df, on="policy_id", how="left")

def join_transaction_fraud_batch(tx_df, fraud_df):
    """
    Some historical transaction records might have multiple fraud signals,
    so we do a left join on transaction_id. In practice, this might create duplicates
    if there's >1 FraudSignal per transaction. We might union or explode. 
    For simplicity, we do left join.
    """
    return tx_df.join(fraud_df, on="transaction_id", how="left")

def join_dims(df, user_dim, merchant_dim, riskrule_dim):
    """
    Join user and merchant. 
    Optionally, if risk rules are directly referenced, we can join them too.
    We'll keep it simple: user_id, merchant_id.
    """
    df_user = df.join(F.broadcast(user_dim), on="user_id", how="left")
    df_merchant = df_user.join(F.broadcast(merchant_dim), on="merchant_id", how="left")
    # For demonstration, let's say risk rule might be referred to in policy_details or reason_code,
    # a more advanced logic might parse reason_code, then join riskrule_dim. We'll skip that detail here.
    return df_merchant

def main():
    spark = SparkSession.builder.appName("BatchIndexJob").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # 1) Read historical data
    tx_df = spark.read.schema(TransactionSchema).parquet(TRANSACTION_PATH)
    policy_df = spark.read.schema(PolicySchema).parquet(POLICY_PATH)
    fraud_df = spark.read.schema(FraudSignalSchema).parquet(FRAUD_PATH)

    # 2) Dimension reads
    user_dim = spark.read.parquet(USER_DIM_PATH)
    merchant_dim = spark.read.parquet(MERCHANT_DIM_PATH)
    riskrule_dim = spark.read.parquet(RISKRULE_DIM_PATH)

    # 3) Joins
    # Join transaction->policy
    joined_tx_policy = join_transaction_policy(tx_df, policy_df)
    # Join transaction->fraud
    joined_tx_fraud = join_transaction_fraud_batch(joined_tx_policy, fraud_df)
    # Join dimension tables
    final_batch_df = join_dims(joined_tx_fraud, user_dim, merchant_dim, riskrule_dim)

    # 4) Write "Batch Index"
    final_batch_df.write.mode("overwrite").parquet(BATCH_INDEX_OUTPUT)

    spark.stop()

if __name__ == "__main__":
    main()
```

**Key Points**  
- We keep each join in a well-named function.  
- We illustrate how multi-join scenarios might create duplicates if a transaction has multiple fraud signals. In a production environment, you may handle that carefully (e.g., grouping or exploding).  
- We produce a single output for the “Batch Index.”

---

### 4.3 final_aggregate.py

```python
"""
final_aggregate.py

Merges the "Streaming Index" with the "Batch Index" to create a
unified "Final Aggregate Index."
"""

import os
from pyspark.sql import SparkSession, functions as F

STREAMING_INDEX_PATH = os.environ.get("STREAMING_INDEX_PATH", "s3://my-output/streaming_index/")
BATCH_INDEX_PATH = os.environ.get("BATCH_INDEX_PATH", "s3://my-output/batch_index/")
FINAL_INDEX_OUTPUT = os.environ.get("FINAL_INDEX_OUTPUT", "s3://my-output/final_index/")

def merge_indexes(df_stream, df_batch):
    """
    Example: We do a union and then handle duplicates by picking the latest record.
    This is typical if the streaming data might overlap with batch data in time or IDs.
    We'll demonstrate a simple approach (group by transaction_id).
    """
    union_df = df_stream.unionByName(df_batch)
    # Suppose we define "timestamp" as a string in these indexes. Convert to a real timestamp for ordering.
    # We'll assume there's a field "timestamp" to pick the most recent record if duplicates exist.
    union_df = union_df.withColumn("timestamp_parsed", F.to_timestamp(F.col("timestamp")))

    window = (
        F.window(F.col("timestamp_parsed"), "99999 days")  # a big window just for grouping
    )
    # Alternatively, we can group by transaction_id and pick the max timestamp. We'll do this without a real window:
    from pyspark.sql import Window
    w = Window.partitionBy("transaction_id").orderBy(F.col("timestamp_parsed").desc())

    deduped = union_df.withColumn("rn", F.row_number().over(w)) \
                      .filter("rn = 1") \
                      .drop("rn", "timestamp_parsed")

    return deduped

def main():
    spark = SparkSession.builder.appName("FinalAggregateJob").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df_stream = spark.read.parquet(STREAMING_INDEX_PATH)
    df_batch = spark.read.parquet(BATCH_INDEX_PATH)

    # Merge them
    final_df = merge_indexes(df_stream, df_batch)

    # Write final
    final_df.write.mode("overwrite").parquet(FINAL_INDEX_OUTPUT)

    spark.stop()

if __name__ == "__main__":
    main()
```

**Key Points**  
- We define a function `merge_indexes` that unifies the two data sets and resolves duplicates by picking the row with the latest timestamp.  
- We produce a “Final Aggregate Index” in S3.

---

## 5. Putting It All Together

1. **Dimension Setup**: Store dimension tables (User, Merchant, RiskRule) as Parquet in S3.  
2. **Batch Index**:  
   - Run `batch_index.py`.  
   - Reads historical (Transaction, Policy, FraudSignal) from S3.  
   - Joins dimension data.  
   - Writes “Batch Index.”  
3. **Streaming Index**:  
   - Keep `streaming_index.py` running.  
   - Consumes Transaction & FraudSignal from Kafka in near real-time.  
   - Joins dimension data (User, Merchant).  
   - Writes “Streaming Index.”  
4. **Final Aggregate**:  
   - Periodically run `final_aggregate.py` to unify the streaming and batch indexes.  
   - Writes the “Final Aggregate Index” (deduplicated, up-to-date) for downstream queries/analysis.

---

## 6. Summary of Improvements

- **Clear Schema Definitions** for six entities (User, Merchant, Transaction, Policy, FraudSignal, RiskRule).  
- **Entity Relationship Diagram** that clarifies how data is related.  
- **Code** broken into multiple functions, so we can see exactly how each join is performed.  
- **No `argparse`**; we rely on environment variables or static config.  
- **Local Kafka** usage is implied; we just point to `localhost:9092`.  
- **Consistent** approach to reading/writing Parquet, broadcasting dimension data, and defining final merges.

