Below is a **finalized**, no-ambiguity codebase implementing our **two-index design** (Streaming and Batch) plus a **Final Aggregator**. We define **six schemas** for the entities (User, Merchant, Transaction, Policy, FraudSignal, RiskRule), and we do **explicit joins** without broadcast or optional paths. This represents a single, decided approach for a teaching reference:

- **No environment variables** or `argparse`.  
- **No broadcast** joins.  
- **One stable join strategy** for each entity relationship.  
- **Final aggregator** merges the two indexes in a consistent way, using a standard deduplication approach if duplicates exist.  

All code is in pure Python+Spark. You can adapt paths to your own S3 or local file system, plus adjust schema details or field names if needed. 

---

## 1) schemas.py

```python
# schemas.py
# Define PySpark StructTypes for our six entities: User, Merchant, Transaction, Policy, FraudSignal, RiskRule.

from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)

# We store timestamps as StringType for simplicity here.
# In a real system, you might use TimestampType.

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
```

---

## 2) streaming_index.py

Reads **Transaction** and **FraudSignal** data from **Kafka** (two topics), plus **User**, **Merchant**, **RiskRule**, **Policy** dimension tables from storage (here, Parquet on local/HDFS/S3). Produces a **“Streaming Index”** as Parquet.

```python
# streaming_index.py
#
# 1) Reads from Kafka (Transaction topic, FraudSignal topic).
# 2) Joins them on transaction_id (inner join).
# 3) Joins with dimension data: User, Merchant, Policy, RiskRule.
# 4) Writes a "Streaming Index" in Parquet format.

from pyspark.sql import SparkSession, functions as F
from schemas import (
    TransactionSchema, FraudSignalSchema,
    UserSchema, MerchantSchema, PolicySchema, RiskRuleSchema
)

# Hard-coded constants for demonstration
KAFKA_BOOTSTRAP = "localhost:9092"
TRANSACTION_TOPIC = "transactions_topic"
FRAUD_TOPIC = "fraud_signals_topic"

# Dimension tables path (assume Parquet). Adjust for your environment.
USER_DIM_PATH = "path/to/user_dim.parquet"
MERCHANT_DIM_PATH = "path/to/merchant_dim.parquet"
POLICY_DIM_PATH = "path/to/policy_dim.parquet"
RISKRULE_DIM_PATH = "path/to/riskrule_dim.parquet"

# Output path for the Streaming Index
STREAMING_INDEX_OUTPUT = "path/to/streaming_index.parquet"
# Checkpoint location for structured streaming
STREAMING_CHECKPOINT = "path/to/streaming_checkpoint"

def main():
    spark = SparkSession.builder \
        .appName("StreamingIndexJob") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # 1) Read Transaction from Kafka
    tx_raw = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", TRANSACTION_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    tx_df = tx_raw.select(
        F.from_json(F.col("value").cast("string"), TransactionSchema).alias("tx")
    ).select("tx.*")

    # 2) Read FraudSignal from Kafka
    fraud_raw = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", FRAUD_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    fraud_df = fraud_raw.select(
        F.from_json(F.col("value").cast("string"), FraudSignalSchema).alias("fs")
    ).select("fs.*")

    # 3) Join Transaction & FraudSignal on transaction_id
    joined_tx_fraud = tx_df.join(
        fraud_df,
        on="transaction_id",
        how="inner"
    )

    # 4) Load dimension data (User, Merchant, Policy, RiskRule)
    user_dim = spark.read.schema(UserSchema).parquet(USER_DIM_PATH)
    merchant_dim = spark.read.schema(MerchantSchema).parquet(MERCHANT_DIM_PATH)
    policy_dim = spark.read.schema(PolicySchema).parquet(POLICY_DIM_PATH)
    riskrule_dim = spark.read.schema(RiskRuleSchema).parquet(RISKRULE_DIM_PATH)

    # 5) Join with User dimension
    joined_user = joined_tx_fraud.join(
        user_dim,
        on="user_id",
        how="inner"
    )

    # 6) Join with Merchant dimension
    joined_merchant = joined_user.join(
        merchant_dim,
        on="merchant_id",
        how="inner"
    )

    # 7) Join with Policy dimension (if transaction has a policy_id)
    #    We'll do a left join, since not all transactions might have a policy_id
    joined_policy = joined_merchant.join(
        policy_dim,
        on="policy_id",
        how="left"
    )

    # 8) Join with RiskRule dimension if we want to link a reason_code or something
    #    For demonstration, let's assume fraud_signal.reason_code == riskrule_dim.rule_id
    joined_risk = joined_policy.join(
        riskrule_dim,
        joined_policy["reason_code"] == riskrule_dim["rule_id"],
        how="left"
    )

    # Now we have a single DataFrame with all fields from Transaction, FraudSignal,
    # and each dimension. We'll call this the "Streaming Index."
    streaming_index_df = joined_risk

    # 9) Write to Parquet in micro-batches
    query = streaming_index_df.writeStream \
        .format("parquet") \
        .option("path", STREAMING_INDEX_OUTPUT) \
        .option("checkpointLocation", STREAMING_CHECKPOINT) \
        .outputMode("append") \
        .trigger(processingTime="10 seconds") \
        .start()

    query.awaitTermination()
    spark.stop()

if __name__ == "__main__":
    main()
```

**Key Points**  
- **No broadcast** usage. We do standard inner/left joins.  
- **Final** approach for dimension joins.  
- Produces a “Streaming Index” in Parquet, appended every 10 seconds.

---

## 3) batch_index.py

Processes historical or large data from local/HDFS/S3, merges Transaction, FraudSignal, and dimension data, and writes a **“Batch Index”** in Parquet.

```python
# batch_index.py
#
# 1) Reads Transaction, FraudSignal from some historical path (Parquet).
# 2) Reads dimension data (User, Merchant, Policy, RiskRule).
# 3) Performs all joins.
# 4) Outputs a "Batch Index" as Parquet.

from pyspark.sql import SparkSession, functions as F
from schemas import (
    TransactionSchema, FraudSignalSchema,
    UserSchema, MerchantSchema, PolicySchema, RiskRuleSchema
)

# Paths to raw historical data. Adjust as necessary.
HISTORICAL_TX_PATH = "path/to/historical/transactions.parquet"
HISTORICAL_FRAUD_PATH = "path/to/historical/fraud_signals.parquet"

# Dimension table paths
USER_DIM_PATH = "path/to/user_dim.parquet"
MERCHANT_DIM_PATH = "path/to/merchant_dim.parquet"
POLICY_DIM_PATH = "path/to/policy_dim.parquet"
RISKRULE_DIM_PATH = "path/to/riskrule_dim.parquet"

# Output for the Batch Index
BATCH_INDEX_OUTPUT = "path/to/batch_index.parquet"

def main():
    spark = SparkSession.builder \
        .appName("BatchIndexJob") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # 1) Read historical data
    tx_df = spark.read.schema(TransactionSchema).parquet(HISTORICAL_TX_PATH)
    fraud_df = spark.read.schema(FraudSignalSchema).parquet(HISTORICAL_FRAUD_PATH)

    # 2) Read dimension data
    user_dim = spark.read.schema(UserSchema).parquet(USER_DIM_PATH)
    merchant_dim = spark.read.schema(MerchantSchema).parquet(MERCHANT_DIM_PATH)
    policy_dim = spark.read.schema(PolicySchema).parquet(POLICY_DIM_PATH)
    riskrule_dim = spark.read.schema(RiskRuleSchema).parquet(RISKRULE_DIM_PATH)

    # 3) Join Transaction & FraudSignal
    joined_tx_fraud = tx_df.join(
        fraud_df,
        on="transaction_id",
        how="inner"
    )

    # 4) Join with user dim (inner)
    joined_user = joined_tx_fraud.join(
        user_dim,
        on="user_id",
        how="inner"
    )

    # 5) Join with merchant dim (inner)
    joined_merchant = joined_user.join(
        merchant_dim,
        on="merchant_id",
        how="inner"
    )

    # 6) Join with policy dim (left, in case some transactions have no policy)
    joined_policy = joined_merchant.join(
        policy_dim,
        on="policy_id",
        how="left"
    )

    # 7) Join with risk rule dimension (left), linking reason_code to rule_id
    final_batch_df = joined_policy.join(
        riskrule_dim,
        joined_policy["reason_code"] == riskrule_dim["rule_id"],
        how="left"
    )

    # 8) Write "Batch Index"
    final_batch_df.write.mode("overwrite").parquet(BATCH_INDEX_OUTPUT)

    spark.stop()

if __name__ == "__main__":
    main()
```

**Key Points**  
- **All joins** are spelled out, no broadcast, no partial references.  
- Produces “Batch Index,” fully enriched with dimension data.

---

## 4) final_aggregate.py

Merges the **Streaming Index** and **Batch Index** into a single **“Final Aggregate Index”**. If both indexes contain the same transaction_id, we pick the **latest** record by timestamp. No optional logic—this is final.

```python
# final_aggregate.py
#
# 1) Reads "Streaming Index" from Parquet
# 2) Reads "Batch Index" from Parquet
# 3) Union and deduplicate by picking the row with the latest timestamp if duplicates exist
# 4) Writes "Final Aggregate Index"

from pyspark.sql import SparkSession, functions as F, Window

STREAMING_INDEX_PATH = "path/to/streaming_index.parquet"
BATCH_INDEX_PATH = "path/to/batch_index.parquet"
FINAL_INDEX_OUTPUT = "path/to/final_index.parquet"

def main():
    spark = SparkSession.builder \
        .appName("FinalAggregateJob") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # 1) Read streaming & batch indexes
    streaming_df = spark.read.parquet(STREAMING_INDEX_PATH)
    batch_df = spark.read.parquet(BATCH_INDEX_PATH)

    # 2) Union them by name
    combined_df = streaming_df.unionByName(batch_df)

    # 3) Deduplicate if the same transaction_id appears in both.
    #    We'll parse "timestamp" as a real timestamp for ordering.
    #    We assume there's a single field named 'timestamp' from the transaction side.
    combined_df = combined_df.withColumn(
        "ts_parsed", F.to_timestamp(F.col("timestamp"))
    )

    # Use a window partitioned by transaction_id, ordered by ts_parsed desc
    w = Window.partitionBy("transaction_id").orderBy(F.col("ts_parsed").desc())
    deduped = combined_df.withColumn("rn", F.row_number().over(w)) \
                        .filter(F.col("rn") == 1) \
                        .drop("rn", "ts_parsed")

    # 4) Write final aggregate
    deduped.write.mode("overwrite").parquet(FINAL_INDEX_OUTPUT)

    spark.stop()

if __name__ == "__main__":
    main()
```

**Key Points**  
- We do a straightforward **union** then pick the record with the latest `timestamp`.  
- No optional logic; this is final, decided approach.  
- Produces “Final Aggregate Index.”

---

## Final Summary

1. **Schemas** are defined in one place (`schemas.py`), ensuring consistency.  
2. **streaming_index.py** does real-time ingestion from Kafka for **Transaction** and **FraudSignal**, then joins everything (User, Merchant, Policy, RiskRule) in **inner** or **left** joins, writing a final **“Streaming Index.”**  
3. **batch_index.py** does the same style of joins for historical data, generating a **“Batch Index.”**  
4. **final_aggregate.py** merges these two indexes, picking the newest record on conflict, ensuring **no** partial or uncertain logic.  

This codebase has a **single** decided approach for each step:  
- **Inner join** for mandatory relationships (Transaction → FraudSignal, Transaction → User, Transaction → Merchant).  
- **Left join** where data might be optional (Policy, RiskRule).  
- **No broadcast** or “maybe” logic.  
- **Final aggregator** merges indexes with a standard dedup strategy.

Students can now see a complete, production-like pipeline with consistent, unambiguous design and code. This aligns with a teaching environment where each step is **explicit** and decisions are already **made**—no placeholders, no “or” statements, and no partial references.