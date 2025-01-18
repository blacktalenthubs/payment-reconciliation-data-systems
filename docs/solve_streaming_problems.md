Below is a **single code example** demonstrating how to incorporate solutions for several **streaming issues**—all in one **Spark Structured Streaming** job. This example tackles:

1. **Checkpointing** to avoid data loss.  
2. **Rate Limiting** (max rate per partition).  
3. **Handling Out-of-Order Data** with a watermark.  
4. **Data Skew Mitigation** via salting.  
5. **Deduplication** if duplicate events appear.  
6. **Write Efficiency** (fewer, larger files).  
7. **Adaptive Execution & Partitioning** to reduce shuffle overhead.

This script assumes you are reading **transactions** and **fraud signals** from Kafka, with possible out-of-order event times, duplicates, and skew around certain merchant IDs. Adjust field names, paths, or references to your actual environment.

---

## streaming_index_advanced.py

```python
"""
streaming_index_advanced.py

Spark Structured Streaming job that:
1) Reads from Kafka topics (transactions & fraud_signals).
2) Mitigates data skew by salting "merchant_id" if needed.
3) Deduplicates by transaction_id to handle duplicates.
4) Uses a watermark for out-of-order data based on 'event_time'.
5) Rate limits ingestion if data surges.
6) Writes fewer, larger Parquet files with compression.
7) Uses checkpointing and adaptive execution to handle restarts and partial aggregates.
"""

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)

# Example schemas
TransactionSchema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("merchant_id", StringType(), True),
    StructField("event_time", StringType(), True),  # We'll parse as a real Timestamp
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("status", StringType(), True)
])

FraudSignalSchema = StructType([
    StructField("fraud_id", StringType(), True),
    StructField("transaction_id", StringType(), True),
    StructField("model_version", StringType(), True),
    StructField("fraud_score", DoubleType(), True),
    StructField("inference_time", StringType(), True),
    StructField("reason_code", StringType(), True)
])

def main():
    # Build the Spark session with relevant configs
    spark = (
        SparkSession.builder
        .appName("AdvancedStreamingIndex")
        # 1) Rate limiting & shuffle partitions
        .config("spark.streaming.kafka.maxRatePerPartition", "1000")       # Example limit
        .config("spark.sql.shuffle.partitions", "200")                     # Manage shuffle tasks
        # 2) Adaptive Execution can help mitigate shuffle overhead & skew
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.shuffle.reducePostShufflePartitions.enabled", "true")
        # 3) Parquet compression
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Kafka config & topic info
    KAFKA_BOOTSTRAP = "localhost:9092"
    TX_TOPIC = "transactions_topic"
    FRAUD_TOPIC = "fraud_signals_topic"

    # Read from transactions Kafka
    tx_raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TX_TOPIC)
        # 4) If you'd like to handle from earliest offset, do so; 'latest' is typical for new consumption
        .option("startingOffsets", "latest")
        .load()
    )
    tx_df = tx_raw.select(F.from_json(F.col("value").cast("string"), TransactionSchema).alias("t")).select("t.*")

    # Read from fraud signals Kafka
    fraud_raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", FRAUD_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )
    fraud_df = fraud_raw.select(F.from_json(F.col("value").cast("string"), FraudSignalSchema).alias("f")).select("f.*")

    # Convert event_time to an actual Timestamp for watermarking
    tx_df = tx_df.withColumn("event_ts", F.to_timestamp("event_time"))
    fraud_df = fraud_df.withColumn("inference_ts", F.to_timestamp("inference_time"))

    # (Optional) Watermark for out-of-order data: assume 10m max lateness
    # This is relevant if doing time-based aggregations. If we only do simple transformations,
    # the watermark might not be strictly needed. For demonstration:
    tx_df = tx_df.withWatermark("event_ts", "10 minutes")
    fraud_df = fraud_df.withWatermark("inference_ts", "10 minutes")

    # 5) Join on transaction_id
    joined = tx_df.join(fraud_df, on="transaction_id", how="left")

    # 6) Data skew mitigation (salting). If merchant_id is extremely skewed, we can add a salt:
    # If you see one merchant with 90% of data, you can do:
    SALT_FACTOR = 10  # e.g., 10 buckets
    joined = joined.withColumn("merchant_salt", (F.rand() * SALT_FACTOR).cast("int"))
    # Then you can partition or group by merchant_id + salt if needed in later transformations.
    # For writing, you can store merchant_id_salted = merchant_id || '_' || merchant_salt

    # 7) Deduplicate by transaction_id if duplicates can appear
    # If the pipeline can see the same transaction multiple times, do a stateful dedup:
    # We'll do a dropDuplicates on the key transaction_id, but we must keep a small in-memory or checkpointed state.
    # Because it's streaming, we use a Watermark approach + dropDuplicates:
    deduped = joined.dropDuplicates(["transaction_id", "event_ts"])

    # Additional transformations as needed...
    # For demonstration, let's just rename a few columns
    final_df = deduped.select(
        "transaction_id", "user_id", "merchant_id", "amount", "currency", "status",
        "model_version", "fraud_score", "reason_code", "event_ts"
    )

    # 8) Write out fewer, larger Parquet files
    # Using outputMode="append", plus a triggered micro-batch every 10 seconds, for example.
    # We specify a checkpoint location for fault-tolerance.

    query = (
        final_df.writeStream
        .format("parquet")
        .option("checkpointLocation", "path/to/advanced_stream_checkpoint")
        .option("path", "path/to/advanced_streaming_index")   # E.g., s3://my-bucket/streaming_index
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .start()
    )

    query.awaitTermination()
    spark.stop()

if __name__ == "__main__":
    main()
```

---

## How These Code Changes Address the 7 Problems

1. **Backpressure / High Input Rate**  
   - We set `spark.streaming.kafka.maxRatePerPartition=1000`. This caps consumption at 1000 messages/partition/second, preventing an onslaught if Kafka surges.

2. **Data Loss on Failure**  
   - The `.option("checkpointLocation", "path/to/advanced_stream_checkpoint")` ensures Spark commits offsets and state so restarts don’t cause reprocessing or data loss.

3. **Out-of-Order / Late Data**  
   - `withWatermark("event_ts", "10 minutes")` tells Spark to keep waiting for up to 10 minutes of late-arriving data for any windowed operations or dedup.  
   - If you have a window-based aggregation, Spark uses that watermark to handle event-time correctly.

4. **Data Skew**  
   - We add a `merchant_salt` column to spread out data for extreme skew. You could partition or group by `(merchant_id, merchant_salt)` in further transforms. That’s a typical salting approach.

5. **Duplication**  
   - We do `dropDuplicates(["transaction_id", "event_ts"])` in a streaming context. This can keep a small state (depending on your watermark) so that re-delivered events won’t get double-counted.  
   - For large-scale dedup, you might need a more robust approach with a TTL-based state store.

6. **High-Latency / Slow Sinks**  
   - We produce data in micro-batches (append mode) every 10 seconds. This chunk-based writing ensures we’re not sending tiny files each second.  
   - If S3 writes are still slow, you can up the trigger interval or add partitioning by time.

7. **File Efficiency**  
   - We rely on Spark’s normal micro-batch approach to produce fewer files. If we want even fewer, we might add a final coalesce or let a separate job do file merging.  
   - We set `"spark.sql.parquet.compression.codec"="snappy"` so each file is compressed.

---

## Summary of the Code Changes

- **Checkpointing**: Always set `checkpointLocation` in `.writeStream(...)`.  
- **Rate Limiting**: `spark.streaming.kafka.maxRatePerPartition`.  
- **Watermark**: `withWatermark("event_ts", "10 minutes")` for out-of-order data.  
- **Skew**: Salting technique around `merchant_id`.  
- **Dedup**: `dropDuplicates(...)` with a watermark-based approach.  
- **Config for Shuffle & Adaptive Execution**: `spark.sql.shuffle.partitions`, `spark.sql.adaptive.enabled`.  
- **Write Config**: Snappy compression, micro-batch intervals, path for final Parquet output.

This single script exemplifies how to address multiple streaming pitfalls—students can compare it to a simpler, less robust version to see how each feature improves reliability, scalability, and correctness.