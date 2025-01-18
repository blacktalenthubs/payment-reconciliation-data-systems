Below is a deeper **storage-focused refactoring** of our **batch pipelines** to address cost optimization, partitioning, compression, and data management best practices. We’ll first outline **common storage problems** and **solutions**, then provide an updated **batch pipeline** snippet that incorporates these strategies (e.g., Parquet partitioning, compression codecs, file merging). This helps keep data size and costs under control while improving query performance.

---

# Storage Problems & Cost Optimizations

## 1) Excessive Small Files
**Problem**: If each Spark job writes many small Parquet files, metadata overhead in S3 or HDFS grows, plus queries slow down due to reading lots of tiny files.  
**Solution**:  
- **Coalesce or Repartition** to produce a smaller number of larger files (e.g., 128–512 MB per file).  
- Periodically **merge** small files (e.g., a final step after each daily job).

## 2) Lack of Meaningful Partitioning
**Problem**: Data is stored in a flat directory structure without partition columns. This leads to full-table scans for typical queries, wasting I/O.  
**Solution**:  
- Partition the Parquet output by columns commonly used for filters (e.g., `date`, `merchant_id`, or `status`).  
- Tools like Athena, Hive, Spark can then skip irrelevant partitions.

## 3) No Compression or Inefficient Codecs
**Problem**: Storing uncompressed or poorly compressed Parquet files leads to high storage costs and slower I/O.  
**Solution**:  
- Use columnar compression in Parquet with efficient codecs like **Snappy** or **ZSTD**.  
- Snappy is widely supported and provides a good balance of compression ratio and speed.

## 4) Stale/Unused Data
**Problem**: Old or rarely accessed data remains in expensive storage, raising costs.  
**Solution**:  
- Lifecycle policies (e.g., S3 tiering to Glacier or Infrequent Access after N days).  
- Periodically archive or delete unused snapshots.

## 5) Overly Wide Columns or Repeated Fields
**Problem**: Storing columns that are rarely used or large string fields repeated in every row.  
**Solution**:  
- **Schema Pruning**: Write only the columns needed for certain use cases; keep large optional fields in separate datasets.  
- Use an approach like **Delta Lake** or **Iceberg** for incremental schema evolution if needed.

## 6) Poor File Format Choice
**Problem**: Using row-based formats (CSV, JSON) or older columnar formats (ORC) might hinder interoperability or performance.  
**Solution**:  
- **Parquet** is usually the default choice in modern Spark-based data lakes.  
- If advanced ACID transactions or time travel is required, consider **Delta** or **Iceberg**.

## 7) Unclear Retention / Versioning Strategy
**Problem**: Keeping indefinite versions or snapshots of large datasets.  
**Solution**:  
- Define retention periods for intermediate data or partial aggregates.  
- Only keep final, daily or monthly snapshots for compliance.  
- Use S3 object versioning carefully; remove old versions if not needed.

## 8) Inconsistent Partition Granularity
**Problem**: Over-partitioning (e.g., partition by `merchant_id` if you have 100,000 merchants) can create huge directory structures. Under-partitioning can force scanning big files.  
**Solution**:  
- Choose partition columns that strike a balance (often `year`, `month`, `day` for time-based data). For a moderate cardinality dimension, you might partition by “merchant_category” instead of “merchant_id.”

## 9) Excessive Rewrites
**Problem**: Overwriting entire large tables daily, even if minimal changes occur.  
**Solution**:  
- **Partition Overwrites**: Only overwrite the partitions that changed (e.g., today’s data).  
- Consider incremental frameworks (Delta, Iceberg) that handle merges.

## 10) Not Exploiting Column Pruning
**Problem**: Queries read all columns from Parquet even if only 2 columns are needed, or the data was not properly structured for pruning.  
**Solution**:  
- Carefully specify columns (`select()`) in Spark read steps.  
- Use columnar formats (Parquet) so the engine physically skips reading unused columns.

---

# Refactored Batch Pipeline Example

Below is a **storage-optimized** version of our **batch_index.py**, focusing on:

1. **Partitioning** the output by a relevant column (like `status` or a date extracted from `timestamp`).  
2. **Using Snappy compression**.  
3. **Controlling the number of output files** with a coalesce or repartition.  
4. **Potentially dropping columns** we no longer need for the final index.  

```python
"""
batch_index_refactored.py

This version shows how to:
- Partition final Parquet data by a relevant column (e.g., date or status)
- Use Snappy compression
- Coalesce or Repartition output
- Possibly prune columns
"""

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

# Import the same schemas
from schemas import (
    TransactionSchema, FraudSignalSchema,
    UserSchema, MerchantSchema, PolicySchema, RiskRuleSchema
)

# Input paths for "raw" or historical data
HISTORICAL_TX_PATH = "path/to/historical/transactions.parquet"
HISTORICAL_FRAUD_PATH = "path/to/historical/fraud_signals.parquet"

# Dimension table paths
USER_DIM_PATH = "path/to/user_dim.parquet"
MERCHANT_DIM_PATH = "path/to/merchant_dim.parquet"
POLICY_DIM_PATH = "path/to/policy_dim.parquet"
RISKRULE_DIM_PATH = "path/to/riskrule_dim.parquet"

# Output for the "Batch Index"
# We'll store it partitioned by "status", and compressed with Snappy.
BATCH_INDEX_OUTPUT = "path/to/batch_index_partitioned.parquet"

def main():
    spark = (SparkSession.builder
             .appName("BatchIndexRefactored")
             # 1) Shuffle & compression config
             .config("spark.sql.shuffle.partitions", "200")
             .config("spark.sql.parquet.compression.codec", "snappy")
             .getOrCreate())

    spark.sparkContext.setLogLevel("WARN")

    # 1) Load main fact data
    tx_df = spark.read.schema(TransactionSchema).parquet(HISTORICAL_TX_PATH)
    fraud_df = spark.read.schema(FraudSignalSchema).parquet(HISTORICAL_FRAUD_PATH)

    # 2) Load dimension data
    user_dim = spark.read.schema(UserSchema).parquet(USER_DIM_PATH)
    merchant_dim = spark.read.schema(MerchantSchema).parquet(MERCHANT_DIM_PATH)
    policy_dim = spark.read.schema(PolicySchema).parquet(POLICY_DIM_PATH)
    riskrule_dim = spark.read.schema(RiskRuleSchema).parquet(RISKRULE_DIM_PATH)

    # 3) Join Transaction & FraudSignal
    #    We'll do an inner join on transaction_id, but you might do a left if not all have signals
    joined_tx_fraud = tx_df.join(
        fraud_df,
        on="transaction_id",
        how="inner"
    )

    # 4) Join with user, merchant, policy, riskrule
    #    We'll do multiple joins in one chain:
    joined_all = (joined_tx_fraud
                  .join(user_dim, "user_id", "inner")
                  .join(merchant_dim, "merchant_id", "inner")
                  .join(policy_dim, "policy_id", "left")
                  .join(riskrule_dim,
                        joined_tx_fraud["reason_code"] == riskrule_dim["rule_id"],
                        "left"))

    # 5) (Optional) Convert "timestamp" to a date column to partition
    #    We'll create a new column "trans_date" = cast to date
    enriched_df = joined_all.withColumn(
        "trans_date",
        F.to_date(F.col("timestamp"))
    )

    # 6) (Optional) Column pruning
    #    Suppose we only need some columns in the final index:
    final_columns = [
        "transaction_id",
        "user_id", "user_name",
        "merchant_id", "merchant_name",
        "amount", "currency", "status",
        "policy_id", "policy_name",
        "fraud_score", "reason_code",
        "trans_date"  # newly derived partition col
    ]
    final_batch_df = enriched_df.select(*final_columns)

    # 7) Repartition or coalesce if we want fewer files. Example: we produce 20 output files.
    final_batch_df = final_batch_df.repartition(20)

    # 8) Write out partitioned by "status" and compressed with Snappy
    #    (We already set "spark.sql.parquet.compression.codec" to "snappy".)
    final_batch_df.write \
        .mode("overwrite") \
        .partitionBy("status") \
        .parquet(BATCH_INDEX_OUTPUT)

    spark.stop()

if __name__ == "__main__":
    main()
```

### Explanation

1. **Shuffle & Compression**  
   - We set `spark.sql.shuffle.partitions` to 200 for controlled parallelism.  
   - We set `spark.sql.parquet.compression.codec` = “snappy” for the entire session.  

2. **Partitioning**  
   - In the final write step, we do `.partitionBy("status")` so the output structure looks like:
     ```
     batch_index_partitioned.parquet/
       status=AUTHORIZED/
       status=DECLINED/
       status=PENDING/
       status=REFUNDED/
     ```
   Queries filtering on `status="AUTHORIZED"` can skip all other directories.

3. **Coalescing to 20 files**  
   - We explicitly do `repartition(20)` so we end up with about 20 files per partition directory. This avoids writing hundreds or thousands of small files.

4. **Optional**:  
   - If we have a date/time column for queries, we might partition by `trans_date` or `(trans_date, status)` if that’s a frequent filter dimension.  
   - If dimension tables are large but we still want to store them, we keep them in a separate folder. If they are small, we might broadcast them (not shown, since we’re focusing on the storage side now).

---

# Conclusion

By **partitioning** on a relevant column (e.g., `status` or `trans_date`), **coalescing** output, using **Snappy compression**, and **pruning** unneeded columns, we address several core storage and cost problems:

1. **Fewer, larger files** reduce overhead for metadata and speed up queries.  
2. **Partitioned directories** allow partition pruning, cutting down I/O for typical queries.  
3. **Columnar compression** with Snappy shrinks file sizes, saving S3 or HDFS storage costs.  
4. **Column pruning** ensures we don’t store or read columns we don’t need.  

These optimizations form the baseline for a cost-effective, high-performance Spark batch pipeline. Combined with the earlier mention of life-cycle policies for old data (to move rarely accessed data to cheaper storage), you have a robust approach to storing large volumes of batch-processed data in a more **cost-efficient** and **query-optimized** manner.