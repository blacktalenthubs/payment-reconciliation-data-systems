Below is a **downstream consumer system** that leverages the **streaming or batch outputs** we’ve created. We’ll assume our final Parquet outputs (e.g., the “Aggregate Index” or “Streaming Index”) reside in **S3** under some well-known paths, and we have various **downstream** use cases (dashboards, analytics queries, additional machine-learning pipelines, etc.). This example focuses on how a typical system might load these outputs into:

1. A **BI Dashboard**: For business and risk analysts.  
2. A **Lightweight Aggregation Service**: For user-facing or operational endpoints.  
3. A **Predictive/ML Pipeline**: For further advanced modeling based on final curated data.

The code examples are conceptual, showing how you’d structure a solution that **reads** from the data product in S3, then serves or processes it further.

---

## 1) BI Dashboard / Ad-Hoc Queries

### 1.1 AWS Athena or Spark SQL

- **Athena**: If we’ve defined external tables on top of the Parquet data, analysts can run SQL queries:
  ```sql
  SELECT
    merchant_name,
    status,
    SUM(amount) AS total_sales
  FROM payment_fraud.final_aggregate
  WHERE event_date >= '2025-01-01'
  GROUP BY merchant_name, status
  ORDER BY total_sales DESC;
  ```
  This is purely **SQL-based** with no additional code, letting business users or data analysts get immediate insights.

- **Spark SQL**: A downstream Spark job can do:
  ```python
  final_df = spark.read.parquet("s3://my-output-bucket/final_index/")
  final_df.createOrReplaceTempView("final_agg")

  top_merchants = spark.sql("""
    SELECT merchant_name, status, SUM(amount) AS total
    FROM final_agg
    GROUP BY merchant_name, status
    ORDER BY total DESC
    LIMIT 20
  """)
  top_merchants.show()
  ```
  This might feed a dashboard or a data warehouse.

### 1.2 BI Tools

- Tools like **Tableau**, **Power BI**, or **Looker** can connect to Athena or a Hive Metastore.  
- They discover the `final_aggregate` table, build visualizations (bar charts of total sales by merchant, time-series of fraud_score averages, etc.).

---

## 2) Lightweight Aggregation Service

Imagine we want a small web service that provides near-real-time aggregated stats to risk or product teams. For example, a daily or hourly rollup of transaction statuses by user or merchant. We can read the **Streaming Index** in S3 every few minutes or hour, pre-aggregate it, and store in a low-latency store like DynamoDB or Redis.

### 2.1 Code Example: Aggregation to Redis

```python
"""
aggregation_service.py

A script that runs periodically (cron, container, or ephemeral Spark job),
reads the streaming output in S3, aggregates, and pushes key metrics into Redis.
"""

import redis
from pyspark.sql import SparkSession, functions as F

STREAMING_OUTPUT_PATH = "s3://my-bucket/streaming_index/"
REDIS_HOST = "myredis.company.com"
REDIS_PORT = 6379

def main():
    spark = SparkSession.builder.appName("DownstreamAggregationService").getOrCreate()

    # Read the latest streaming index
    streaming_df = spark.read.parquet(STREAMING_OUTPUT_PATH)

    # Simple aggregation: total amounts grouped by merchant_id
    # (In practice, might want to filter by last hour/day)
    merchant_agg = (
        streaming_df.groupBy("merchant_id")
        .agg(F.sum("amount").alias("total_amount"))
    )

    # Collect data to driver, or write to a DB if bigger scale
    merchant_list = merchant_agg.collect()

    # Connect to Redis
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)

    # Store each merchant’s aggregated stats
    for row in merchant_list:
        merchant_id = row["merchant_id"]
        total_amount = row["total_amount"]
        r.set(f"merchant:{merchant_id}:total_amount", total_amount)

    spark.stop()

if __name__ == "__main__":
    main()
```

1. **Scheduler**: Could run every 15 minutes or hour.  
2. **Result**: A super-fast key-value store for a front-end or internal dashboard that needs near-real-time aggregated stats.

---

## 3) Additional Machine Learning Pipeline

We might have an advanced ML pipeline that requires the **Final Aggregate** for offline training or more complex feature engineering. For instance, combining the final enriched transactions with user-level features for a new fraud model.

### 3.1 Code Example: PySpark ML Pipeline

```python
"""
ml_training_pipeline.py

Reads the final_aggregate Parquet from S3, preps features,
and trains a new fraud classifier in Spark ML.
"""

from pyspark.sql import SparkSession, functions as F
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import GBTClassifier
from pyspark.ml import Pipeline

FINAL_INDEX_PATH = "s3://my-output-bucket/final_index/"
MODEL_OUTPUT_PATH = "s3://my-model-bucket/fraud_model/"

def main():
    spark = SparkSession.builder.appName("MLTrainingPipeline").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # 1) Load curated data
    df = spark.read.parquet(FINAL_INDEX_PATH)

    # 2) Feature engineering
    # Example: numeric columns: amount, fraud_score
    # We can transform or filter out certain status, etc.
    training_data = df.select(
        "transaction_id",
        "amount",
        "fraud_score",
        # We'll treat 'status' as a label if we consider DECLINED=1, otherwise=0, e.g.
        F.when(F.col("status") == "DECLINED", 1).otherwise(0).alias("label")
    ).na.fill(0)

    assembler = VectorAssembler(
        inputCols=["amount", "fraud_score"],
        outputCol="features"
    )

    # 3) Build a Spark ML pipeline
    gbt = GBTClassifier(labelCol="label", featuresCol="features")
    pipeline = Pipeline(stages=[assembler, gbt])

    # 4) Train model
    model = pipeline.fit(training_data)

    # 5) Save the model to S3 (Spark’s save() format)
    model.write().overwrite().save(MODEL_OUTPUT_PATH)

    spark.stop()

if __name__ == "__main__":
    main()
```

1. **Training**: We interpret the “status=DECLINED” as a proxy for “fraud.”  
2. **New model** is saved in S3, can be deployed to a real-time inference service.  
3. This pipeline runs weekly or daily, consuming the final curated data from the entire pipeline.

---

## 4) Summary of Downstream Examples

1. **BI & Analytics**:  
   - **Athena** or **Spark SQL** on top of the final aggregated datasets.  
   - Integrate with **Tableau** / **Power BI** / **Looker** for dashboards.  

2. **Lightweight Aggregation**:  
   - Periodic job reading the streaming or batch index.  
   - Summaries stored in a key-value store (Redis, DynamoDB) for quick lookups or internal microservices.  

3. **Advanced ML Pipeline**:  
   - Uses the final curated index for offline training, model versioning, and re-deployment.  

With these usage examples, your entire pipeline (from ingestion and streaming/batch transformations to final data products) becomes a **foundation** for real-world business or technical consumers. Students can see **how** the data is actually put to use, bridging the gap between “we have data in S3” and “stakeholders gain value from analytics, dashboards, or new ML models.”