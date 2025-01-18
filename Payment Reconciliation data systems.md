# **Payment Reconciliation Index System**


---

## **2. Detailed Design: Streaming & Batch**

### **2.1 Data Modeling**

- **Transactions**: Core fields (transaction_id, user_id, merchant_id, amount, currency, timestamp, status).  
- **Fraud Signals**: For suspicious events (fraud_id, transaction_id, fraud_score, reason_code).  
- **Merchant Data**: (merchant_id, merchant_name, risk_level).  
- **Indexes**: Reconciled or aggregated data sets derived from these raw sources.

**Key**: Each index is a specialized **view** (or aggregated table) that downstream teams use. Some are partial (real-time), some are final (batch).

### **2.2 Streams Pipeline**

1. **Input**: MSK (Kafka) receiving “fraud_signals_topic” or real-time transaction events.  
2. **Spark Streaming** on EKS:
   - **Read** from MSK in micro-batches or continuous mode.  
   - **Optionally join** with dimension data (merchant, user) in S3 or broadcast.  
   - **Produce** partial or real-time indexes → S3 (e.g. “fraud index”, “ops index”).  
   - **Low-latency** (~seconds) ensures BF1 & BF6 are always updated.

### **2.3 Batch Pipeline**

1. **Input**: S3 (transactions, partial indexes from streaming).  
2. **EMR** (Spark batch):
   - **Nightly** or scheduled to unify transaction logs, fraud signals, settlement data.  
   - **Generate** final “reconciliation index” or aggregated “merchant index.”  
   - **Write** these final indexes to S3 for consumption by BF2, BF3, BF5, BF7.  
3. This ensures thorough data coverage and advanced computation requiring large merges or groupBy over extended historical data.

---

## **3. Infrastructure Requirements & Architecture**

### **3.1 Infrastructure Components**

1. **AWS VPC & Subnets**  
   - **Private** subnets for MSK brokers and EKS worker nodes.  
   - **Public** subnets for EMR master node if needed.  
   - NAT gateway for outbound internet if required.

2. **MSK (Kafka)**  
   - 2–3 broker nodes in private subnets.  
   - “fraud_signals_topic” auto-created or manually created using a K8s job or ephemeral EC2.

3. **EKS Cluster**  
   - 1 node group (t3.medium or scale up if needed).  
   - Spark-based streaming code runs as a K8s deployment, container images in ECR.  
   - Low-latency consumption from MSK.

4. **EMR**  
   - Master + Core nodes.  
   - Steps triggered nightly or on-demand, reading from S3, writing final indexes to S3.  
   - IAM roles for S3 read/write, logs in S3.

5. **S3**  
   - “raw transactions”, “fraud signals”, partial “indexes” from streaming, final “indexes” from batch.  
   - Possibly partitioned by date, merchant, etc.

6. **ECR**  
   - Stores Docker images: one for streaming (PySpark + Kafka connector), one for batch (PySpark).  

**All** orchestrated via **Terraform** for consistent spin-up.

### **3.2 Architecture Diagram**

```
                             +-----------------------------+
                             |   Payment Systems (API)     |
                             +-----------------------------+
                                      | (fraud signals)
                                      v
      +----------------------------------------------------+
      |         MSK (Kafka) : "fraud_signals_topic"        |
      +---------------------+------------------------------+
                            | streaming 
                            v
        +----------------------------------------+
        | EKS: Spark Streaming (Docker/ECR)      |
        | - reads MSK -> produces partial index  |
        | - writes to S3 => "fraud_index", etc.  |
        +----------------------------------------+
                         | batch read
                         v
        +----------------------------------------+
        | EMR : Spark Batch                     |
        | - merges partial indexes, transaction |
        |   logs => final "reconciliation idx"  |
        | - writes to S3 => consumed by BFS     |
        +----------------------------------------+
```

**Explanation**:  
- Real-time pipeline in EKS keeps partial indexes updated for immediate queries.  
- Batch pipeline on EMR finalizes heavy merges, generating consolidated indexes for finance, fraud, compliance, etc.

### **3.3 Deployment Steps**

1. **Terraform** apply:
   - VPC, subnets, MSK, EKS, EMR, S3, ECR.  
2. **Build** Docker images (streaming & batch) → push to ECR.  
3. **Create** K8s deployment for streaming job referencing `fraud_signals_topic`.  
4. **Upload** batch script to S3 → run EMR step nightly or on-demand.  
5. **Check** logs in CloudWatch or S3 for each job.

---

**Business Functions (Functional Requirements)**  
• **Real-Time Fraud Visibility**: The system provides an immediate index of suspicious or high-risk transactions that can be consumed by fraud analysts or automated alert systems.  
• **Transactional Reconciliation**: Each day, finance teams leverage a consolidated “reconciliation index” to match internal transactions with bank or aggregator statements.  
• **Merchant Performance Dashboards**: Aggregated data for each merchant’s volume, chargebacks, or refunds is made available for partnerships and sales to track performance and highlight anomalies.  
• **User Spend Profiling**: Summaries of user spending patterns or repeated declines feed marketing or risk teams, enabling targeted promotions or flagging suspicious behavior.  
• **Compliance & Auditing**: A compliance index consolidates required fields for KYC/AML checks and external audits.  
• **Settlement & Payout Timelines**: The system produces a schedule index that treasury can use to see upcoming disbursements, ensuring timely payouts.  
• **Operational Monitoring**: Operational teams view real-time throughput, latencies, or error rates from an operations index to maintain healthy service levels.

**Non-Functional Expectations**  
• **Scalability**: Must handle thousands of transactions per second in real-time while also processing large historical sets.  
• **Low Latency**: Real-time indexes should be updated within seconds.  
• **Reliability**: No data loss if a streaming job restarts. Batch runs must complete successfully or alert on failure.  
• **Cost Efficiency**: Resources should autoscale where possible. Storage footprints minimized.  
• **Security & Access Controls**: Only authorized roles can read/write specific indexes. Data encryption in transit (TLS) for MSK, SSE-KMS for S3.  
• **Data Quality**: Must filter or quarantine malformed events; final indexes must be consistent.

**Infrastructure & High-Level Design**  
• **AWS MSK (Kafka)**: Accepts real-time fraud/transaction events.  
• **EKS**: Runs Spark Streaming as a Kubernetes Deployment (or Spark Operator). Consumes from MSK, writes partial indexes to S3.  
• **EMR**: Runs batch Spark jobs that unify raw logs + partial indexes → final reconciliation or merchant indexes.  
• **S3**: Central data lake for partial indexes (real-time) and final indexes (batch).  

```
  Payment or Fraud Producers → MSK → EKS (Streaming) → S3 → EMR (Batch) → S3 (final)
```

**Data Skew**  
- **Problem**: Some merchants or user IDs dominate the data, causing spark tasks to over-concentrate on a few partitions.  
- **Code Snippet** (example in `batch_index.py`):
  ```python
  from pyspark.sql import functions as F

  # Salting approach
  df = df.withColumn("salt", F.expr("floor(rand() * 10)"))
  df = df.repartition("merchant_id", "salt")  # Spreads skewed keys
  aggregated = df.groupBy("merchant_id", "salt").agg(...)  # or further transformations
  ```
- **By introducing a `salt` column, heavily skewed keys are divided across multiple tasks, preventing straggler tasks.

**Long-Running Queries**  
- **Problem**: Certain groupBy or join operations run excessively long in EMR or the streaming job.  
- **Code Snippet** (Spark config for batch):
  ```python
  # In main code or spark-submit params
  spark.conf.set("spark.sql.shuffle.partitions", "200")  # reduce from a high default
  spark.conf.set("spark.dynamicAllocation.enabled", "true")
  spark.conf.set("spark.dynamicAllocation.maxExecutors", "50")
  ```
- **Dynamic allocation helps add executors if a query scales up. Adjust shuffle partitions to lower overhead on medium data volumes.

**Watermarks & State Management in Streaming**  
- **Problem**: Late-arriving events or indefinite state can cause memory bloat or incorrect real-time aggregates.  
- **Code Snippet** (in `streaming_index.py`):
  ```python
  from pyspark.sql.window import Window

  streaming_df = parsed_df \
    .withWatermark("event_time", "2 hours") \
    .groupBy(
      F.window("event_time", "1 hour"),
      "merchant_id"
    ).agg(F.count("*").alias("count"))
  ```
- **A watermark discards data older than 2 hours, preventing unbounded state if events arrive extremely late. Spark discards states for older windows once the watermark moves.

**Storage / Small Files**  
- **Problem**: Continuous micro-batches can create many small Parquet files in S3, causing high overhead.  
- **Code Snippet**:
  ```python
  # streaming_index.py
  query = (df.writeStream
    .format("parquet")
    .option("path", "s3a://my-stream-output/")
    .option("checkpointLocation", "s3a://my-checkpoints/")
    .option("maxRecordsPerFile", 500000)  # limit # of records per output file
    .start()
  )
  ```
- **For a streaming sink, limiting records per file or using a daily compaction job merges small files into larger ones.

**Compression & File Formats**  
- **Problem**: CSV is large; default snappy parquet might or might not be best for repeated read patterns.  
- **Code Snippet**:
  ```python
  spark.conf.set("spark.sql.parquet.compression.codec", "zstd")
  df.write.mode("overwrite").parquet("s3a://some-output/")
  ```
- **Explanation**: ZSTD can be smaller than snappy with good speeds. Minimizes S3 costs while maintaining decent performance.

**Checkpoints & Exactly-Once**  
- **Problem**: If the streaming job restarts and the checkpoint is lost, double-reads can occur.  
- **Code Snippet**:
  ```python
  query = df.writeStream
    .format("parquet")
    .option("checkpointLocation", "s3a://my-checkpoints/streamingJob/")
    .outputMode("append")
    .start()
  ```
- **A stable S3-based checkpoint directory ensures once the job restarts, it picks up the last processed offset exactly. This helps avoid duplicates.

**Shuffle Overhead & Large Joins**  
- **Problem**: The final batch index merges large transactions + dimension data, generating big shuffle stages.  
- **Code Snippet**:
  ```python
  from pyspark.sql.functions import broadcast

  final_df = transactions_df.join(
      broadcast(merchant_dim_df),
      "merchant_id",
      "left"
  )
  ```
- **By broadcasting smaller dimension tables, Spark avoids shuffling them across partitions.

**Spark Config Tuning**  
- **Problem**: OutOfMemory errors or slow tasks if defaults are used.  
- **Code Snippet**:
  ```python
  spark.conf.set("spark.executor.memory", "4g")
  spark.conf.set("spark.driver.memory", "2g")
  spark.conf.set("spark.executor.cores", "2")
  ```
- **Setting more memory or cores helps heavy transformations. On EKS, also specify CPU/mem requests in the container spec to ensure proper scheduling.

**Data Quality & Null Handling**  
- **Problem**: Some transactions have missing user_id or merchant_id.  
- **Code Snippet**:
  ```python
  valid = df.filter("user_id IS NOT NULL AND merchant_id IS NOT NULL")
  invalid = df.filter("user_id IS NULL OR merchant_id IS NULL")

  invalid.writeStream.format("parquet")...
  ```
- **We route invalid rows for quarantine or analysis, ensuring final indexes remain consistent.

**Storage Lifecycle & Cleanup**  
- **Problem**: Old partial indexes accumulate in S3, incurring cost.  
- **Code**: Not in Spark but an AWS lifecycle or a separate housekeeping job:
  ```hcl
  resource "aws_s3_bucket_lifecycle_configuration" "cleanup" {
    bucket = aws_s3_bucket.main.id
    rule {
      id     = "delete-old-indexes"
      status = "Enabled"
      expiration {
        days = 30
      }
      prefix = "partial-indexes/"
    }
  }
  ```
- **After 30 days, partial indexes are deleted. Or we can transition them to Glacier.

**Stateful Aggregations**  
- **Problem**: Summaries by user for a sliding window can grow large.  
- **Code Snippet** (approx):
  ```python
  streaming_df = streaming_df \
    .groupBy("user_id") \
    .agg(F.sum("amount").alias("total_amount")) \
    .option("checkpointLocation", "s3a://my-checkpoints/statefulAgg/")
```
- **Explanation**: Ensure we have adequate memory or set time-based watermarks to drop old data from state.

**Data Format**  
- **Problem**: CSV is easier but large and lacks schema. Parquet or ORC is recommended.  
- **Code**:
  ```python
  df.write.csv("s3a://my-output/csv/")  # old approach
  # vs
  df.write.parquet("s3a://my-output/parquet/")
  ```
- **We unify on Parquet with compression for smaller footprints and faster reads.

**Orchestration**  
- **Problem**: Manual triggers for batch can conflict with streaming.  
- **Solution**: Introduce Airflow or Step Functions to schedule the “upload_and_run_emr_batch.sh” or to verify streaming job is healthy before triggering batch.


Minicube to run Kubernetes locally 