# **Payment Reconciliation Index System**

## Requirements & Scope

**Business Functions (Functional Requirements)**  
**Real-Time Fraud Visibility**: The system provides an immediate index of suspicious or high-risk transactions that can be consumed by fraud analysts or automated alert systems.  
**Transactional Reconciliation**: Each day, finance teams leverage a consolidated “reconciliation index” to match internal transactions with bank or aggregator statements.  
**Merchant Performance Dashboards**: Aggregated data for each merchant’s volume, chargebacks, or refunds is made available for partnerships and sales to track performance and highlight anomalies.  
**User Spend Profiling**: Summaries of user spending patterns or repeated declines feed marketing or risk teams, enabling targeted promotions or flagging suspicious behavior.  
**Compliance & Auditing**: A compliance index consolidates required fields for KYC/AML checks and external audits.  
**Settlement & Payout Timelines**: The system produces a schedule index that treasury can use to see upcoming disbursements, ensuring timely payouts.  
**Operational Monitoring**: Operational teams view real-time throughput, latencies, or error rates from an operations index to maintain healthy service levels.

**(BR1) Real-Time Payment Ingestion**  
   - The system must capture live transactions from payment gateways (or POS systems) in sub-second latency.  
   - It should enrich each transaction with immediate fraud signals (e.g., rule-based flags, ML-derived risk scores).  
**(BR2) Batch Historical Processing**  
   - The system must support daily or periodic batch ingestion of large historical payment datasets.  
   - It should apply deduplication, transformations, and integrate policy control data that updates infrequently.  

**(BR3) Fraud Signal Integration**  
   - The system must integrate ML-generated fraud signals (e.g., suspicious activity scores) and incorporate them into both real-time and batch pipelines.  
   - It should provide a feedback loop for ML models, so new data can be used to retrain or refine fraud detection algorithms.  

**(BR4) Indexed Output for Risk & Compliance**  
   - The system must output an index (or set of indexed tables) that can be queried in near real-time by risk teams.  
   - These indexes should store transaction details, associated risk scores, and relevant policy flags.  

**(BR5) Mixed Deployment Pattern**  
   - The system must run two distinct deployment flows: one for streaming ingestion (and continuous updates) and one for batch ingestion (and scheduled updates).  
   - Both outputs (Stream Index + Batch Index) must be merged to produce a single “Aggregate Index” accessible to downstreams.  

**(BR6) Downstream Data Delivery & Consumption**  
   - The system must publish the enriched data to:  
     - Payment gateway services (e.g., for final authorization checks).  
     - ML pipelines (for further modeling and scoring).  
     - BI/analytics platforms (for dashboards, compliance reporting).  

**(BR7) Monitoring & Alerting**  
   - The system must include real-time monitoring of ingestion, processing latencies, error rates, and fraud score anomalies.  
   - Critical alerts (e.g., pipeline downtime, abnormal spikes in fraud scores) must notify on-call teams immediately.


---

### Non-Functional Requirements

**Performance**  
  - The streaming pipeline should process and index transactions with sub-second latency for time-sensitive decisions.  
  - The batch pipeline must handle large volumes (millions of records) within scheduled SLAs.

**Scalability**  
  - Must auto-scale to handle variable transaction loads (e.g., seasonal spikes).  
  - Should accommodate future growth in data volumes (transaction expansions, new fraud signals).

**Reliability & Availability**  
  - Must provide high uptime for critical payment workflows.  
  - Implement fault tolerance and recovery strategies (e.g., checkpointing in streaming, retry logic for batch).

**Security & Compliance**  
  - Data encryption at rest and in transit for all sensitive payment and personal information.  
  - Strict role-based access control to comply with PCI-DSS and other regulatory standards.

**Data Quality & Governance**  
  - Enforce schema validation and data integrity checks for both streaming and batch inputs.  
  - Maintain an audit trail of data transformations and changes to fraud scores.

**Maintainability**  
  - Clear separation of streaming vs. batch code, with reusable libraries for data transformations.  
  - Automated CI/CD pipelines for frequent updates and safe rollouts.

**Monitoring & Observability**  
  - Centralized logging, metrics, and dashboards to track streaming throughput, batch job times, and error counts.  
  - Real-time alerts on SLA breaches or unusual fraud activity.
**Infrastructure & High-Level Design**  
 **AWS MSK (Kafka)**: Accepts real-time fraud/transaction events.  
 **EKS**: Runs Spark Streaming as a Kubernetes Deployment. Consumes from MSK, writes partial indexes to S3.  
 **EMR**: Runs batch Spark jobs that unify raw logs + partial indexes → final reconciliation or merchant indexes.  
 **S3**: Central data lake for partial indexes (real-time) and final indexes (batch).  
---
## **Detailed Design: Streaming & Batch**

### **Data Modeling**

- **Transactions**: Core fields (transaction_id, user_id, merchant_id, amount, currency, timestamp, status).  
- **Fraud Signals**: For suspicious events (fraud_id, transaction_id, fraud_score, reason_code).  
- **Merchant Data**: (merchant_id, merchant_name, risk_level).  
- **Indexes**: Reconciled or aggregated data sets derived from these raw sources.


### **Streams Pipeline**

**Input**: MSK (Kafka) receiving “fraud_signals_topic” real-time transaction events.
**Spark Streaming** on EKS:
   - **Read** from MSK in micro-batches or continuous mode.  
   - **Optionally join** with dimension data (merchant, user) in S3 or broadcast.  
   - **Produce** partial or real-time indexes → S3 (e.g. “fraud index”, “ops index”).  
   - **Low-latency** (~seconds) ensures BF1 & BF6 are always updated.

### **Batch Pipeline**

**Input**: S3 (transactions, partial indexes from streaming).  
**EMR** (Spark batch):
   - **Nightly** or scheduled to unify transaction logs, fraud signals, settlement data.  
   - **Generate** final “reconciliation index” or aggregated “merchant index.”  
   - **Write** these final indexes to S3 for consumption by BF2, BF3, BF5, BF7.  

---

## **Infrastructure Requirements & Architecture**

### **Infrastructure Components**

**AWS VPC & Subnets**  
   - **Private** subnets for MSK brokers and EKS worker nodes.  
   - **Public** subnets for EMR master node if needed.  
   - NAT gateway for outbound internet if required.

**MSK (Kafka)**  
   - 2–3 broker nodes in private subnets.  
   - “fraud_signals_topic” auto-created or manually created using a K8s job or ephemeral EC2.

**EKS Cluster**  
   - 1 node group (t3.medium or scale up if needed).  
   - Spark-based streaming code runs as a K8s deployment, container images in ECR.  
   - Low-latency consumption from MSK.

**EMR**  
   - Master + Core nodes.  
   - Steps triggered nightly or on-demand, reading from S3, writing final indexes to S3.  
   - IAM roles for S3 read/write, logs in S3.

**S3**  
   - “raw transactions”, “fraud signals”, partial “indexes” from streaming, final “indexes” from batch.  
   - Possibly partitioned by date, merchant, etc.

**ECR**  
   - Stores Docker images: one for streaming (PySpark + Kafka connector), one for batch (PySpark).  

**All** orchestrated via **Terraform** for consistent spin-up and shutdown.

### **Architecture Diagram**


## **Architectural Design for Payment Recon indexing systems** 

```


Data Sources:
Payment Systems
    Transactions
    Users
    Merchants
    Locations
    
Internally Generate Data:
    Policies
    
    
ML Produced Data:
    ML teams consumes payment systems data and produced an inference service fraud detections and classifications 
    
    
    
Data Systems being built:

    Consuems all above data and produced an Indexing systems  
    
    
                                            +-------------------------------------------+
                                            |    Data Producers (APIs, Partner APIs)    |
                                            |  (Flask endpoints, external systems)       |
                                            +-------------------------+------------------+
                              |  
                                                                      v
                                           +---------------------------------------------------+
                                           |   Ingestion Scripts (Python, etc.)                |
                                           |   - batch: ingest_to_s3.py                        |
                                           |   - stream: ingest_to_kafka.py                    |
                                           +----------------------------+-----------------------+
                                                                        | (2) writes files/JSON
                                                                        |     or Parquet to S3
                                                                        v
                             +----------------------------------------------------+
                             |                 S3 (Raw Data Bucket)               |
                             |     (historical transactions, dimension tables)    |
                             +----------------------------------------------------+
                                                                        ^
                                                                        | (3) publishes messages
                                                                        |     to Kafka topics
                             +------------------------------------------+------------------------+
                             |  Kafka / MSK  (streaming input for Spark)              |
                             |  - transactions_topic, fraud_signals_topic, etc.       |
                             +------------------------------------------+------------------------+

                                                
                           +--------------------------------------------------------------+
                           |     Spark Streaming Pipeline (streaming_index_advanced.py)   |
                           |     - reads from Kafka (transactions & fraud)               |
                           |     - merges dimension data (User, Merchant, etc.)          |
                           |     - handles dedup, watermark, salting for skew            |
                           |     - writes "Streaming Index" to S3                        |
                           +-----------------------------^--------------------------------+
                                                         | (4) micro-batch Parquet output
                                                         v
          +------------------------------------------------------------------------------------+
          |                 S3 (Processed Bucket)                                              |
          |     "streaming_index/"  (continuously updated in micro-batches)                    |
          +------------------------------------------------------------------------------------+
                                                         ^
                                                         | (5) overwrites/append from next job
                                                         |
                           +-------------------------------------------------+-----------------+
                           |   Spark Batch Pipeline (batch_index_refactored.py)               |
                           |   - reads raw historical data (transactions, fraud signals) from |
                           |     S3 plus dimension tables                                     |
                           |   - merges, cleans, transforms                                   |
                           |   - writes "Batch Index" in Parquet                              |
                           +-------------------------------------------------+-----------------+
                                                         |
                                                         v
          +------------------------------------------------------------------------------------+
          |                S3 (Processed Bucket)                                               |
          |     "batch_index/" (partitioned, compressed Parquet)                               |
          +------------------------------------------------------------------------------------+
                                                         ^
                                                         | (6) reads both streaming & batch indexes
                                                         v
                           +----------------------------------------------------------+
                           |       Final Aggregator (final_aggregate.py)             |
                           |   - merges "Streaming Index" & "Batch Index"            |
                           |   - deduplicates, picks latest transaction states        |
                           |   - writes "Final Aggregate" in Parquet                 |
                           +---------------------------+------------------------------+
                                                       | (7) final snapshot
                                                       v
          +------------------------------------------------------------------------------------+
          |                      S3 (Processed Bucket)                                         |
          |   "final_aggregate/"  (comprehensive, up-to-date dataset)                         |
          +------------------------------------------------------------------------------------+
                                                       ^
                                                       |
         +---------------------------------------------------------------------------------------------------+
         |                                         Downstream Consumers                                     |
         +---------------------------------------------------------------------------------------------------+
         | (8) Athena / BI Tools:                                                                             |
         |     - We define external tables on "final_aggregate/"                                             |
         |     - Analysts query with SQL via Athena or build dashboards in Tableau / QuickSight / etc.       |
         |                                                                                                   |
         | (9) Aggregation Service:                                                                           |
         |     - Periodic job reading from "streaming_index/" or "final_aggregate/"                          |
         |     - Summarizes data (e.g. merchant totals), pushes to Redis / DynamoDB                          |
         |     - Powers real-time dashboards for ops or risk teams                                          |
         |                                                                                                   |
         | (10) ML Pipeline (offline training):                                                              |
         |     - ml_training_pipeline.py loads "final_aggregate/"                                            |
         |     - creates advanced features, trains new fraud detection model                                 |
         |     - saves model artifacts to S3 or ML registry                                                  |
         +---------------------------------------------------------------------------------------------------+
```

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

- Real-time pipeline in EKS keeps partial indexes updated for immediate queries.  
- Batch pipeline on EMR finalizes heavy merges, generating consolidated indexes for finance, fraud, compliance, etc.

### **Deployment Steps**

**Terraform** apply:
   - VPC, subnets, MSK, EKS, EMR, S3, ECR.  
**Build** Docker images (streaming & batch) → push to ECR.  
**Create** K8s deployment for streaming job referencing `fraud_signals_topic`.  
**Upload** batch script to S3 → run EMR step nightly or on-demand.  
**Check** logs in CloudWatch or S3 for each job.(Monitoring and production support)


spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 \
  streaming_pipeline.py \
    --kafka_bootstrap_servers localhost:9092 \
    --kafka_topic transactions_topic \
    --output_path ./partial_streaming_index
