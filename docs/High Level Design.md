
**High-Level Implementation Plan for Payment Settlement, Reconciliation & Batch Index (AWS)**

**Business Requirements & S[data-sources](..%2Fdata-sources)cope ** 
- Handles daily reconciliation of large-scale financial transactions among external banks, card networks, and internal transaction systems.  
- Consolidates reconciled data into a “Ledger Reference Index” that finance and audit teams can query for compliance and reporting.  
- Guarantees correctness, timeliness, and strong data governance.

**Cloud Infrastructure** 
- Uses Amazon EMR (Spark on YARN) for distributed data processing and transformation.  
- Stores raw, intermediate, and final data in Amazon S3.  
- Maintains a unified schema and metadata using AWS Glue Data Catalog.  
- Runs a Managed Apache Airflow environment (Amazon MWAA) for orchestration and scheduling of batch workflows.  
- Persists reconciled results in Amazon Aurora PostgreSQL for the Ledger Reference Index.  
- Handles security with AWS KMS for encryption at rest, TLS for encryption in transit, and AWS IAM for role-based access control.  
- Archives historical snapshots to Amazon S3 Glacier for long-term retention.  
- Monitors and logs with Amazon CloudWatch and AWS CloudTrail for auditing and operational insights.

**Data Ingestion & Pre-Processing** 
- Automates file retrieval from external sources and internal systems to Amazon S3 landing buckets on a defined schedule.  
- Validates file integrity by comparing checksums and metadata logs in a manifest table tracked by AWS Glue.  
- Normalizes raw CSV or JSON files into a consistent Parquet structure in Amazon S3 using Spark jobs on Amazon EMR.  
- Handles currency conversion, time zone normalization, and date partitioning for improved query performance.  
- Mitigates data skew by partitioning data by date and bank ID, and using adaptive Spark configurations to balance load.

**Reconciliation Logic**  
- Runs a core Spark job on Amazon EMR that matches external bank transactions against internal system-of-record entries using transaction_id or other composite keys.  
- Applies fee and refund logic referenced from a business-rules table stored in Aurora PostgreSQL, enabling updates without code changes.  
- Manages late or corrected entries by storing interim reconciliation states in Amazon S3 and re-running only the affected partitions.  
- Logs mismatches, disputes, or partial matches in separate S3 output paths and marks them with unique mismatch flags in Aurora PostgreSQL for finance teams to review.

**Ledger Reference Index Generation ** 
- Inserts or upserts reconciled records into Aurora PostgreSQL under a defined schema, partitioned by settlement date.  
- Stores daily or hourly snapshots in separate database tables or schema versions to provide point-in-time references for audits.  
- Indexes critical columns such as transaction_id, mismatch_flag, merchant_id for faster query execution and minimal latency.  
- Implements Amazon Aurora multi-AZ for high availability, ensuring ledger queries are still served if a primary instance fails.

**Orchestration & Deployment** 
- Schedules the entire batch workflow in Amazon MWAA, creating DAGs that include ingestion, transformation, reconciliation, validation, and ledger update tasks.  
- Uses Jenkins pipelines for CI/CD, running Spark job tests and schema compatibility checks before promoting new code to MWAA.  
- Deploys changes to a staging EMR environment for performance testing with a subset of production-like data.  
- Rolls out to production through a blue/green approach: old DAG tasks remain operational until new ones are validated without errors.

**Observability & Operations**  
- Sends EMR Spark job metrics, YARN application logs, and MWAA DAG execution metrics to Amazon CloudWatch for real-time tracking.  
- Configures alerts for SLA breaches, high mismatch counts, or abnormal job runtimes.  
- Centralizes Spark executor logs in Amazon S3, with optional Amazon OpenSearch Service for deeper log analysis.  
- Documents a runbook detailing steps for partial reprocessing, late-arriving data merges, and ledger rollback in the event of incorrect settlements.

**ML Extensions** 
- Uses Amazon SageMaker to train anomaly detection models on historical mismatch data, fees, and volumes.  
- Integrates inference calls into the Spark pipeline or triggers separate workflows in MWAA to label suspicious transaction sets.  
- Stores anomaly flags in Aurora PostgreSQL for finance and risk teams to investigate.  
- Builds forecasting models to estimate future transaction volumes or potential mismatch frequencies, optimizing nightly cluster resource allocation.

**Key Engineering Challenges & Solutions**  
- Data Skew: Managed by date and bank-based partitioning, adaptive execution in Spark, and balanced partition assignments.  
- Late-Arriving or Partial Updates: Implemented versioned reconciliation runs and interim states on S3, ensuring flexibility in reprocessing.  
- SLA Enforcement: Achieved by scaling the EMR cluster based on forecasted volumes and setting CloudWatch alarms for early warnings.  
- Storage Costs: Mitigated by using Parquet for compression, S3 Lifecycle rules for archiving old snapshots to Glacier, and partition pruning.  
- Schema Evolution: Handled by AWS Glue Data Catalog versioning and structured migration scripts for Aurora PostgreSQL.  
- Performance Bottlenecks: Addressed by indexing in Aurora, optimizing Spark shuffle operations, and caching frequently referenced data.  
- Data Duplication & Consistency: Ensured by enforcing unique transaction IDs, thorough deduplication logic, and storing mismatch flags for disputes.  
- Security & Compliance: Ensured by IAM-based access to EMR, MWAA, Aurora, KMS encryption of data at rest, and TLS for data in transit.  
- Deployment Testing Complexity: Addressed by comprehensive staging environments mirroring production traffic, plus automated integration tests in Jenkins.  
- Highly Distributed Team Coordination: Streamlined by storing all pipeline definitions in version control, hosting architecture diagrams in Confluence, and tracking tasks in Jira.





**Data Architecture & Modeling Design**
---

### High-Level Data Flow

- **External Sources**  
  - Multiple banks/card networks provide daily (or more frequent) transaction extracts.  
  - Internal transaction systems produce in-house logs with varying schemas.  

- **Data Landing in S3**  
  - Raw files (CSV, JSON, or custom formats) land in Amazon S3 “landing zones” partitioned by source and date.  
  - A manifest or tracking table in AWS Glue verifies file completeness, size, and checksums.  

- **Staging & Curation**  
  - Spark jobs on Amazon EMR read raw files from S3 landing zones.  
  - Transform and normalize formats into consistent Parquet, applying any currency conversions and time zone standardizations.  
  - Write intermediate (“silver”) datasets back to S3, retaining partition columns (e.g., `bank_id`, `date`).  

- **Reconciliation & Indexing**  
  - A dedicated EMR Spark job combines external bank data with internal ledger data, matching on `transaction_id` or composite keys.  
  - Mismatches, disputes, and partial updates are flagged and written to separate reconciliation tables.  
  - Final “gold” data (fully reconciled transactions) is upserted into Aurora PostgreSQL (the “Ledger Reference Index”).  

- **Downstream Consumption**  
  - Finance and audit teams query Aurora for final settlement reports, aggregated summaries, and mismatch logs.  
  - Historical snapshots stored in S3 provide point-in-time references for compliance and investigation.  

---

### Key Modeling Components

1. **Raw Schema (Landing Layer)**  
   - Variable file formats (CSV, JSON).  
   - Minimal transformations—only structural metadata (filename, upload timestamp, source ID) is appended.  
   - Common fields (e.g., `transaction_id`, `amount`, `currency`) may be untyped or loosely typed here.

2. **Normalized Schema (Curated Layer)**  
   - Structured Parquet tables with consistent data types.  
   - Standard columns such as:  
     - `transaction_id`: Global unique identifier.  
     - `txn_timestamp`: Standardized UTC timestamp.  
     - `amount`: Decimal with precision to handle financial amounts.  
     - `currency_code`: ISO currency code.  
     - `bank_id`: Unique identifier for the source bank or processor.  
   - Additional metadata for transformations (e.g., `load_date`, `batch_id`).  

3. **Reconciled Schema (Gold Layer)**  
   - **Core Transaction Table** in Aurora:  
     - `transaction_id` (PK)  
     - `bank_id`  
     - `internal_ledger_id`  
     - `amount`, `fees`, `refund_amount`  
     - `status` (e.g., `MATCHED`, `DISPUTED`, `PARTIAL_MATCH`)  
     - `mismatch_flag` (boolean or enum indicating mismatch type)  
     - `reconcile_date` (the date/time of the reconciliation run)  
   - **Dispute & Mismatch Table** for exceptions:  
     - Tracks additional fields like mismatch reason, dispute resolution status.  
   - **Daily Snapshot Table** for historical audits:  
     - Suffix or partition key to differentiate daily runs (e.g., `settlement_date`).  

4. **Dimension & Reference Tables**  
   - **Bank Dimension**: Bank ID, bank name, region, currency preferences.  
   - **Fee Rules**: Lookup table for dynamic fee calculations.  
   - **Exchange Rates**: Daily or hourly rates for currency conversion.  

---

### Common Modeling & Architecture Challenges

Below are some of the main issues that arise in this architecture, which will guide further solution designs:

- **Partial/Delayed Data**  
  - External providers may deliver files late or multiple times (corrections).  
  - Requires versioned records and reprocessing logic to avoid double-counting or losing transactions.

- **Data Skew**  
  - Certain banks can contribute disproportionately large transaction volumes.  
  - Leads to uneven Spark partition distributions and potential performance bottlenecks.

- **Schema Evolution**  
  - New transaction attributes (e.g., `promotion_code`, `tax_details`) may appear over time.  
  - Requires robust schema versioning in AWS Glue Data Catalog and backward-compatible transformations.

- **Duplicate Transactions**  
  - A single transaction may appear in different files or from multiple sources.  
  - Necessitates deduplication logic keyed on `transaction_id` plus supplemental fields.

- **Complex Fee & Refund Structures**  
  - Different card networks and banks apply varying fee rates, partial refunds, or dispute adjustments.  
  - Requires dynamic lookups from reference tables to keep logic maintainable.

- **SLA Constraints**  
  - Must complete reconciliation by a strict cutoff (e.g., 2 AM daily).  
  - Demands well-tuned Spark jobs and auto-scaling EMR clusters to handle peak volumes quickly.

- **Data Quality & Governance**  
  - Financial data demands near-perfect accuracy, including correct currency conversions and balanced ledgers.  
  - Requires thorough validations and audit trails at each stage (landing, curated, gold).

- **Performance & Query Optimization**  
  - Large table scans and complex joins can slow analytics or data extraction from Aurora.  
  - Necessitates partitioning by date, indexing key columns, and possibly summarizing common aggregates.

- **Security & Compliance**  
  - Must protect sensitive financial info with encryption at rest (AWS KMS) and in transit (TLS).  
  - Requires fine-grained IAM roles for different teams (finance, audit, engineering) and comprehensive logging.

---

### Example Data Model Diagram (Conceptual)

```
           ┌─────────────┐         ┌─────────────────────┐
           │  Bank Dim   │         │  Fee Rules Dim      │
           └─────────────┘         └─────────────────────┘
                   |                        |
                   |                        |
┌────────────┐     ▼                        ▼
│ Transaction ├──────────> Reconciliation ──> Ledger Reference (Aurora)  
└────────────┘      (Spark on EMR)         (Daily Snapshots + Real-time Upserts)
                   |        
                   ▼        
          ┌────────────────┐
          │ Mismatch/Dispute│
          └────────────────┘
```

- **Transaction** table is generated by merging external (bank) and internal transaction logs into a normalized form.  
- **Bank Dim** and **Fee Rules** provide reference data for fee calculations.  
- **Reconciliation** uses Spark logic to match, label mismatches, and produce final records.  
- **Ledger Reference** in Aurora is the authoritative store with daily snapshot partitions and real-time upserts for any late corrections.

---

**Next Steps**  
1. **Implement Versioned Lake Schemas** in AWS Glue to manage partial data updates and backward-compatible transformations.  
2. **Refine Partitioning & Indexing** in Aurora to handle query performance at scale.  
3. **Build Automated Validation** for currency conversions, transaction amounts, and daily check sums.  
4. **Establish Runbooks** for partial reprocessing, handling late arrivals, and executing dispute workflows.  
5. **Incorporate ML** for anomaly detection and forecasting (via Amazon SageMaker) to further strengthen the overall data pipeline.





**High-Level Design Document for the Payment & Fraud Signals Data Platform**  

---

## 1. Introduction  
This high-level design addresses the business requirements set forth in the Payment Data & Fraud Signals Platform. It integrates **real-time** (streaming) and **batch** ingestion of payment data, along with fraud signals from machine learning systems. The goal is to create an **Aggregate Index** that downstream services—such as payment gateways, risk and compliance teams, and analytical platforms—can leverage for secure, near real-time insights.

---

## 2. System Architecture Overview

### 2.1 Architecture Components

1. **Real-Time Ingestion Pipeline**  
   - Consumes live payment transactions (from POS, web checkout, or microservices) through a streaming platform (e.g., Kafka or Kinesis).  
   - Incorporates immediate fraud signals returned by ML inference services (e.g., model APIs).  
   - Produces an in-memory or near-real-time “**Live Index**” that the payment gateway and risk engine can query.

2. **Batch Ingestion Pipeline**  
   - Processes large historical datasets or scheduled daily/weekly data dumps.  
   - Merges data with **Policy & Control Tables** (which change infrequently) and historical fraud labels or signals.  
   - Outputs a “**Batch Index**” stored in a data lake or database, which is periodically updated and optimized.

3. **Index Consolidation**  
   - Merges the **Live Index** and **Batch Index** into a single **Aggregate Index** (could be a unified data store or consolidated tables) that ensures consistency for downstream consumption.

4. **ML Feedback Loop**  
   - Feeds the curated transaction data (including outcomes: chargebacks, disputes) back to ML training pipelines.  
   - ML models generate new risk scores or fraud signals that re-enter the stream/batch pipelines for continuous enrichment.

5. **Downstream Consumers**  
   - **Payment Gateway**: Queries the Live Index for real-time decisions.  
   - **Risk & Compliance Teams**: Accesses the Aggregate Index for advanced queries, audits, and reports.  
   - **BI/Analytics Tools**: Generates dashboards, aggregates, and historical trends.  
   - **Customer Support**: Retrieves enriched transaction data to assist users with queries or disputes.

---

## 3. Data Architecture & Flows

### 3.1 Real-Time Flow

1. **Event Sources**  
   - Payment requests flow into Kafka or Kinesis topics.  
   - ML inference services produce immediate fraud signals for each transaction (e.g., score, reason_code).  

2. **Stream Processing**  
   - A streaming job (e.g., Flink or Spark Structured Streaming) reads from the payment topic.  
   - Joins real-time fraud scores (from a separate stream or via synchronous API calls).  
   - Enriches the payment event with relevant user/account attributes (e.g., pulled from a fast store or cache).  
   - Writes the enriched events to a **Live Index** (e.g., low-latency NoSQL, Elasticsearch, or a specialized indexing system).

3. **Live Index**  
   - Optimized for rapid lookups and sub-second queries.  
   - Keyed by `transaction_id` or a composite key (e.g., `(user_id, transaction_timestamp)`).  
   - May store partial or ephemeral data if final settlement details will come later (in the batch pipeline).

### 3.2 Batch Flow

1. **Data Lake / Landing Zone**  
   - Periodic dumps (CSV, Parquet, or JSON) from external payment partners or internal logs land in a data lake (e.g., Amazon S3, Azure Data Lake).  
   - Policy control data (e.g., region restrictions, advanced rule sets) also reside in a versioned table or separate folder.

2. **Batch ETL**  
   - A distributed compute engine (Spark on EMR or Databricks) reads raw files, performs cleansing, deduplication, and applies any standard transformations (e.g., currency normalization, time zone alignment).  
   - Fraud signals that arrive in batch form (e.g., historical model outputs, additional offline ML validations) are joined.  
   - Outputs the enriched, historical set of transactions to a **Batch Index** (e.g., partitioned Parquet tables, a data warehouse, or a highly scalable SQL/NoSQL store).

3. **Batch Index**  
   - Structured for large-scale analytics and incremental updates (e.g., daily or hourly merges).  
   - Contains comprehensive history, final settlement statuses, dispute outcomes, and policy flags.

### 3.3 Aggregate Index

- **Merge & Consolidation**  
  - A scheduled job or continuous pipeline merges the Live Index and the recently updated Batch Index.  
  - Transactions that were only partially enriched in real-time can be updated with final settlement details from the Batch Index.  
- **Unified Data Store**  
  - Could be a relational database (e.g., Postgres, MySQL, or Snowflake) or a NoSQL store (e.g., Cassandra, Elasticsearch) depending on query patterns.  
  - Provides a “single source of truth” for risk teams, audits, and compliance queries.

---

## 4. Data Modeling & Schema Definitions

### 4.1 Core Transaction Schema

| **Field**           | **Type**           | **Description**                                                    |
|---------------------|--------------------|--------------------------------------------------------------------|
| `transaction_id`    | String (UUID)      | Unique identifier for the transaction.                             |
| `user_id`           | String / BigInt    | Unique identifier of the user/customer.                            |
| `timestamp`         | Timestamp (UTC)    | Date/time of the transaction event.                                |
| `amount`            | Decimal(15, 2)     | Transaction amount in smallest currency unit.                      |
| `currency`          | String (ISO code)  | Currency type, e.g., USD, EUR.                                     |
| `fraud_score`       | Float / Decimal    | ML-generated risk or fraud score (0.0 - 1.0).                      |
| `fraud_reason_code` | String (enum)      | Short code indicating reason for suspicion (e.g., IP_MISMATCH).    |
| `policy_flag`       | String (enum)      | Indicates relevant policy (geo block, velocity limit, etc.).       |
| `transaction_status`| String            | Real-time status (e.g., `PENDING`, `AUTHORIZED`, `DECLINED`).      |
| `settlement_status` | String            | Final settlement state (e.g., `SETTLED`, `REFUNDED`, `CHARGEBACK`).|
| `updated_at`        | Timestamp (UTC)    | Last update timestamp (for merges/late arrivals).                  |

### 4.2 Policy & Control Schema

| **Field**         | **Type**  | **Description**                                                    |
|-------------------|-----------|--------------------------------------------------------------------|
| `policy_id`       | String    | Unique identifier for the policy.                                  |
| `policy_name`     | String    | Human-readable name of the policy.                                 |
| `policy_details`  | JSON      | Arbitrary JSON describing region blocks, velocity thresholds, etc. |
| `effective_date`  | Timestamp | When this policy becomes active.                                   |

### 4.3 Fraud Model Output Schema

| **Field**        | **Type**           | **Description**                                                 |
|------------------|--------------------|-----------------------------------------------------------------|
| `model_version`  | String            | Version identifier for the deployed ML model.                   |
| `transaction_id` | String            | Maps back to core transaction.                                  |
| `fraud_score`    | Float / Decimal    | Probability or risk score.                                      |
| `explanations`   | JSON              | Key-value pairs explaining why the model flagged certain risks. |
| `inference_time` | Timestamp (UTC)    | When the model produced this score.                             |

---

## 5. Deployment Models

### 5.1 Streaming Deployment

- **Infrastructure**  
  - A managed Kafka cluster or AWS Kinesis streams for ingest.  
  - A real-time processing engine (e.g., Apache Flink, Spark Streaming) deployed on Kubernetes or a managed service.  
  - A low-latency data store (Elasticsearch, DynamoDB, or Cassandra) serving as the **Live Index**.  

- **Key Considerations**  
  - Checkpointing & fault tolerance for stream processors.  
  - Sub-second ingestion to enrich and output.  
  - High-availability configurations (multi-AZ) to avoid data loss.

### 5.2 Batch Deployment

- **Infrastructure**  
  - Object storage (Amazon S3, Azure Data Lake) as a landing zone for daily or weekly files.  
  - A Spark cluster (EMR, Databricks, or on-prem) to run ETL jobs.  
  - A data warehouse or partitioned file store (Parquet/Delta) for the **Batch Index**.  

- **Key Considerations**  
  - Scheduling and SLA (must finish processing by a certain time).  
  - Data quality checks (deduplication, invalid records).  
  - Handling late-arriving or corrected transactions.

### 5.3 Aggregate Index Consolidation

- **Process**  
  - Periodic or on-demand job merges the streaming and batch data.  
  - Updates final transaction states (e.g., from `AUTHORIZED` to `SETTLED`).  
  - Replaces placeholders in the Live Index with accurate data from the Batch Index.  

- **Potential Approach**  
  - If using a relational store: an UPSERT (merge) strategy keyed by `transaction_id`.  
  - If using NoSQL: a near real-time “compaction” or partial update job.  

---

## 6. Alignment to Requirements

Below is how this high-level design meets the **functional** and **non-functional** requirements:

1. **(BR1) Real-Time Payment Ingestion**:  
   - The Streaming Pipeline ensures sub-second latencies for transaction ingestion and fraud enrichment.  

2. **(BR2) Batch Historical Processing**:  
   - The Batch Pipeline processes large volumes of historical data, merges policy data, and updates the Batch Index daily/weekly.  

3. **(BR3) Fraud Signal Integration**:  
   - Real-time scores are joined in the streaming pipeline; historical ML outputs (like offline analysis) join in the batch pipeline.  

4. **(BR4) Indexed Output for Risk & Compliance**:  
   - Both the Live Index (for immediate queries) and the Aggregate Index (for more complete data) are queryable by risk teams.  

5. **(BR5) Mixed Deployment Pattern**:  
   - Clearly segregated streaming pipeline (near real-time) and batch pipeline (scheduled) with consolidation logic.  

6. **(BR6) Downstream Delivery & Consumption**:  
   - Payment gateways, ML pipelines, and BI tools consume consistent data from the consolidated Aggregate Index.  

7. **(BR7) Monitoring & Alerting**:  
   - Centralized logging and metrics across streaming and batch jobs, with alerts for SLA breaches or unusual fraud spikes.

**Non-Functional Requirements**  
- **Performance**: Sub-second real-time path and scalable batch ETL.  
- **Scalability**: Auto-scaling for spikes in transaction volume.  
- **Reliability**: Stream checkpointing, multi-AZ deployment, robust job retry for batch.  
- **Security**: Encryption at rest/in transit, role-based access, compliance with PCI-DSS.  
- **Data Quality & Governance**: Schema enforcement, auditing, version-controlled policy data.  
- **Maintainability**: Modular pipeline design, CI/CD, infrastructure as code (e.g., Terraform).  
- **Monitoring & Observability**: Real-time dashboards for streaming, daily batch run logs, anomaly detection for fraud scores.

---
Architectural Design of the Payment reconciliation indexing systems
```
                   +---------------------+
                   | Flask APIs         |
                   | (transactions,     |
                   |  fraud_signals,    |
                   |  merchants,        |
                   |  policies)         |
                   +---------+-----------+
                             |
                             |  (1) Data requested via HTTP (e.g. GET /transactions?count=100)
                             v
                +--------------------------------+
                |  Ingest Code                   |
                |  (batch -> S3, stream -> Kafka)|
                +--------+------------------------+
                         |                       \
       (2) Writes JSON   |(3) Publishes events to \ (4) Data in Kafka topics
       or Parquet to S3  | Kafka topics            \
                         |                         v
     +--------------------v-----------------+      +---------------------------------+
     |        S3 (Raw Data)                |      |  AWS MSK (Kafka)                |
     | (batch exports, dimension tables)    |      | (transactions_topic,            |
     +--------------------------------------+      |  fraud_signals_topic, etc.)     |
                         ^                          +---------------------------------+
                         |                                   ^
                         |                                   |
                         |(5) Batch job reads from S3        |(6) Streaming job reads from Kafka
                         |     +-----------------------------+v+------------------------------+
                         |     |    Spark on EMR (batch_index.py)                            |
                         |     |  - Reads historical tx & policy from S3                     |
                         |     |  - Joins with merchant dim from S3                          |
                         |     |  - Writes "Batch Index" to S3                                |
                         |     +-------+------------------------------------------------------+
                         |             |
                         |(7) "Batch   |  
                         |    Index"   |  
                         v             |
               +-----------------------v----------------------+
               |       "Batch Index" in Parquet (S3)         |
               +---------------------------------------------+
                                                       
                                                        (8) Spark Structured Streaming Job
                                                        +-------------------------------------+
(9) Spark (streaming_index.py)                          |   Spark on EMR (streaming_index.py) |
- Reads real-time from Kafka (tx, fraud)                | - Joins transactions + fraud signals |
- Joins with merchant dimension from S3                 | - Broadcast merchant dimension from S3|
- Writes "Streaming Index" to S3                        | - Writes "Streaming Index" to S3      |
+--------------------+----------------------------------+--------------------------------------+
                     |
                     |(10) "Streaming Index" in Parquet (S3)
                     v
         +---------------------------------------------+
         |      "Streaming Index" in Parquet (S3)      |
         +---------------------------------------------+


                          +---------------------------------------------------+
(11) Final aggregator job |  Spark on EMR (final_aggregate.py)                |
- Reads "Batch Index" &   |  - Reads from S3 (batch_index + streaming_index)  |
  "Streaming Index" from S3| - Merges them (e.g. union or join)               |
- Writes "Final Aggregate" | - Writes "Final Aggregate" to S3                 |
  to S3 in Parquet         +----------------------+----------------------------+
                                                 |
                                                 |(12) "Final Aggregate Index"
                                                 v
                               +-------------------------------------------+
                               |      "Final Aggregate Index" in S3        |
                               |  (Comprehensive data for risk, analytics) |
                               +-------------------------------------------+
```

### Explanation of Each Step

1. **API Calls**: The ingest scripts (`ingest_to_s3.py` or `ingest_to_kafka.py`) request data from the Flask endpoints (transactions, policies, merchants, fraud signals).  
2. **Batch to S3**: The ingest script writes JSON or Parquet files into Amazon S3 for batch or historical storage.  
3. **Stream to Kafka**: The streaming ingest script (`ingest_to_kafka.py`) pushes events into Kafka topics (e.g., `transactions_topic`, `fraud_signals_topic`).  
4. **Kafka Topics**: MSK (Managed Streaming for Apache Kafka) retains event data for real-time/near real-time consumption.  
5. **Spark Batch Job**: `batch_index.py` runs on EMR (or similar), reading large files from S3 (transactions, policy data), plus merchant dimension from S3, then writes out the **Batch Index** to S3 in Parquet.  
6. **Spark Streaming Job**: `streaming_index.py` continuously consumes events from Kafka (both transactions and fraud signals), joins with the merchant dimension, then produces a **Streaming Index** (Parquet on S3).  
7. **Batch Index**: The batch pipeline output in S3.  
8. **Streaming Index**: The streaming pipeline output in S3.  
9. **Final Aggregate**: `final_aggregate.py` merges (union/join) the two indexes in S3 to produce a **Final Aggregate Index**, ensuring historical coverage plus the latest real-time data.  
10. **Final Aggregate Index**: A unified dataset in S3, used for analytics, ML training, reporting, or further ingestion into downstream systems.

