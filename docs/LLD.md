**High-Level Design Document: Payment & Fraud Signals Data Platform (Spark Structured Streaming + MSK)**

**Introduction**  
This document outlines a Payment & Fraud Signals Data Platform that ingests real-time payment events through Apache Kafka (AWS MSK) using Spark Structured Streaming, and processes batch data in AWS. The goal is to enrich transactions with fraud signals, consolidate outputs, and serve an Aggregate Index for downstream consumers such as payment gateways, risk teams, and analytics tools.

---

**System Architecture Overview**  
- A **real-time pipeline** handles live payment events from MSK, uses Spark Structured Streaming for enrichment, and writes a “Live Index” to a low-latency store (for example, DynamoDB or Elasticsearch).  
- A **batch pipeline** runs on Amazon EMR, processes larger historical or scheduled datasets from Amazon S3, merges them with additional policy or ML data, and produces a “Batch Index” also stored in AWS (for instance, partitioned Parquet on S3).  
- A **consolidation job** merges the Live Index and Batch Index to form an Aggregate Index, ensuring that near real-time transactions eventually align with final settlement and policy details.

---

**Data Architecture & Flows**  
- **Real-Time Flow**  
  - Payment events are published to AWS MSK (Managed Streaming for Apache Kafka).  
  - Spark Structured Streaming (deployed on EMR or as a standalone cluster) consumes from Kafka topics.  
  - Fraud signals may arrive synchronously from an ML inference API or asynchronously from another Kafka topic.  
  - The streaming job enriches transactions with fraud scores, reason codes, and partial policy checks.  
  - Enriched records are written to a Live Index (for fast lookups by the payment gateway or risk tools).

- **Batch Flow**  
  - Historical or scheduled data drops land in Amazon S3 (e.g., daily transaction logs).  
  - Apache Spark on EMR reads these files, performs deduplication, currency normalization, and joins offline ML outputs or slowly changing policy data.  
  - The result is stored in a Batch Index (for example, Parquet files in S3, partitioned by date or region).  

- **Index Consolidation**  
  - A scheduled Spark job merges the latest Live Index records with the finalized Batch Index records.  
  - Transactions that were partially enriched in real time are updated with definitive settlement details, refunds, or chargeback statuses.  
  - The consolidated dataset becomes the Aggregate Index, consumed by risk teams, compliance, and analytics platforms.

---

**Data Modeling & Schemas**  

Core Transaction Schema

| Field                | Type                    | Description                                                      |
|----------------------|-------------------------|------------------------------------------------------------------|
| **transaction_id**   | String (UUID)          | Unique identifier for the transaction                           |
| **user_id**          | String or BigInt        | Unique identifier for the payer/customer                         |
| **timestamp**        | Timestamp (UTC)         | Time of the transaction event                                    |
| **amount**           | Decimal(15,2)           | Transaction amount in smallest currency unit                     |
| **currency**         | String (ISO code)       | Currency type (e.g. USD, EUR)                                    |
| **fraud_score**      | Float or Decimal        | ML-derived likelihood of fraud                                   |
| **fraud_reason_code**| String (enum)           | Reason code for flagged or suspicious transactions              |
| **policy_flag**      | String (enum)           | Indicates policy rules triggered (velocity limit, geo-block, etc.)|
| **transaction_status**| String                 | Status such as AUTHORIZED, DECLINED, or PENDING                 |
| **settlement_status**| String                 | Final settlement outcome like SETTLED, REFUNDED, or CHARGEBACK  |
| **updated_at**       | Timestamp (UTC)         | Timestamp of last update (for partial or late arrivals)         |

Policy & Control Schema

| Field          | Type           | Description                                                       |
|----------------|----------------|-------------------------------------------------------------------|
| **policy_id**  | String         | Unique identifier for the policy                                 |
| **policy_name**| String         | Human-readable name for the policy                               |
| **policy_data**| JSON           | Contains region blocks, velocity thresholds, or other rule sets  |
| **effective_date** | Timestamp (UTC) | Start date/time for policy to be active                   |

Fraud Model Output Schema

| Field          | Type                    | Description                                                      |
|----------------|-------------------------|------------------------------------------------------------------|
| **model_version**    | String           | Identifier for the deployed ML model version                     |
| **transaction_id**   | String           | Foreign key linking to core transaction_id                       |
| **fraud_score**      | Float or Decimal  | Probability or score from ML                                     |
| **explanations**     | JSON             | Key-value details about why the transaction is flagged           |
| **inference_time**   | Timestamp (UTC)   | Time when the ML model produced this inference                   |

---

**Deployment and Operations**  
- **Real-Time Pipeline**  
  - AWS MSK holds incoming payment events.  
  - Spark Structured Streaming jobs run on EMR containers or a dedicated Spark cluster, consuming from Kafka.  
  - Enriched data is stored in a low-latency index such as DynamoDB or Amazon OpenSearch Service for immediate reads.

- **Batch Pipeline**  
  - Scheduled ingestions or daily snapshots land in S3.  
  - Spark on EMR processes these snapshots, applying transformations, quality checks, and merges with historical policy data or offline ML signals.  
  - The Batch Index is written to S3 in Parquet format, partitioned by relevant keys (e.g., date, region).

- **Consolidation & Serving**  
  - A Spark job merges Live and Batch data, ensuring final statuses are updated in the consolidated Aggregate Index.  
  - Risk teams and analytics tools query this index for deeper investigation, compliance audits, or performance reporting.  
  - Monitoring leverages Amazon CloudWatch for Spark job metrics, MSK consumer lag, and error logs, while Amazon SNS or PagerDuty handles alerts for SLA breaches or abnormal spikes in fraud scores.

---

**Conclusion**  
This design leverages Spark Structured Streaming with AWS MSK for real-time ingestion and AWS EMR with S3 for batch processing. By unifying enriched real-time data (Live Index) and comprehensive historical data (Batch Index) into a final Aggregate Index, the platform provides a scalable, low-latency, and highly reliable solution for managing payment transactions and fraud signals. Downstream consumers can access near real-time risk insights, finalize settlement details, and drive continuous improvements in fraud detection models.