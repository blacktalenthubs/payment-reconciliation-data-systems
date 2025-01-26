# payment-reconciliation-data-systems
## This project cover key advanced concepts in data engineering interviews and real world use cases .
 **Data & Schema Details** (Upstream, Internally Generated, Enriched Outputs)
 **Detailed Architecture** (covering streaming and batch pipelines)
 **UDF Requirements**  
 **Infrastructure & Deployment Plans** (EMR, Docker/EKS, MSK, IaC)  
  **Orchestration** (Streaming with Spinnaker or similar CI/CD tooling, Batch with Airflow)
 

---

# **Introduction & Context**

The **Payment Reconciliation Index System** is designed to handle:

- **High-volume payment transactions** streaming in from multiple sources (POS terminals, payment gateways).  
- **ML-generated fraud signals** providing real-time and historical risk scores.  
- **Internally generated policy data** (KYC/AML rules, blacklists, compliance flags).  

The system produces:

- **Two core indexes**:  
  1. **Streaming Index** for real-time fraud/risk visibility.  
  2. **Batch Index** for daily or periodic reconciliation.  

- **Final Aggregate Index**: A unified dataset merging both the streaming and batch indexes into a single source of truth for **Finance**, **Compliance**, **Risk**, and **Audit** teams.

---

# **Data & Schema Requirements**

## **Upstream Schemas**

### A. **Payment Transactions**  
- **transaction_id** (string/UUID) – Unique identifier for each payment.  
- **merchant_id** (string) – ID referencing merchant or partner.  
- **user_id** (string) – End customer or account identifier.  
- **amount** (decimal) – Payment amount.  
- **currency** (string) – Currency code (USD, GBP, etc.).  
- **status** (string) – Possible values: `PENDING`, `APPROVED`, `DECLINED`, `REFUNDED`.  
- **timestamp** (datetime) – Event time from the payment gateway.  

### B. **Bank/Aggregator Statements**  
- **statement_id** (string) – External reference for the statement.  
- **transaction_id** (string) – Cross-reference to Payment Transactions.  
- **posting_date** (date) – When the transaction was officially posted.  
- **amount_posted** (decimal) – Final amount recorded by the bank.  
- **fee_charged** (decimal) – Any fees or commissions deducted.  

### C. **ML Fraud Signals**  
- **fraud_signal_id** (string/UUID) – Unique identifier for ML inference records.  
- **transaction_id** (string) – Join key to Payment Transactions.  
- **fraud_score** (float) – ML-based risk score (0–1).  
- **model_version** (string) – Which model generated the score.  
- **inference_time** (datetime) – Timestamp of fraud detection.  

## **Internally Generated Data**

### A. **Policy/Compliance Reference**  
- **policy_id** (string) – Unique ID for each policy rule set.  
- **policy_type** (string) – E.g., `KYC`, `AML`, `REGIONAL_LIMIT`.  
- **rule_description** (string) – Explanation of the policy.  
- **active_flag** (boolean) – Indicates if rule is currently enforced.  

### B. **Merchant Profiles**  
- **merchant_id** (string) – Join key to Payment Transactions.  
- **merchant_name** (string) – Descriptive name for front-end.  
- **risk_level** (string) – E.g., `LOW`, `MEDIUM`, `HIGH`.  
- **onboarding_date** (date) – Merchant onboarding date for compliance.  

## **Output Schemas: Indexes**

### A. **Streaming Index**  
- **transaction_id**  
- **status** (latest known from streaming)  
- **fraud_score** (from ML)  
- **enriched_flags** (policy-based flags, e.g., `KYC_REQUIRED`, `AML_PENDING`)  
- **stream_ingest_time** (timestamp)  

### B. **Batch Index**  
- **transaction_id**  
- **reconciled_status** (e.g., `MATCHED`, `UNMATCHED`, `PARTIAL`)  
- **amount_posted** (from statements)  
- **discrepancy** (calculated difference between posted and expected)  
- **batch_run_date**  

### C. **Final Aggregate Index** (merged from Streaming + Batch)  
- **transaction_id**  
- **merchant_id**  
- **status** (latest from either streaming or batch)  
- **fraud_score**  
- **discrepancy**  
- **policy_flags**  
- **reconciliation_timestamp** (when final merge occurred)

---

# **Data Systems Architecture**

The **Data Systems Architecture** covers ingestion, enrichment, indexing, and final consumption layers.

```
         +------------------------------------------------------------------------------------+
         |               Diagram 1: Payment Reconciliation Data Flow (High-Level)             |
         +------------------------------------------------------------------------------------+

   Upstream Sources                     Real-Time (Streams)           Batch (Scheduled)             Final Output
   ==================                  ====================           ==================             ============

   1) Payment Gateway            +----------------------------+                                  
      transactions  ------------>|  Kafka/MSK: transactions  |------------+                      
                                  +----------------------------+            \                     
   2) Bank/Aggregator                                                     \  (Spark Streaming)
      statements  --------------> (loaded daily in S3)                    v                     
                                                                     +------------------------+      
   3) ML Fraud Signals          +----------------------------------->   EKS Spark Streaming  |---->  (Partial "Streaming Index")
      (inference service)       +------------------------------------->    (Docker)          |---->  S3
                                                                     +------------------------+
                                                                             |                            
   4) Policy Data                                                               
      (internal DB or S3)     <---- Accessed for rule-based checks in Streaming & Batch            
   
                                                     +-------------------------------------------+
                                                     |       Airflow (Batch Orchestration)       |
                                                     |        - Schedules Spark on EMR           |
                                                     +------------------------+--------------------+
                                                                               |
                                                                               v
                                                               +-------------------------------+
                                                               |      EMR Spark (Batch)       |
                                                               |  - merges aggregator data     |
                                                               |  - reconciles partial indexes |
                                                               |  - produces "Batch Index"     |
                                                               +-------------------------------+
                                                                               |
                                                                               v
                                                                   +------------------------+
                                                                   | Final Aggregation Job |
                                                                   | - merges streaming &   |
                                                                   |   batch indexes        |
                                                                   +----------+-------------+
                                                                              |
                                                                              v
                                                        +------------------------------------------------+
                                                        |               Final "Aggregate Index"           |
                                                        |     S3 partitioned data (daily, monthly)       |
                                                        +------------------------------------------------+
                                                                              |
                                                          +-----------------------------------+
                                                          |  BI / Analytics / Compliance Tools |
                                                          +-----------------------------------+
```

### **Key Steps**:

**Transaction Ingestion**  
   - Real-time: Payment events flow into **MSK (Kafka)**.  
   - Periodic/Batch: Statements arrive in S3 from external bank aggregators.

**Streaming Pipeline** (Spark on EKS)  
   - Consumes Kafka topics (transactions, fraud signals).  
   - Enriches with policy data (via broadcast variables or direct DB lookup).  
   - Writes out partial **Streaming Index** to S3.

**Batch Pipeline** (Spark on EMR)  
   - Airflow triggers daily jobs.  
   - Reads aggregator statements, merges with partial Streaming Index, compares amounts, flags discrepancies.  
   - Outputs the **Batch Index**.

**Final Aggregation**  
   - Single job merges the Streaming Index and Batch Index.  
   - Produces a comprehensive **Final Aggregate Index** for downstream BI, compliance, and risk analysis.

---

#  **UDF Requirements**

Various **User-Defined Functions** are required for in-stream and in-batch transformations:

**`udf_calc_discrepancy(amount, amount_posted)`**  
   - Returns the difference between expected transaction amount and posted amount from external statements.

**`udf_flag_compliance(fraud_score, policy_flags)`**  
   - Applies business logic to combine ML fraud score and policy-based flags into a single risk assessment output.

**`udf_mask_sensitive(user_id)`**  
   - Masks or tokenizes user identifiers to comply with **PCI-DSS** or **PII** requirements.

**`udf_derive_status(cur_status, bank_status)`**  
   - Resolves final transaction status by blending real-time updates with aggregator statuses in batch.

**`udf_kpi_calculation(merchant_id, daily_index)`**  
   - Summarizes or aggregates daily metrics for merchant dashboards (e.g., total volume, refunds, chargebacks).

---

# **Infrastructure & Deployment Architecture**

The second diagram focuses on how these services are provisioned, their interactions, and the **DevOps** approach:

```
         +-----------------------------------------------------------------------------------+
         |               Diagram 2: Infrastructure Deployment (AWS & Containerization)       |
         +-----------------------------------------------------------------------------------+

                                           GitLab/Jenkins/Spinnaker
                                                     |
                                                     |  (CI/CD)
      +----------------------------------------+      v       +--------------------------------+
      |          IaC (Terraform)              |  +----------->+ Docker Images in ECR           |
      |  - VPC, Subnets, Security Groups      |  |            +--------------------------------+
      |  - MSK (Kafka), EKS, EMR, S3 Buckets  |  |
      +----------------+----------------------+  |
                         |                      |
                         | (applies)            |
                         v                      |
      +------------------------------------+    |
      |        AWS Infrastructure          |<---+
      |------------------------------------|
      |  1) MSK (Kafka Topics)            |
      |     - "transactions_topic"        |
      |     - "fraud_signals_topic"       |
      |  2) EKS Cluster                   |
      |     - Spark Streaming pods        |
      |     - Docker containers from ECR  |
      |  3) EMR Cluster                   |
      |     - On-demand or scheduled      |
      |  4) S3 Buckets (Data Lake)        |
      |     - raw/processed/final indexes |
      +------------------------------------+
                  |          ^
                  |(reads/writes)
                  v          |
      +--------------------------+
      | Airflow (Batch Jobs)     |  (Deployed on ECS, or EKS, or separate EC2)
      | - triggers Spark on EMR  |
      +--------------------------+

                         +-----------------------------------------------+
                         |  Monitoring & Alerting (CloudWatch, Datadog) |
                         +-----------------------------------------------+
                                     | (logs, metrics, events)
                                     v
                                 On-Call Engineers
```

### **Key Infrastructure Components**

**AWS MSK (Managed Kafka)**  
   - Holds streaming topics for transactions (`transactions_topic`) and fraud signals (`fraud_signals_topic`).  
   - **Provisioned** by Terraform with appropriate replication factors and ACLs.

**EKS (Elastic Kubernetes Service)**  
   - Runs Spark Streaming jobs as **Docker** containers.  
   - Uses **ECR** for container images with PySpark dependencies.

**EMR** (Elastic MapReduce)  
   - Scalable cluster for batch processing.  
   - Airflow triggers nightly or on-demand Spark steps.  
   - Reads/writes to S3 for raw, partial, and final indexes.

**S3**  
   - **Data Lake** buckets: raw (unprocessed input), curated (intermediate partial indexes), final (aggregated indexes).

**Spinnaker** (or another CI/CD tool)  
   - Automates build, test, and deployment pipelines for the Docker images used by EKS streaming services.

**Airflow**  
   - Schedules, coordinates, and monitors **batch** pipelines on EMR.  
   - DAGs define job dependencies (e.g., “Wait for aggregator data -> run reconciliation Spark job -> produce Batch Index”).

**Monitoring & Alerting**  
   - **CloudWatch** for logs, metrics, and alarms.  
   - Datadog or similar tools for deeper application performance monitoring (latency, throughput, error rates).

---

# **Deployment & Production Plan**

## *Infrastructure Automation**

- **Terraform** scripts define:  
  - **VPC** with private subnets for MSK and EKS worker nodes.  
  - **MSK cluster** with appropriate topics.  
  - **EKS cluster** and node groups.  
  - **EMR** cluster configuration, S3 buckets, and IAM roles.

## **Dockerization of Services**

- **Spark Streaming** app is packaged into a Docker image with:  
  - Python (PySpark, dependencies)  
  - Kafka connectors (e.g., `spark-sql-kafka-0-10_2.12`)  
  - UDF modules (in the same container or separate library jar/whl)

- Image stored in **ECR**; pulled by EKS for streaming pods.

## **Streaming Pipeline Deployment**

**Code Merge** in Git triggers **CI/CD** pipeline in Jenkins/GitLab.  
**Spinnaker** (or similar) builds Docker image, runs unit/integration tests.  
On success, the new image is tagged and pushed to **ECR**.  
**Kubernetes Deployment** in EKS references the updated image, rolling out changes to Spark streaming pods.

## **Batch Pipeline Deployment**

- **Airflow** DAG definitions stored in Git.  
- Automated pipeline updates the DAG code on the Airflow server.  
- **EMR** job steps are triggered based on the updated DAG schedule (nightly, hourly, etc.).

## **Production Support**

- **CloudWatch** monitors streaming job throughput (records/sec), batch job execution times, EMR cluster performance.  
- On critical errors (e.g., pipeline downtime), automated alerts notify on-call engineers via Slack/Email/SMS.

---

# **Orchestration & Scheduling**

## **Streaming Orchestration** (Spinnaker or GitLab CI/CD)  
- **Continuous** operation once deployed; any code changes or scale-up triggers a new rollout.  
- **Auto-scaling** triggers based on CPU/memory usage in EKS (via Kubernetes Horizontal Pod Autoscaler).

## **Batch Orchestration** (Airflow)  
- **DAG** :
  1. **Check aggregator data** in S3.  
  2. **Start EMR Cluster** (if needed) or run step on existing cluster.  
  3. **Run Spark job** to unify partial indexes, aggregator statements, produce **Batch Index**.  
  4. **Final Aggregation** job merges streaming & batch indexes.  
  5. **Publish** or notify downstream systems once the final dataset is ready.  

---
![Recon Design.drawio.svg](Recon%20Design.drawio.svg)