Below is an **end-to-end plan** for deploying the data pipeline to AWS, integrating with **Athena**, **Airflow** (for orchestration), and **Docker** (for containerizing code). This plan demonstrates how to take the existing Spark-based streaming/batch pipelines, run them in a production-like environment, and expose the curated data to business stakeholders via Athena queries.

---

# 1) AWS Architecture Overview

1. **S3 Buckets**  
   - **Raw Bucket**: Receives data from ingestion scripts or real-time producers (for batch).  
   - **Processed Bucket**: Stores output from the Spark pipelines (e.g., Streaming Index, Batch Index, Final Aggregate).  
2. **Kafka / MSK (Optional)**  
   - If streaming is required, run an Amazon MSK cluster or a local Kafka cluster on EC2/ECS.  
   - Spark Structured Streaming jobs consume from MSK.  
3. **EMR or EKS for Spark**  
   - **Option A**: Amazon EMR to run Spark jobs (batch or streaming).  
   - **Option B**: Spark on Amazon EKS (Kubernetes), using custom Docker images.  
4. **AWS Glue Data Catalog**  
   - Stores table definitions for Athena.  
   - Allows schema-on-read queries on Parquet in S3.  
5. **Amazon Athena**  
   - Lets business analysts query the curated S3 data (e.g., final_aggregate) using standard SQL.  
   - We create external tables referencing the partitioned Parquet output.  
6. **Apache Airflow on AWS (MWAA or self-managed)**  
   - Orchestrates pipeline steps for batch jobs (e.g., daily triggers).  
   - Schedules ingestion, triggers Spark steps on EMR, updates or repairs Athena partitions, etc.

---

# 2) Dockerization

We containerize our code so we can deploy it either to EMR with a custom bootstrap or to EKS (Spark on Kubernetes), or simply for local dev. Below is a **simplified Dockerfile**:

```dockerfile
# Dockerfile

FROM amazoncorretto:11  # or python:3.9 if we only need Python
LABEL maintainer="you@example.com"

# Install necessary packages
RUN yum install -y python3 python3-pip && \
    pip3 install --upgrade pip

# Copy local code
WORKDIR /app
COPY requirements.txt .
RUN pip3 install -r requirements.txt

COPY . /app

# (Optional) If you want to do a local testing entrypoint:
CMD ["python3", "generate_initial_data.py"]
```

1. **Build**:
   ```bash
   docker build -t payment-fraud-demo:latest .
   ```
2. **Run**:
   ```bash
   docker run -it payment-fraud-demo:latest
   ```
In a real scenario, you might push this image to **Amazon ECR** for use in EMR on EKS or ECS tasks.

---

# 3) Airflow Orchestration

We can define two DAGs in Airflow: one for **batch** pipelines, one for **stream** pipelines (or a single DAG with multiple tasks). Here’s a conceptual approach:

```python
# dag_batch_pipeline.py
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {"owner": "data_eng", "start_date": datetime(2023, 1, 1)}

with DAG(
    dag_id="batch_pipeline",
    default_args=default_args,
    schedule_interval="@daily",  # e.g. daily
    catchup=False
) as dag:

    # Step 1: Ingestion (optional, if we fetch from external or run ingestion script)
    ingest_data = BashOperator(
        task_id="ingest_data",
        bash_command="python /app/phase1_baseline/ingest_to_s3.py --endpoint ... --count 100"
    )

    # Step 2: Spark job to run batch_index
    run_batch_index = BashOperator(
        task_id="run_batch_index",
        # Could call spark-submit on EMR or EKS
        bash_command="""
        spark-submit \
         --master yarn \
         --deploy-mode cluster \
         s3://my-code-bucket/phase1_baseline/batch_index.py
        """
    )

    # Step 3: Spark job to finalize aggregates
    run_final_aggregate = BashOperator(
        task_id="run_final_aggregate",
        bash_command="""
        spark-submit \
         --master yarn \
         --deploy-mode cluster \
         s3://my-code-bucket/phase1_baseline/final_aggregate.py
        """
    )

    # Step 4: (Optional) Athena partition refresh
    refresh_partitions = BashOperator(
        task_id="athena_refresh",
        bash_command="""
        aws athena start-query-execution \
          --query-string "MSCK REPAIR TABLE my_batch_index_table" \
          --region us-east-1 \
          --output text
        """
    )

    ingest_data >> run_batch_index >> run_final_aggregate >> refresh_partitions
```

Similarly, a **streaming** DAG might orchestrate the provisioning of a long-running Spark streaming job on EMR or EKS, but often streaming jobs are just started once and run continuously, so Airflow might only **deploy** or **redeploy** them occasionally.

---

# 4) Setting Up Athena Tables

Once data is in S3 (Parquet format), we define Athena tables referencing them. For example, if we have a `final_aggregate` dataset at `s3://my-processed-bucket/final_index/`, we can do:

```sql
CREATE DATABASE IF NOT EXISTS payment_fraud;

CREATE EXTERNAL TABLE IF NOT EXISTS payment_fraud.final_aggregate(
  transaction_id   string,
  user_id          string,
  user_name        string,
  merchant_id      string,
  merchant_name    string,
  amount           double,
  currency         string,
  status           string,
  policy_id        string,
  policy_name      string,
  fraud_score      double,
  reason_code      string,
  trans_date       date
)
PARTITIONED BY (status string)    -- if we partitioned by status
STORED AS PARQUET
LOCATION 's3://my-processed-bucket/final_index/'
TBLPROPERTIES ("parquet.compress"="SNAPPY");
```

After creation, we can run:
```sql
MSCK REPAIR TABLE payment_fraud.final_aggregate;
```
to pick up all existing partitions. Alternatively:
```sql
ALTER TABLE payment_fraud.final_aggregate
ADD PARTITION (status='AUTHORIZED') LOCATION 's3://.../status=AUTHORIZED';
```
for each partition if we want more manual control.

**Business Value**: Analysts can now run Athena queries like:
```sql
SELECT merchant_name, SUM(amount) as total_sales
FROM payment_fraud.final_aggregate
WHERE status='AUTHORIZED'
GROUP BY merchant_name
ORDER BY total_sales DESC;
```
all without provisioning a database server.  

---

# 5) EMR vs. EKS for Deployment

## Option A: EMR
- **Cluster**: Create an EMR cluster with a matching IAM role that grants access to S3, Athena, etc.  
- **Bootstrap**: Provide the Spark scripts (batch_index.py, streaming_index.py) either via S3 or a custom EMR step.  
- **Airflow Steps**: Use `aws emr add-steps` or the official EMR Airflow operators to run each Spark job on the cluster.

## Option B: EKS (Spark on Kubernetes)
- **Docker**: Build/push the container to ECR.  
- **Spark**: Use Spark’s native K8s mode:  
  ```bash
  spark-submit \
    --master k8s://https://<eks-endpoint> \
    --conf spark.kubernetes.container.image=<account>.dkr.ecr.us-east-1.amazonaws.com/payment-fraud-demo:latest \
    ...
  ```
- **Airflow**: Use a custom K8sPodOperator or similar to orchestrate each job.

---

# 6) Security & IAM

1. **S3 Bucket Policies**  
   - Restrict read/write from Spark roles, Airflow roles, or ingestion scripts.  
2. **IAM Roles**  
   - EMR or EKS role with permission to read raw S3 paths and write processed data.  
   - Airflow MWAA execution role to call `athena start-query-execution`, `emr add-steps`, etc.  
3. **Encryption**  
   - Optionally enable S3 server-side encryption (SSE-S3 or SSE-KMS).  
   - Use SSL for Kafka/MSK if streaming is sensitive data.

---

# 7) Final Deployment Flow

1. **Infrastructure as Code**  
   - Use Terraform or AWS CloudFormation to provision S3 buckets, EMR cluster, MSK (if streaming), IAM roles, Athena database, and an Airflow environment (or Amazon MWAA).  
2. **Docker Build & Push**  
   - Build the container containing your code.  
   - Push to ECR.  
3. **Airflow Setup**  
   - Deploy your DAG files (`dag_batch_pipeline.py`, etc.) to MWAA or a self-managed Airflow cluster.  
   - Provide credentials or assume-role for reading from S3, writing logs, etc.  
4. **Run**  
   - The DAG triggers ingestion, calls `spark-submit` on EMR or EKS to run `batch_index.py`, then `final_aggregate.py`, then an Athena repair partitions command.  
5. **Validation**  
   - In Athena, confirm that the new partitions appear and queries return expected data.

---

# Conclusion

With this **deployment plan**:

- You **Dockerize** the code to maintain consistent environments.  
- You **orchestrate** the pipelines with Airflow tasks (for batch).  
- You **expose** curated data in S3 to **Athena**, letting business users run SQL analytics on the final indexes.  
- You can **choose** EMR or EKS for Spark runs, depending on your org’s preference.  
- **Security** is handled via IAM, restricted S3 buckets, and optional encryption.

This approach provides a **comprehensive** solution for teaching advanced data engineering students how to go from a local Spark pipeline to a full-blown AWS-based production environment, including best practices in containerization, orchestration, data lake queries, and cloud security.