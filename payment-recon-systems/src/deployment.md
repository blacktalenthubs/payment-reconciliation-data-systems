
1. **Dockerize** each part of the code (Streaming, Batch, Final Index).  
2. **Push** those Docker images to **AWS ECR**.  
3. **Deploy** the streaming pipeline on **Kubernetes** using a manifest.  
4. **Use EMR** for the batch pipeline, triggered/orchestrated by **Airflow**.  
5. **Continuously deploy** the streaming code (and potentially other services) using **Spinnaker** (or a similar CD pipeline).  

We'll include **example Dockerfiles**, a **K8S manifest**, an **Airflow DAG** snippet, and a **conceptual Spinnaker pipeline** approach. The code is simplified but captures the **key patterns** you’d see in a production environment.

---

# 1. **Dockerization**

We have **three** main Python scripts from the previous sections:

1. `streaming_pipeline.py` – Runs **continuously**, reading from Kafka and writing partial streaming index.  
2. `batch_reconciliation.py` – Runs **on-demand** or scheduled, merging aggregator statements with the streaming index.  
3. `final_index_pipeline.py` – Another batch job that merges the batch index with ML signals or partial streaming data to produce the final index.

Below, we’ll show **two** example Dockerfiles:

- **Dockerfile for the Streaming Pipeline** (runs on K8S).  
- **Dockerfile for the Batch Jobs** (the same container can be used to run `batch_reconciliation.py` and `final_index_pipeline.py` on EMR or as a K8S job).

You can choose to have separate Dockerfiles for each pipeline if that better fits your architecture, but often the batch jobs share many dependencies, so they can be in the same image.

---

## 1.1 **Dockerfile: Streaming Pipeline**

**File: `Dockerfile.streaming`**

```dockerfile
# Dockerfile for streaming_pipeline.py
FROM python:3.9-slim

# Install OS dependencies (if needed)
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements_streaming.txt /app/requirements_streaming.txt

WORKDIR /app

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements_streaming.txt

# Copy streaming code
COPY ../dags/scripts/streaming_pipeline.py /app/streaming_pipeline.py

# For Spark Structured Streaming we can:
# 1) either install 'pyspark' in the container, or
# 2) run Spark in cluster mode (e.g., Spark on K8S) with a separate approach.

# We'll do a simple approach:
RUN pip install pyspark==3.3.1  # or version matching your cluster

# Expose no port needed for Spark driver if running client mode, but typically
# not essential if we handle driver/exec within K8S
CMD ["python", "/app/streaming_pipeline.py"]
```

### **requirements_streaming.txt** (example)

```
kafka-python==2.0.2
pyarrow==10.0.1
pyspark==3.3.1
```

> In reality, your streaming job might run **Spark on Kubernetes** with separate images for the driver and executors. Or you might run **Spark on EMR**. The above Dockerfile is a **simplistic** version if you want a container that can run `streaming_pipeline.py` directly on a K8S cluster.

---

## 1.2 **Dockerfile: Batch Jobs**

**File: `Dockerfile.batch`**

```dockerfile
# Dockerfile for batch.py & final_index_pipeline.py
FROM python:3.9-slim

# Basic OS dependencies
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements_batch.txt /app/requirements_batch.txt
WORKDIR /app

RUN pip install --no-cache-dir -r requirements_batch.txt

# Copy batch codes
COPY ../dags/scripts/batch.py /app/batch_reconciliation.py
COPY final_index_pipeline.py /app/final_index_pipeline.py

# Similarly, if you need Spark for local testing (EMR cluster might handle it differently):
RUN pip install pyspark==3.3.1

CMD ["python", "/app/batch_reconciliation.py"]
# Or override the command when you run the container to run final_index_pipeline.py
```

### **requirements_batch.txt** (example)

```
pyarrow==10.0.1
boto3==1.26.0
pyspark==3.3.1
# Possibly other libraries for AWS CLI if needed
```

---

## 1.3 **Build & Push to ECR**

1. **Authenticate** Docker to your AWS ECR repository:
   ```bash
   aws ecr get-login-password --region us-east-1 | \
   docker login --username AWS --password-stdin <your_account_id>.dkr.ecr.us-east-1.amazonaws.com
   ```
2. **Build** the streaming image:
   ```bash
   docker build -t streaming-pipeline:latest -f Dockerfile.streaming .
   ```
3. **Tag** and **push**:
   ```bash
   docker tag streaming-pipeline:latest <your_account_id>.dkr.ecr.us-east-1.amazonaws.com/streaming-pipeline:latest
   docker push <your_account_id>.dkr.ecr.us-east-1.amazonaws.com/streaming-pipeline:latest
   ```
4. **Repeat** for the batch image:
   ```bash
   docker build -t batch-jobs:latest -f Dockerfile.batch .
   docker tag batch-jobs:latest <your_account_id>.dkr.ecr.us-east-1.amazonaws.com/batch-jobs:latest
   docker push <your_account_id>.dkr.ecr.us-east-1.amazonaws.com/batch-jobs:latest
   ```

Now your images are in ECR, ready for deployment on K8S, EMR, or anywhere else.

---

# 2. **Kubernetes Manifests**

We want to **continuously run** the streaming pipeline in K8S. Typically, you have **two** ways to run Spark on K8S:

1. **Spark-on-Kubernetes** approach (Spark driver + executors orchestrated by the Spark Operator or custom approach).  
2. **A standalone container** that runs the code with embedded Spark, not truly distributed. This is simpler for a small use case.

Below is a simple **K8S Deployment** for the streaming pipeline container. We assume it’s a modest approach, not a full Spark cluster distribution.

---

## 2.1 **`streaming-pipeline-deployment.yaml`**

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: streaming-pipeline-deployment
  labels:
    app: streaming-pipeline
spec:
  replicas: 1
  selector:
    matchLabels:
      app: streaming-pipeline
  template:
    metadata:
      labels:
        app: streaming-pipeline
    spec:
      containers:
      - name: streaming-pipeline-container
        image: <your_account_id>.dkr.ecr.us-east-1.amazonaws.com/streaming-pipeline:latest
        imagePullPolicy: Always
        # Environment variables for Kafka config if needed
        env:
        - name: KAFKA_BOOTSTRAP_SERVERS
          value: "kafka1:9092"
        - name: KAFKA_TOPIC
          value: "transactions_topic"
        # (Optional) resource limits
        resources:
          requests:
            cpu: "250m"
            memory: "512Mi"
          limits:
            cpu: "500m"
            memory: "1024Mi"
        # (Optional) volume mounts if you store partial data on a PersistentVolume.
      imagePullSecrets:
      - name: ecr-registry-credentials
```

**Apply** it:

```bash
kubectl apply -f streaming-pipeline-deployment.yaml
```

This will schedule a **single** pod that runs `streaming_pipeline.py` continuously. If the pod crashes, it’ll be restarted by K8S. If you need **scalable** Spark streaming, you’d use **Spark’s native Kubernetes integration** or the **Spark Operator** to manage drivers/executors.

---

# 3. **Batch with EMR**

The user specifically mentioned they want to use **EMR** for the batch jobs. Typically, you’d package your Spark job in a `.py` or `.jar` and submit it to an **EMR** cluster via steps or a transient cluster. The **Docker** approach can be used for local testing or for a container-based Spark on EKS—but let’s show how to do it with EMR steps in production:

## 3.1 **Submitting to EMR**

1. **Upload** your Python scripts (`batch_reconciliation.py`, `final_index_pipeline.py`) to S3, or bundle them in an S3 ZIP file.  
2. From Airflow (or CLI), you can run:

   ```bash
   aws emr add-steps \
     --cluster-id j-XXXXXXXX \
     --steps Type=Spark,Name="BatchReconciliation",ActionOnFailure=CONTINUE,\
Args=[--deploy-mode,cluster,--master,yarn,s3://my-bucket/code/batch_reconciliation.py,\
--some-arg,haha] \
     ...
   ```

   The above approach means you rely on EMR’s environment to have the dependencies. Alternatively, you can define a “bootstrap action” or “virtual env” for your dependencies.

> In many real setups, you might dynamically spin up a *transient* EMR cluster, run the job, then tear it down. Or you might run on a *long-running* EMR cluster.

---

# 4. **Airflow Orchestration**

We can orchestrate the batch runs using Airflow. Suppose we have an **Airflow DAG** that triggers:

1. **Data Ingestion** (which might or might not be relevant if data is continuously streaming)  
2. **Run the Batch Reconciliation** on EMR  
3. **Run the Final Index** job on EMR or the same cluster.  

Below is a simplified DAG using **`EmrAddStepsOperator`** from `airflow.providers.amazon.aws.operators.emr_add_steps` to run each job. We assume the scripts are on S3.

**File: `airflow_dag_batch.py`**

```python
from datetime import datetime
from airflow import DAG
from airflow.providers.amazon.aws.operators.emr_add_steps import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr_step import EmrStepSensor
from airflow.providers.amazon.aws.operators.emr_containers import EmrContainerOperator
# or whichever approach you prefer for EMR

default_args = {
    "owner": "data-team",
    "start_date": datetime(2025, 1, 1),
    "retries": 1
}

with DAG(
    dag_id="batch_reconciliation_dag",
    default_args=default_args,
    schedule_interval="@daily",  # or cron
    catchup=False
) as dag:

    # Example: Add steps to an existing EMR cluster
    BATCH_STEPS = [
        {
            'Name': 'BatchReconciliationStep',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'spark-submit',
                    '--deploy-mode', 'cluster',
                    '--master', 'yarn',
                    's3://my-bucket/code/batch_reconciliation.py'
                ]
            }
        },
        {
            'Name': 'FinalIndexStep',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'spark-submit',
                    '--deploy-mode', 'cluster',
                    '--master', 'yarn',
                    's3://my-bucket/code/final_index_pipeline.py'
                ]
            }
        }
    ]

    add_batch_steps = EmrAddStepsOperator(
        task_id="add_batch_steps",
        job_flow_id="j-XXXXXXXXX",  # your EMR cluster id
        steps=BATCH_STEPS
    )

    # We can sense the final step for completion
    wait_for_final_step = EmrStepSensor(
        task_id="wait_for_final_step",
        job_flow_id="j-XXXXXXXXX",
        step_id="{{ task_instance.xcom_pull(task_ids='add_batch_steps', key='return_value')[-1] }}",  # last step
        timeout=3600,
        poke_interval=60
    )

    add_batch_steps >> wait_for_final_step
```

**How It Works**:  
- The DAG runs daily.  
- The first operator adds two Spark steps to an existing EMR cluster: one for `batch_reconciliation.py`, another for `final_index_pipeline.py`.  
- The second operator waits for the final step to complete.  
- You can chain additional tasks or notifications after `wait_for_final_step`.

---

# 5. **Continuous Deployment of the Streaming Code (Spinnaker)**

Finally, you asked: “How do we **deploy and continuously run** the streaming code?” This is where a **Continuous Delivery** tool like **Spinnaker** (or Jenkins, Argo CD, GitLab CI/CD, etc.) can help.

### **Typical Flow**:

1. **Developer** pushes new code to Git.  
2. **CI Pipeline** (e.g., Jenkins or GitHub Actions) **builds** the Docker image (`Dockerfile.streaming`), **runs tests**, and **pushes** the image to ECR.  
3. **Spinnaker** detects the **new image** in ECR (trigger).  
4. **Spinnaker** updates the **K8S Deployment** to the **new** image tag (e.g., `streaming-pipeline:build-123`).  
5. **Kubernetes** performs a rolling update, spinning up new pods and shutting down old pods gracefully.

In Spinnaker, you would create a **pipeline** that:

- **Monitors** the ECR repository for new tags.  
- **Deploys** to your K8S cluster (using a manifest like `streaming-pipeline-deployment.yaml`, but referencing the new image).  
- Optionally runs automated canary analysis or other checks before finalizing the rollout.

A minimal Spinnaker pipeline might look like:

1. **Trigger**: “New Docker Image” in ECR.  
2. **Bake/Find Image** stage (Spinnaker artifact).  
3. **Deploy Manifest** stage: Updates the `Deployment` in your K8S cluster with the new image.  
4. **Manual Judgment** (optional) if you want to verify the new streaming pipeline in staging before production.  
5. **Complete**.  

This allows your streaming pipeline to be **always running** in your K8S cluster, automatically updated whenever a developer merges changes that pass CI tests.

---

# 6. **Putting It All Together**

1. **Data Setup**: You have a script that creates aggregator statements, policy data, etc. and pushes transactions to Kafka.  
2. **Streaming Pipeline**: Deployed on K8S via a **Deployment** that runs indefinitely, reading from Kafka and writing partial streaming index (Parquet) to your chosen storage (S3 or local for demo).  
3. **Batch Pipeline**: 
   - Orchestrated by **Airflow** (or Step Functions).  
   - Submits Spark jobs on **EMR** (reading aggregator statements + partial streaming index).  
   - Produces a “Batch Index.”  
4. **Final Index**: Another Spark job run on EMR (or a separate job in Airflow) merges ML signals, policy data, or partial streaming data to produce a final “golden” dataset.  
5. **Spinnaker** (or other CD) ensures your streaming pipeline code is **continuously deployed** to K8S.  

This architecture is **modular**:
- The **streaming** side is containerized on K8S, continuously pulling data from Kafka.  
- The **batch** side is ephemeral on EMR, orchestrated by Airflow.  
- The final merges happen in the same or an additional Spark job.  
- All code can be updated automatically if you have a well-defined CI/CD pipeline (e.g. Jenkins + Spinnaker).

---

### **Conclusion**

This end-to-end approach covers:

- **Dockerization** of each pipeline.  
- **ECR** push for storing images.  
- **Kubernetes** manifests to run the streaming pipeline continuously.  
- **EMR + Airflow** for batch orchestration.  
- **Spinnaker** for automated rolling updates of your streaming pipeline container in K8S.


Below is an **example Airflow DAG** that orchestrates:

1. **Synthetic Data Generation** (the script that creates merchants, aggregator statements, policies, pushes transactions to Kafka, and posts fraud signals to an API).  
2. **Batch Reconciliation** (the script that merges partial streaming data and aggregator statements).  
3. **Final Index** pipeline (the script that merges the Batch Index with ML signals or partial data).

All are assumed to run on **EKS** using **KubernetesPodOperator** (i.e., ephemeral Pods). This approach does **not** use EMR; everything is containerized and orchestrated in a single Kubernetes cluster. The **streaming** pipeline (the code that continuously runs in the cluster) is **not** part of the DAG, because it’s a **long-running** Deployment. Instead, the DAG focuses on the **one-time or scheduled** tasks: data generation, batch job, and final index job.

Below is a **minimal** code sample. In a real environment, you would:

- Store your images in AWS ECR (or another registry).  
- Supply the correct **credentials** to Airflow so it can pull from ECR.  
- Possibly mount volumes, set environment variables, or pass secrets.

---

## **Airflow DAG File**: `orchestration_dag.py`

```python
"""
orchestration_dag.py

An example Airflow DAG (running on EKS) that orchestrates:
1) Synthetic Data Ingestion (container runs data setup script).
2) Batch Reconciliation (container runs batch.py).
3) Final Index (container runs final_index_pipeline.py).

Prerequisites:
- Airflow is itself deployed on EKS (e.g., using Helm or another method).
- KubernetesPodOperator is configured to create pods in the same or a connected namespace.
- The container images for these tasks are stored in ECR (or another registry).

Author: Your Name
Date: 2025-01-19
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
# If you're on older versions, you might see different import paths (e.g., from airflow.contrib.operators...).

default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    # "start_date": datetime(2025, 1, 1),  # or your desired start_date
}

dag = DAG(
    dag_id="payment_reconciliation_dag",
    description="Orchestrates synthetic data ingestion, batch reconciliation, and final index creation on EKS",
    default_args=default_args,
    schedule_interval=None,  # or a cron expression, e.g. "0 2 * * *"
    catchup=False
)

# -----------------------------------------------------------------------------
# 1) Synthetic Data Ingestion Task
# -----------------------------------------------------------------------------

ingestion_task = KubernetesPodOperator(
    task_id="synthetic_data_ingestion",
    name="synthetic-data-ingestion-pod",
    namespace="default",  # adjust as needed
    image="<your_aws_account_id>.dkr.ecr.us-east-1.amazonaws.com/synthetic-data:latest",  # your data generation image
    cmds=["python", "end_to_end_data_setup.py"],
    # or if your image's entrypoint automatically runs the script, you can omit cmds
    image_pull_policy="Always",
    get_logs=True,
    dag=dag
)

# -----------------------------------------------------------------------------
# 2) Batch Reconciliation Task
# -----------------------------------------------------------------------------

batch_recon_task = KubernetesPodOperator(
    task_id="batch_reconciliation",
    name="batch-reconciliation-pod",
    namespace="default",
    image="<your_aws_account_id>.dkr.ecr.us-east-1.amazonaws.com/batch-jobs:latest",  
    cmds=["python", "batch.py"],
    image_pull_policy="Always",
    get_logs=True,
    dag=dag
)

# -----------------------------------------------------------------------------
# 3) Final Index Task
# -----------------------------------------------------------------------------

final_index_task = KubernetesPodOperator(
    task_id="final_index",
    name="final-index-pod",
    namespace="default",
    image="<your_aws_account_id>.dkr.ecr.us-east-1.amazonaws.com/batch-jobs:latest",
    cmds=["python", "final_index_pipeline.py"],
    image_pull_policy="Always",
    get_logs=True,
    dag=dag
)

# Define dependencies in a simple linear flow:
ingestion_task >> batch_recon_task >> final_index_task
```

---

### **Explanation**

1. **DAG Definition**  
   - We create a DAG named `"payment_reconciliation_dag"`, configured **not** to schedule automatically (`schedule_interval=None`) for manual runs or triggered runs.  
   - Adjust as needed if you want a daily or hourly schedule.

2. **KubernetesPodOperator**  
   - Each task runs as an ephemeral **Pod** on the same EKS cluster that hosts Airflow.  
   - We reference images in ECR, e.g. `"<your_aws_account_id>.dkr.ecr.us-east-1.amazonaws.com/batch-jobs:latest"`.  
   - We assume each image already has the script we need (like `batch_reconciliation.py`).  
   - `cmds=["python", "script.py"]` executes the Python entrypoint within the container.

3. **Tasks**:
   - **`synthetic_data_ingestion`**: Runs the script that creates aggregator statements, merchants, policy data, pushes transactions to Kafka, and posts ML signals.  
   - **`batch_reconciliation`**: After synthetic data is created, we run the reconciliation job, which merges the partial streaming index with aggregator statements.  
   - **`final_index`**: Produces a final index after the batch index is ready.

4. **Dependencies**:  
   - The DAG ensures the ingestion task completes before launching batch reconciliation, and the final index job waits for the batch job to finish.

---

## **Where is the Streaming Pipeline?**

- The **streaming pipeline** (`streaming_pipeline.py`) is **not** ephemeral; it runs **continuously**. Thus, we typically **deploy** it as a **Kubernetes Deployment** (via a manifest) or a **StatefulSet**.  
- That is **not** orchestrated by Airflow (since Airflow tasks are designed to start and stop).  
- Once deployed, the streaming pipeline keeps writing partial index data.  
- The **batch pipeline** (run by Airflow) then reads that partial data whenever it’s triggered.

For example, you might have already done:

```bash
kubectl apply -f streaming-pipeline-deployment.yaml
```

So your streaming code runs indefinitely. The DAG is only for discrete tasks: data generation, batch, final merges, etc.

---

## **Continuous Deployment of These Jobs**

- If you want to **continuously deploy** updated images (for ingestion/batch/final code) to EKS, you can connect your **CI/CD** system (e.g., Jenkins, GitHub Actions, or Spinnaker) to **redeploy** the updated images.  
- Airflow is just **running** them as ephemeral Pods; if the image tag changes (and you use `image_pull_policy="Always"`), a new run will pull the latest.  
- The streaming pipeline is a **long-lived** Deployment, so updating it typically involves applying a new Deployment manifest referencing the new image tag, or using a tool like Spinnaker that triggers a rolling update in K8S.

---

## **Summary**

- **DAG**: A straightforward **three-task** pipeline in Airflow.  
- **Synthetic Data** -> **Batch Reconciliation** -> **Final Index**.  
- **KubernetesPodOperator**: Each task spins up a container from ECR.  
- **Streaming** job is **long-running** in the cluster, not part of Airflow.  
- This architecture keeps streaming separate from batch orchestration, while letting Airflow handle all the one-shot or scheduled jobs on **EKS**.

