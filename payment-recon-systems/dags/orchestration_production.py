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
