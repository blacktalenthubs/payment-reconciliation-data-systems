from datetime import datetime
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

default_args = {
    "owner": "student",
    "start_date": datetime(2025, 1, 1),
    "retries": 0
}

dag = DAG(
    "local_pipeline_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False
)

# Task 1: Synthetic data ingestion
ingestion_task = KubernetesPodOperator(
    task_id="synthetic_data_ingestion",
    namespace="default",
    name="synthetic-data-ingestion-pod",
    image="localhost:5000/batch-jobs:local",
    cmds=["python", "end_to_end_data_setup.py"],
    image_pull_policy="IfNotPresent",
    get_logs=True,
    dag=dag
)

# Task 2: Batch Reconciliation
batch_recon_task = KubernetesPodOperator(
    task_id="batch_reconciliation",
    namespace="default",
    name="batch-reconciliation-pod",
    image="localhost:5000/batch-jobs:local",
    cmds=["python", "batch.py"],
    image_pull_policy="IfNotPresent",
    get_logs=True,
    dag=dag
)

# Task 3: Final Index
final_index_task = KubernetesPodOperator(
    task_id="final_index",
    namespace="default",
    name="final-index-pod",
    image="localhost:5000/batch-jobs:local",
    cmds=["python", "final_index_pipeline.py"],
    image_pull_policy="IfNotPresent",
    get_logs=True,
    dag=dag
)

ingestion_task >> batch_recon_task >> final_index_task
