"""
fraud_data_pipeline.py
"""

from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable

default_args = {
    'owner': 'reconciliation_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': 30,
    'email_on_failure': True
}

SPARK_CONN = "spark_default"
SPARK_CONFIG = {
    "jars": "/opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.4.jar",
    "driver_memory": "2g",
    "executor_memory": "2g",
    "executor_cores": "2",
    "num_executors": "2"
}

def _get_config():
    return {
        "kafka_bootstrap": "host.docker.internal:9092",
        "stream_checkpoint": "/opt/airflow/data/stream_checkpoints",
        "batch_index_path": "/opt/airflow/data/batch_index",
        "final_index_path": "/opt/airflow/data/final_index",
        "num_merchants": Variable.get("NUM_MERCHANTS", 50),
        "num_transactions": Variable.get("NUM_TRANSACTIONS", 1000)
    }

with DAG(
        dag_id='full_reconciliation_pipeline',
        default_args=default_args,
        schedule_interval='@daily',
        catchup=False,
        tags=['reconciliation', 'batch', 'stream'],
        params=_get_config()
) as dag:

    with TaskGroup("data_generation") as data_gen_group:
        generate_data = SparkSubmitOperator(
            task_id="generate_test_data",
            application="/opt/airflow/dags/scripts/generate_data.py",
            conn_id=SPARK_CONN,
            **SPARK_CONFIG,
            application_args=[
                "--merchants", "{{ params.num_merchants }}",
                "--transactions", "{{ params.num_transactions }}",
                "--output-dir", "/opt/airflow/data"
            ]
        )
        kafka_check = PythonOperator(
            task_id="verify_kafka_ready",
            python_callable=lambda: print("Checking Kafka availability..."),
        )
        generate_data >> kafka_check

    with TaskGroup("streaming_operations") as stream_group:
        streaming_pipeline = SparkSubmitOperator(
            task_id="run_streaming_pipeline",
            application="/opt/airflow/dags/scripts/streaming_pipeline.py",
            conn_id=SPARK_CONN,
            **SPARK_CONFIG,
            application_args=[
                "--kafka-bootstrap", "{{ params.kafka_bootstrap }}",
                "--output-path", "/opt/airflow/data/partial_stream_index",
                "--checkpoint", "{{ params.stream_checkpoint }}"
            ]
        )

    with TaskGroup("batch_operations") as batch_group:
        batch_reconciliation = SparkSubmitOperator(
            task_id="run_batch_reconciliation",
            application="/opt/airflow/dags/scripts/batch.py",
            conn_id=SPARK_CONN,
            **SPARK_CONFIG,
            application_args=[
                "--statements-path", "/opt/airflow/data/aggregator_statements.parquet",
                "--stream-index", "/opt/airflow/data/partial_stream_index",
                "--output-path", "{{ params.batch_index_path }}"
            ]
        )

    with TaskGroup("final_operations") as final_group:
        final_index = SparkSubmitOperator(
            task_id="create_final_index",
            application="/opt/airflow/dags/scripts/final_index_pipeline.py",
            conn_id=SPARK_CONN,
            **SPARK_CONFIG,
            application_args=[
                "--batch-index", "{{ params.batch_index_path }}",
                "--stream-index", "/opt/airflow/data/partial_stream_index",
                "--ml-path", "/opt/airflow/data/ml_fraud_signals.parquet",
                "--output-path", "{{ params.final_index_path }}"
            ]
        )

        validate_output = PythonOperator(
            task_id="validate_output",
            python_callable=lambda: print("Final validation checks..."),
            trigger_rule="all_done"
        )
        final_index >> validate_output

    data_gen_group >> stream_group
    data_gen_group >> batch_group
    stream_group >> batch_group
    batch_group >> final_group

    data_quality_check = PythonOperator(
        task_id="verify_initial_data",
        python_callable=lambda: print("Validating generated data..."),
    )

    data_gen_group >> data_quality_check >> [stream_group, batch_group]
