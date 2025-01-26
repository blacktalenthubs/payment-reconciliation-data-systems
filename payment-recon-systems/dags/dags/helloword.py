from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="hello_world",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
) as dag:
    task = BashOperator(
        task_id="print_date",
        bash_command="date",
    )