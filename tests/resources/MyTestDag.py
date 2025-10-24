from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'fabric-api',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'MyTestDag',
    default_args=default_args,
    description='A simple test DAG',
    catchup=False,
)

# Simple task
hello_task = BashOperator(
    task_id='hello_world',
    bash_command='echo "Hello World from Fabric API!"',
    dag=dag,
)

# Example of a more complex task
print_date_task = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag,
)

# Set task dependencies
hello_task >> print_date_task