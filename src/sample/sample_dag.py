"""
Sample Airflow DAG demonstrating common patterns and features.

This DAG shows:
- Basic task dependencies
- PythonOperator usage
- BashOperator usage
- Task groups
- XCom for inter-task communication
- Dynamic task generation
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'sample_dag',
    default_args=default_args,
    description='A sample DAG demonstrating common Airflow patterns',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['sample', 'tutorial', 'example'],
)


# Python functions for PythonOperator tasks
def print_hello():
    """Simple hello world function"""
    print("Hello from Airflow!")
    return "Hello task completed"


def process_data(**context):
    """Demonstrate data processing and XCom"""
    execution_date = context['execution_date']
    print(f"Processing data for execution date: {execution_date}")
    
    # Simulate data processing
    data = {
        'records_processed': 1000,
        'execution_date': str(execution_date),
        'status': 'success'
    }
    
    print(f"Processed {data['records_processed']} records")
    
    # Push data to XCom for downstream tasks
    return data


def use_xcom_data(**context):
    """Demonstrate pulling data from XCom"""
    # Pull data from previous task
    task_instance = context['task_instance']
    data = task_instance.xcom_pull(task_ids='process_data')
    
    print(f"Retrieved data from XCom: {data}")
    print(f"Records processed: {data.get('records_processed', 0)}")
    
    return f"Successfully used data from previous task"


def generate_report(**context):
    """Generate a simple report"""
    execution_date = context['execution_date']
    
    report = f"""
    ========================================
    Daily Report - {execution_date.strftime('%Y-%m-%d')}
    ========================================
    DAG: sample_dag
    Status: Running
    Tasks: Multiple tasks executed successfully
    ========================================
    """
    
    print(report)
    return "Report generated"


# Task 1: Start task
start = DummyOperator(
    task_id='start',
    dag=dag,
)

# Task 2: Hello world task
hello_task = PythonOperator(
    task_id='hello_task',
    python_callable=print_hello,
    dag=dag,
)

# Task 3: Bash task
bash_task = BashOperator(
    task_id='bash_task',
    bash_command='echo "Running bash command in Airflow"',
    dag=dag,
)

# Task 4: Process data task
process_task = PythonOperator(
    task_id='process_data',
    python_callable=process_data,
    provide_context=True,
    dag=dag,
)

# Task 5: Use XCom data task
xcom_task = PythonOperator(
    task_id='use_xcom_data',
    python_callable=use_xcom_data,
    provide_context=True,
    dag=dag,
)

# Task Group: Parallel processing example
with TaskGroup('parallel_tasks', tooltip='Tasks that run in parallel', dag=dag) as parallel_group:
    
    # Create 3 parallel tasks
    for i in range(1, 4):
        PythonOperator(
            task_id=f'parallel_task_{i}',
            python_callable=lambda task_num=i: print(f"Parallel task {task_num} executing"),
            dag=dag,
        )

# Task 6: Generate report
report_task = PythonOperator(
    task_id='generate_report',
    python_callable=generate_report,
    provide_context=True,
    dag=dag,
)

# Task 7: End task
end = DummyOperator(
    task_id='end',
    dag=dag,
)

# Define task dependencies
# Linear flow: start -> hello -> bash -> process -> xcom
start >> hello_task >> bash_task >> process_task >> xcom_task

# Parallel tasks after xcom
xcom_task >> parallel_group

# Report task depends on parallel tasks completing
parallel_group >> report_task

# End task depends on report
report_task >> end

# Alternative dependency syntax (commented out):
# start.set_downstream(hello_task)
# hello_task.set_downstream(bash_task)
# bash_task.set_downstream(process_task)
# process_task.set_downstream(xcom_task)
# xcom_task.set_downstream(parallel_group)
# parallel_group.set_downstream(report_task)
# report_task.set_downstream(end)
