from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'spark_example_dag',
    default_args=default_args,
    description='An example DAG that uses Spark',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['example', 'spark'],
)

def print_context(**kwargs):
    print("Airflow and Spark integration example")
    return 'Context printed successfully'

start_task = PythonOperator(
    task_id='start_task',
    python_callable=print_context,
    dag=dag,
)

# Use BashOperator instead of SparkSubmitOperator
spark_job = BashOperator(
    task_id='spark_job',
    bash_command='echo "Running a simulated Spark job"',
    dag=dag,
)

start_task >> spark_job 