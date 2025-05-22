# Airflow Setup Guide

## Overview

This directory contains the Apache Airflow setup for the Big Data environment. Airflow is used to orchestrate data workflows, create ETL pipelines, and manage job scheduling and monitoring.

## Directory Structure

```
airflow/
│
├── dags/                # Put your DAG files here
│   ├── spark_example_dag.py  # Example DAG
│
├── logs/                # Airflow logs (mounted volume)
├── plugins/             # Airflow plugins
├── airflow/             # Internal directory created by Docker volume mapping
│
├── docker-compose-official.yml  # Official Airflow Docker Compose setup
├── docker-compose-airflow.yml   # Alternative Airflow Docker Compose
├── docker-compose-nginx.yml     # Nginx configuration for Airflow
└── nginx.conf                   # Nginx configuration file
```

## Starting and Stopping Airflow

To start Airflow:

```bash
cd /path/to/bigdata-env/airflow
docker-compose -f docker-compose-official.yml up -d
```

To stop Airflow:

```bash
cd /path/to/bigdata-env/airflow
docker-compose -f docker-compose-official.yml down
```

## Accessing the UI

- Airflow Web UI: http://localhost:9090
- Username: airflow
- Password: airflow

## Creating DAGs

1. Create your Python DAG files in the `airflow/dags/` directory
2. Follow the pattern in the example DAG file `spark_example_dag.py`
3. The DAG files will be automatically detected by Airflow

Example DAG structure:

```python
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
    'my_dag_id',
    default_args=default_args,
    description='Description of your DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['example', 'tag2'],
)

task1 = BashOperator(
    task_id='task1',
    bash_command='echo "Hello World"',
    dag=dag,
)

task1
```

## Configuration Files

### docker-compose-official.yml

This is the main Docker Compose file for the Airflow services. It defines:

- Airflow Webserver (UI)
- Airflow Scheduler
- Airflow Worker
- Airflow Triggerer
- PostgreSQL (metadata database)
- Redis (message broker)
- Flower (Celery monitoring)
- Nginx (web proxy)

Key configuration options:

- `AIRFLOW__CORE__LOAD_EXAMPLES: 'false'` - Disables example DAGs
- Volumes for dags, logs, and plugins are mounted from the host
- Network configuration to connect with other Big Data services

## Using Spark with Airflow

### Option 1: Using BashOperator (Simple)

Use the BashOperator to execute Spark commands:

```python
spark_task = BashOperator(
    task_id='spark_task',
    bash_command='echo "Running a Spark job"',
    dag=dag,
)
```

### Option 2: Using SparkSubmitOperator (Advanced)

To use SparkSubmitOperator, you need to ensure the Apache Spark provider package is installed. There are two ways to do this:

1. **Using Environment Variables in docker-compose-official.yml**:
   ```yaml
   environment:
     ADDITIONAL_PYTHON_MODULES: "apache-airflow-providers-apache-spark apache-airflow-providers-apache-hdfs apache-airflow-providers-apache-hive"
   ```

2. **Using a Custom Dockerfile**:
   A Dockerfile.custom is provided in this directory that contains the necessary packages. To use it:
   
   - Update docker-compose-official.yml to use this custom image:
     ```yaml
     build:
       context: .
       dockerfile: Dockerfile.custom
     ```
   - Rebuild and restart Airflow:
     ```bash
     docker-compose -f docker-compose-official.yml down
     docker-compose -f docker-compose-official.yml build
     docker-compose -f docker-compose-official.yml up -d
     ```

Example SparkSubmitOperator usage:

```python
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

spark_job = SparkSubmitOperator(
    task_id='spark_job',
    application='path/to/spark_job.py',
    conn_id='spark_default',
    application_args=['arg1', 'arg2'],
    conf={
        'spark.master': 'spark://spark-master:7077',
        'spark.driver.memory': '1g',
        'spark.executor.memory': '1g',
        'spark.executor.cores': '1',
        'spark.submit.deployMode': 'client'
    },
    verbose=True,
    dag=dag,
)
```

## Troubleshooting

### Issue: ModuleNotFoundError for airflow.providers.apache packages

**Solution 1**: Use BashOperator instead of SparkSubmitOperator
**Solution 2**: Install required packages by modifying the docker-compose file to include:
```yaml
environment:
  _PIP_ADDITIONAL_REQUIREMENTS: 'apache-airflow-providers-apache-spark==4.11.0 apache-airflow-providers-apache-hdfs==4.5.1 apache-airflow-providers-apache-hive==8.2.0'
```

### Issue: Permission errors when installing packages

**Solution**: Use a custom Dockerfile (Dockerfile.custom) with proper permissions. Note that due to security restrictions in Airflow Docker images, you may encounter permission issues with some dependencies like Kerberos.

### Issue: DAGs not appearing in web UI

**Solution**: Check these common causes:
1. Ensure your DAG file doesn't have syntax errors
2. Verify that the file has the right permissions
3. Check Airflow logs to see if there are import errors
4. Restart the scheduler: `docker restart airflow-scheduler-2`

## Why are there two airflow directories?

The nested directory structure:
- The outer `airflow/` directory contains your configuration files for the project
- The inner `airflow/airflow/` directory is created by Docker's volume mounting mechanism
- You should always place your DAGs in the outer `airflow/dags/` directory 