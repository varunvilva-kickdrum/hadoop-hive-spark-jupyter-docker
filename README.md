# Big Data Environment

This is a Docker Compose environment for big data processing, including Hadoop, Spark, Hive, Jupyter Notebook, and Apache Airflow.

## Components

- **Hadoop**: Namenode + 2 Datanodes
- **Spark**: Master + 2 Workers (each with 6 cores, 6GB memory)
- **Hive**: HiveServer2 with PostgreSQL metastore
- **Jupyter**: Notebook with PySpark support
- **Airflow**: Complete workflow management with webserver, scheduler, worker, triggerer, and flower

## Prerequisites

- Docker
- Docker Compose
- At least 16GB RAM
- At least 14 CPU cores

## Directory Structure

```
bigdata-env/
├── hadoop/
├── spark/
├── hive/
│   ├── warehouse/
│   ├── pgdata/
│   └── conf/
│       └── hive-site.xml
├── jupyter/
│   ├── Dockerfile
│   └── kernel.json
├── airflow/
│   ├── dags/
│   ├── logs/
│   ├── plugins/
│   └── docker-compose-airflow.yml
├── datasets/
├── docker-compose.yml
├── hadoop.env
├── start-spark.sh
├── start-essential.sh
├── start-airflow.sh
└── README.md
```

## Starting the Environment

1. Make the startup scripts executable:
```bash
chmod +x start-spark.sh start-essential.sh start-airflow.sh
```

2. Start the full environment (includes Hadoop, Spark, Hive, and Jupyter):
```bash
./start-spark.sh
```

OR start only essential services:
```bash
./start-essential.sh
```

3. Start the Airflow services:
```bash
./start-airflow.sh
```

AND load the data from local machine to HDFS (Needs to be done when the data is updated as well)
```bash
./init-hdfs.sh
```

4. Wait for all services to start (this may take a few minutes):
```bash
docker-compose ps
```

## Container Startup Order
The startup scripts handle this automatically:
1. Namenode
2. Datanodes
3. Spark master
4. Spark workers
5. Postgres
6. Hive
7. Jupyter
8. Airflow

## Loading Datasets to HDFS
To ensure efficient data sharing between nodes, load your datasets to HDFS:
```bash
./init-hdfs.sh
```

## Accessing Services

- **Hadoop Namenode UI**: http://localhost:9870
- **Spark Master UI**: http://localhost:8080
- **Jupyter Notebook**: http://localhost:8888
- **HiveServer2**: http://localhost:10002
- **Airflow Webserver**: http://localhost:9090
- **Airflow Flower**: http://localhost:5555

## Using Jupyter Notebook

1. Access Jupyter at http://localhost:8888
2. The default password is 'jupyter'
3. Select the "PySpark" kernel for Spark jobs
4. Example code to test the environment:

```python
from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder \
    .appName("Test") \
    .config("spark.master", "spark://spark-master:7077") \
    .getOrCreate()

# IMPORTANT: Always use HDFS paths for optimal performance
# Read from HDFS (recommended for distributed processing)
df = spark.read.option("header", "true").option("inferSchema", "true").csv("hdfs://namenode:9000/data/cities.csv")
df.show()

# Alternative file formats
df_parquet = spark.read.parquet("hdfs://namenode:9000/data/sales_data.parquet")
df_orc = spark.read.orc("hdfs://namenode:9000/data/sales_data.orc")
df_json = spark.read.json("hdfs://namenode:9000/data/order_singleline.json")
```

## Important File Access Notes

1. **Do not use relative paths** like `datasets/cities.csv` as they will be incorrectly resolved to HDFS paths under `/user/jovyan/`

2. **Always use one of these path formats**:
   - HDFS path (recommended): `hdfs://namenode:9000/data/cities.csv`
   - Local filesystem: `file:///home/jovyan/datasets/cities.csv`
   - Worker path: `file:///opt/bitnami/spark/datasets/cities.csv`

## Using Hive

1. Connect to Hive using beeline:
```bash
docker exec -it hive beeline -u jdbc:hive2://localhost:10000
```

2. Example Hive commands:
```sql
CREATE TABLE test (id INT, value STRING);
INSERT INTO test VALUES (1, 'test'), (2, 'test2');
SELECT * FROM test;
```

## Using Apache Airflow

1. Access Airflow webserver at http://localhost9090
2. Default credentials: username: airflow, password: airflow
3. The Airflow instance is pre-configured with connections to:
   - Spark master at spark://spark-master:7077
   - HDFS at hdfs://namenode:9000
   - Hive at hive://hive:10000
4. DAGs are stored in the `airflow/dags` directory
5. An example DAG that integrates with Spark is provided

### Creating New DAGs

Create new DAG files in the `airflow/dags` directory. Example:

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Define DAG
dag = DAG(
    'my_spark_job',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False
)

# Define Spark job
spark_job = SparkSubmitOperator(
    task_id='run_spark_job',
    application='/opt/airflow/datasets/my_spark_job.py',
    conn_id='spark_default',
    dag=dag
)
```

### Stopping Airflow

```bash
docker-compose -f airflow/docker-compose-airflow.yml down
```

## Stopping the Environment

```bash
docker-compose down
```

To remove all data volumes:
```bash
docker-compose down -v
```

## Worker Resource Configuration
Each Spark worker is configured with:
- 6 CPU cores
- 6GB memory

This configuration provides good performance for most big data tasks. You can modify these values in the docker-compose.yml file if needed.

## Troubleshooting

1. If services fail to start, check logs:
```bash
docker-compose logs [service-name]
```

2. If Hive metastore fails to initialize:
```bash
docker exec -it hive schematool -dbType postgres -initSchema
```

3. If Spark workers fail to connect:
```bash
docker-compose restart spark-worker1 spark-worker2
```

4. If datasets are not visible in HDFS, check and reload them:
```bash
./init-hdfs.sh
```

## Non-Essential Files
These files are not critical for basic operation:
- `.git/` directory
- `.gitignore`
- Test files like `test_spark_dataset.py` and `spark_dataset_test.ipynb`

## Security Notes

- This setup is for development purposes only
- Default passwords are used for PostgreSQL and Jupyter
- HDFS permissions are disabled for easier development
- In production, enable security features and use proper authentication 