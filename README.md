# Big Data Environment

This is a Docker Compose environment for big data processing, including Hadoop, Spark, Hive, and Jupyter Notebook.

## Components

- **Hadoop**: Namenode + 2 Datanodes
- **Spark**: Master + 2 Workers
- **Hive**: HiveServer2 with PostgreSQL metastore
- **Jupyter**: Notebook with PySpark support

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
├── datasets/
├── docker-compose.yml
├── hadoop.env
└── README.md
```

## Starting the Environment

1. Create the required directories:
```bash
mkdir -p bigdata-env/{hadoop,spark,hive,jupyter,datasets} bigdata-env/hive/{warehouse,pgdata,conf}
```

2. Start the environment:
```bash
cd bigdata-env
docker-compose up -d
```

3. Wait for all services to start (this may take a few minutes):
```bash
docker-compose ps
```

## Accessing Services

- **Hadoop Namenode UI**: http://localhost:9870
- **Spark Master UI**: http://localhost:8080
- **Jupyter Notebook**: http://localhost:8888
- **HiveServer2**: localhost:10000

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

# Test Spark
df = spark.createDataFrame([(1, "test"), (2, "test2")], ["id", "value"])
df.show()
```

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

## Stopping the Environment

```bash
docker-compose down
```

To remove all data volumes:
```bash
docker-compose down -v
```

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

## Security Notes

- This setup is for development purposes only
- Default passwords are used for PostgreSQL and Jupyter
- HDFS permissions are disabled for easier development
- In production, enable security features and use proper authentication 