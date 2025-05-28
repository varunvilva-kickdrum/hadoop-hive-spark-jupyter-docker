# EV Data Pipeline DAG

This directory contains the Electric Vehicle Data Pipeline DAG that processes EV population data from Washington State's Open Data API.

## Overview

The pipeline consists of 5 main tasks:

1. **load_data_to_hdfs**: Fetches data from the API and uploads to HDFS
2. **transform_data**: Processes and cleans data using Spark, saves in Hudi format
3. **query_data**: Creates Hive tables and runs analytics queries
4. **pipeline_health_check**: Verifies successful completion
5. **cleanup_temp_files**: Removes temporary files

## File Structure

```
airflow/
├── dags/
│   ├── ev_data_pipeline_dag.py    # Main DAG definition
│   └── README.md                  # This file
├── scripts/
│   ├── load_data_to_hdfs.py       # Data loading script
│   ├── transform.py               # Data transformation script
│   └── query.py                   # Data querying script
└── requirements.txt               # Python dependencies
```

## Prerequisites

1. **Infrastructure Running**:
   - Apache Airflow
   - Apache Spark cluster
   - HDFS cluster
   - Apache Hive with Metastore

2. **Python Dependencies**:
   ```bash
   pip install -r ../requirements.txt
   ```

3. **Network Connectivity**:
   - Access to Washington State API: `data.wa.gov`
   - Internal connectivity between services

## Configuration

The DAG uses the following default configurations:

- **Schedule**: Daily (`@daily`)
- **Retries**: 2 attempts with 5-minute delays
- **Owner**: data-team
- **Start Date**: Yesterday

### Service Endpoints

The scripts expect these service endpoints:
- HDFS Namenode: `http://namenode:9000`
- Spark Master: `spark://spark-master:7077`
- Hive Metastore: `thrift://hive:9083`

## Usage

### Manual Trigger

To manually trigger the DAG:

1. Access Airflow UI
2. Navigate to DAGs page
3. Find `ev_data_pipeline`
4. Click the "Play" button

### Monitoring

Monitor the pipeline through:

1. **Airflow UI**: Task status and logs
2. **Spark UI**: Spark job execution (usually on port 8080)
3. **HDFS UI**: File storage status (usually on port 9870)

### Expected Outputs

After successful execution:

- **HDFS**: `/data/Electric_Vehicle_Population_Data.csv`
- **Hudi**: `/data/hudi/ev_data_cleaned/`
- **Hive**: `ev_data.ev_vehicles_cleaned` table

## Data Schema

### Raw Data Fields

The API provides these fields (after transformation):
- `vehical_number`: Vehicle identification number (1-10 chars)
- `county`: County where vehicle is registered
- `city`: City where vehicle is registered
- `state`: State (WA)
- `postal_code`: ZIP code
- `model_year`: Vehicle model year
- `make`: Vehicle manufacturer
- `model`: Vehicle model
- `vehical_type`: Electric vehicle type
- `cavf_eligibility`: Clean Alternative Fuel Vehicle eligibility
- `electric_range`: Electric range in miles
- `base_msrp`: Manufacturer's suggested retail price
- `legislative_district`: Legislative district
- `vehicle_location`: Geographic coordinates
- `electric_utility`: Electric utility provider
- `latitude`: Extracted latitude
- `longitude`: Extracted longitude
- `event_time`: Pipeline processing timestamp

## Troubleshooting

### Common Issues

1. **API Connection Errors**:
   - Check internet connectivity
   - Verify API endpoint availability
   - Check proxy settings if behind firewall

2. **HDFS Connection Errors**:
   - Verify HDFS cluster is running
   - Check namenode accessibility
   - Ensure proper permissions

3. **Spark Job Failures**:
   - Check Spark cluster resources
   - Verify Spark master connectivity
   - Review Spark executor logs

4. **Hive Connection Issues**:
   - Ensure Hive Metastore is running
   - Check thrift connection to port 9083
   - Verify Hive warehouse permissions

### Log Analysis

Check logs in this order:
1. Airflow task logs for high-level errors
2. Spark application logs for processing issues
3. HDFS logs for storage problems
4. Hive logs for query issues

### Recovery Procedures

- **Failed Load Task**: Usually retries automatically; check API status
- **Failed Transform**: May need to clear HDFS output path and retry
- **Failed Query**: Check Hive connectivity and table permissions

## Development

### Adding New Analytics

To add new queries to the pipeline:

1. Modify `scripts/query.py`
2. Add new SQL queries to the `query_ev_data()` function
3. Update the health check to validate new results

### Performance Tuning

Key parameters to adjust:
- Spark executor cores and memory
- Hudi parallelism settings
- HDFS block size for large datasets

## Support

For issues with:
- **DAG Logic**: Check task logs and XCom values
- **Data Quality**: Review transformation logic and input data
- **Performance**: Monitor resource usage in Spark UI
- **Infrastructure**: Verify service connectivity and health

---

**Last Updated**: Created with the pipeline
**Version**: 1.0
**Maintainer**: Data Team 