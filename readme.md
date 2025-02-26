# Airflow ETL Project

## About project

This project showcases an ETL pipeline built using Apache Airflow to retrieve job listing data from the Adzuna API, transform it, and load it into a PostgreSQL database. The entire setup runs in Docker, ensuring a consistent and reproducible environment for extracting, transforming, and loading data.


## How to Run the Pipeline

1. **Run Airflow services**:

```
docker compose up -d
```

2. **Import variables in Airflow**:

 - Open Airflow web UI at http://localhost:8080/
 - Click Admin > Variables > Choose File 
 - Select your `airflow_vars_to_import.json` file with populated var values, and click Import Variables

3. **Create connection to Postgres database in Airflow**:
 - In Airflow web UI, click Admin > Connections > + (Add a new record):
 - In the "Connection Id", enter "postgres"
 - In the "Connection Type", select "Postgres"
 - In the "Host", type "postgres"
 - In the "Database", type "adzuna"
 - Click Save

4. **Run the `adzuna_etl_pipeline` DAG**:
 - In Airflow web UI, click DAGs
 - Find the adzuna_etl_pipeline DAG and switch it On and run it
