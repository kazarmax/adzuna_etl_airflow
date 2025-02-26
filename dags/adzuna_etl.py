from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import pandas as pd
import requests
import csv
import psycopg2
import json
import os
import math
import shutil


# Shared folders mounted to all services to store raw and transformed data files
SHARED_DATA_FOLDER = "/opt/airflow/shared-data/"
RAW_DATA_UNPROCESSED_FOLDER = "/opt/airflow/shared-data/raw_data/to_process/"
RAW_DATA_PROCESSED_FOLDER = "/opt/airflow/shared-data/raw_data/processed/"
TRANSFORMED_DATA_FOLDER = "/opt/airflow/shared-data/transformed_data/"


# adzuna database connection credentials
db_name     = Variable.get("ADZUNA_DB_NAME")
db_user     = Variable.get("ADZUNA_DB_USER")
db_host     = Variable.get("ADZUNA_DB_HOST")
db_password = Variable.get("ADZUNA_DB_PASSWORD")


def get_adzuna_raw_data():

    # Getting Adzuna API creds
    ADZUNA_APP_ID = Variable.get("ADZUNA_APP_ID")
    ADZUNA_APP_KEY = Variable.get("ADZUNA_APP_KEY")

    # Define the API endpoint and base parameters
    url = "https://api.adzuna.com/v1/api/jobs/ca/search/"
    base_params = {
        'app_id': ADZUNA_APP_ID,
        'app_key': ADZUNA_APP_KEY,
        'results_per_page': 50,  # Maximum allowed results per page
        'what_phrase': "data engineer",
        'max_days_old': 2,
        'sort_by': "date"
    }
    print("Adzuna Function triggered to extract raw json data from Adzuna API.")

    # Initialize a list to store all job postings
    all_job_postings = []
    
    # Make the first request to determine the total number of pages
    print("Making the first request to determine the total number of pages")
    response = requests.get(f"{url}1", params=base_params)
    
    if response.status_code != 200:
        error_message = f"Error fetching page 1: {response.status_code}, {response.text}"
        print(error_message)

    data = response.json()  # Parse the JSON response
    total_results = data.get('count', 0)
    results_per_page = base_params['results_per_page']

    # Calculate the total number of pages
    total_pages = math.ceil(total_results / results_per_page)
    print(f"Total number of pages = {total_pages}")

    # Store the results from the first page
    all_job_postings.extend(data.get('results', []))

    # Loop through the remaining pages and request data from each
    print("Looping through the remaining pages to request data from each")
    for page in range(2, total_pages + 1):  # Start from page 2
        response = requests.get(f"{url}{page}", params=base_params)
        if response.status_code == 200:
            page_data = response.json()
            all_job_postings.extend(page_data.get('results', []))
        else:
            print(f"Error fetching page {page}: {response.status_code}, {response.text}")

    print(f"Total jobs retrieved: {len(all_job_postings)}")

    raw_json_data = json.dumps({"items": all_job_postings})
    raw_json_bytes = raw_json_data.encode('utf-8')

    # Generate a filename with the current timestamp to store raw data
    current_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = f"adzuna_raw_data_{current_timestamp}.json"
    file_path = RAW_DATA_UNPROCESSED_FOLDER + file_name
    print(f"File name to store raw data: {file_path}")

    with open(file_path, "wb") as file:
        file.write(raw_json_bytes)
    print("Done")

    return file_path


def transform_raw_data(**kwargs):

    print("Launched transform function")
    # Pull the file path from the XCom of extract_task
    ti = kwargs['ti']  # "ti" = TaskInstance
    raw_data_file_path = ti.xcom_pull(task_ids='extract_raw_data_task')
    print(f"Received raw_data_file_path from previous task: '{raw_data_file_path}'")

    # Reading json data from raw data source file
    with open(raw_data_file_path, "r") as f:
        raw_data = json.load(f)
    all_job_postings = raw_data['items']

    # Parsing json data to a list of dicts
    parsed_jobs = []
    for job in all_job_postings:
        parsed_jobs.append(
            dict(
                job_id = job['id'],
                job_title = job['title'],
                job_location = job['location']['display_name'],
                job_company = job['company']['display_name'],
                job_category = job['category']['label'],
                job_description = job['description'],
                job_url = job['redirect_url'],
                job_created = job['created']
            )
        )

    jobs_df = pd.DataFrame.from_dict(parsed_jobs)
    jobs_df['job_created'] = pd.to_datetime(jobs_df['job_created'])
    print(f"Number of records in the transformed data: {len(jobs_df)}")

    # Generate a filename with the current timestamp to store transformed data
    current_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = f"adzuna_transformed_data_{current_timestamp}.csv"
    file_path = TRANSFORMED_DATA_FOLDER + file_name
    print(f"File name to store transformed data: {file_path}")

    # Saving transformed data as .csv file
    jobs_df.to_csv(file_path, index=False)
    print("Done")

    return file_path


def load_csv_to_staging_postgres(**kwargs):

    print("Launched load_csv_to_staging_postgres function")
    # Pull the file path from the XCom of extract_task
    ti = kwargs['ti']  # "ti" = TaskInstance
    data_file_path = ti.xcom_pull(task_ids='transform_raw_data_task')
    print(f"Received data_file_path from previous task: '{data_file_path}'")

    conn = psycopg2.connect(dbname=db_name, user=db_user, host=db_host, password=db_password)
    cur = conn.cursor()

    try:
        # Open the CSV file in read mode.
        with open(data_file_path, 'r') as f:
            # Prepare the COPY command.
            # This example assumes the CSV file has a header row.
            copy_sql = """ COPY stg_adzuna_jobs FROM STDIN WITH (FORMAT csv, DELIMITER ',', HEADER, QUOTE '\"') """ 
            # Execute the COPY command using copy_expert.
            cur.copy_expert(sql=copy_sql, file=f)
        
        # Commit the transaction to persist changes.
        conn.commit()
        print("CSV data loaded successfully into table 'stg_adzuna_jobs'")
    
    except Exception as e:
        # Roll back in case of any error during the copy.
        conn.rollback()
        print("Error loading CSV data:", e)
    
    cur.close()
    conn.close()


def move_processed_file(**kwargs):
    print("Launched move_processed_file function")
    # Pull the file path from the XCom of extract_task
    ti = kwargs['ti']  # "ti" = TaskInstance
    raw_data_file_path = ti.xcom_pull(task_ids='extract_raw_data_task')
    print(f"Received raw_data_file_path from extraction task: '{raw_data_file_path}'")
    source_file = raw_data_file_path
    print(f"source_file = {source_file}")
    destination_file = RAW_DATA_PROCESSED_FOLDER + raw_data_file_path.split("/")[-1]
    print(f"destination_file = {destination_file}")
    
    # Move (or rename) the file
    shutil.move(source_file, destination_file)
    print(f"Moved {source_file} -> {destination_file}")


default_args = {
    "owner": "Maksim",
    "depends_on_past": False,
    "start_date": datetime(2025, 2, 22),
}


with DAG(
    dag_id = "adzuna_etl_pipeline",
    default_args = default_args,
    description="Download, transform and load data to Postgres",
    schedule_interval= None #timedelta(hours=1) to run every hour; timedelta(days=1) to run every 24 hours
) as dag:
    
    extract_raw_data_task = PythonOperator(
        task_id = "extract_raw_data_task",
        python_callable = get_adzuna_raw_data
    )

    transform_raw_data_task = PythonOperator(
        task_id = "transform_raw_data_task",
        python_callable = transform_raw_data
    )

    load_csv_to_staging_postgres_task = PythonOperator(
        task_id="load_csv_to_postgres_task",
        python_callable=load_csv_to_staging_postgres
    )

    truncate_stage_table_task = PostgresOperator(
        task_id="truncate_stage_table_task",
        postgres_conn_id="postgres",
        sql= "TRUNCATE TABLE stg_adzuna_jobs;"
    )


    upsert_batch_command = f'PGPASSWORD="{db_password}" psql -U airflow -h postgres -d adzuna -f /opt/scripts/upsert_into_final_table.sql'
    upsert_batch_to_final_table_task = BashOperator(
        task_id="upsert_batch_to_final_table_task",
        bash_command=upsert_batch_command
    )

    move_processed_file_task = PythonOperator(
        task_id="move_processed_file_task",
        python_callable=move_processed_file,
    )


    extract_raw_data_task >> transform_raw_data_task >> truncate_stage_table_task
    truncate_stage_table_task >> load_csv_to_staging_postgres_task
    load_csv_to_staging_postgres_task >> upsert_batch_to_final_table_task >> move_processed_file_task
