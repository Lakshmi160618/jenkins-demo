"""A liveness prober dag for monitoring composer.googleapis.com/environment/healthy."""
import os
import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta
from google.cloud import storage
import pymysql
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator 
    
default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Define your SQL query
INSERT_ROWS_QUERY = """
INSERT INTO onyx-syntax-413205.Datalake_dev.customer_profile_datalake (
    customer_id,
    username,
    email,
    phone_number,
    address,
    effective_from,
    effective_to,
    is_current
)
SELECT
    id AS customer_id,
    username,
    email,
    phone_number,
    address,
    CAST(created_at AS TIMESTAMP) AS effective_from,
    TIMESTAMP '9999-12-31 00:00:00 UTC' AS effective_to,
    CASE 
        WHEN CAST(created_at AS TIMESTAMP) <= CURRENT_TIMESTAMP() THEN TRUE 
        ELSE FALSE 
    END AS is_current
FROM
    onyx-syntax-413205.Master.customers_one;
"""
# Construct the absolute path to the JSON schema file
json_schema_file_path = os.path.join('/home/airflow/gcs/dags', 'bigquery_stage_schema_file.json')

# Define the bash command with the JSON schema file path incorporated
bash_command = """
bq --location=us load \
   --source_format=CSV \
   --skip_leading_rows=0 \
   onyx-syntax-413205.Master.customers_stage \
   "gs://demo_01bucket/bq_data.csv" \
   {}  # This curly braces will be replaced with the actual file path
""".format(json_schema_file_path)

def cloudsql_to_gcs():
    # Set up the connection to Cloud SQL
    connection = pymysql.connect(
        host='34.135.251.97',
        user='hani',
        password='hani',
        database='crm_database',
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )
    
    # Query to retrieve data from Cloud SQL table
    query = "SELECT * FROM customers"
    
    # Execute the query
    with connection.cursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchall()
    
    # Write the data to a CSV file
    csv_data = '\n'.join([','.join(str(value) for value in row.values()) for row in result])
    with open('bq_data.csv', 'w') as f:
        f.write(csv_data)
    
    # Upload the CSV file to GCS bucket
    storage_client = storage.Client()
    bucket = storage_client.bucket('demo_01bucket')
    blob = bucket.blob('bq_data.csv')
    blob.upload_from_filename('bq_data.csv')
    
    print("Data has been exported from Cloud SQL and uploaded to GCS bucket.")

dag = DAG(
    'cloudsql_to_bigquery_etl',
    default_args=default_args,
    description='liveness monitoring dag',
    schedule_interval=None,
    max_active_runs=2,
    catchup=False,
    dagrun_timeout=timedelta(minutes=10),
)

# priority_weight has type int in Airflow DB, uses the maximum.
# Define the PythonOperator
python_operator_task = PythonOperator(
    task_id='cloudsql_to_gcs_export',
    python_callable=cloudsql_to_gcs,
    dag=dag
)

bash_operator_task = BashOperator(
    task_id='gcs_to_bq_stage_t',
    bash_command=bash_command,
    dag=dag
)

# Define your BigQueryInsertJobOperator
insert_query_job = BigQueryInsertJobOperator(
    task_id="bq_stage_to_bq_datalake",
    configuration={
        "query": {
            "query": INSERT_ROWS_QUERY,
            "useLegacySql": False,
            "priority": "BATCH",
        }
    },
    location='US',  # Define your BigQuery location
    dag=dag
)

python_operator_task >> bash_operator_task >> insert_query_job