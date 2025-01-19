import os #add access to operating system modules

from airflow import models #import models functionality from airflow
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator #import airflow operator for transfers from GCS to BQ
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator #import airflow functionality for BQ dataset manipulation
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator #import standard BQ job functions for airflow


from airflow.utils.dates import days_ago #somewhat irrelevant. Used for scheduling

DATASET_NAME = os.environ.get("GCP_DATASET_NAME", 'addidas') #specify the dataset name you want created in BQ
TABLE_NAME = os.environ.get("GCP_TABLE_NAME", 'shoes_dim') #specify the table name you want created in the BQ dataset

#define and name the DAG
dag = models.DAG(
    dag_id='shoes_dim',
    start_date=days_ago(1),
    schedule_interval=None,
    tags=['shoes_dim'],

    )
#Task 1 to create dataset
create_test_dataset = BigQueryCreateEmptyDatasetOperator(
    task_id='create_airflow_test_dataset', dataset_id=DATASET_NAME, dag=dag
)
#Task 2 to read source file and populate table in BQ
load_csv = GCSToBigQueryOperator(
    task_id='move_shoes_dim',
    bucket='addidasdata', #name of bucket where source data resides
    source_objects=['shoes_dim.csv'], #name of the file to read (include any folders in name if they exist)
    destination_project_dataset_table=f"{DATASET_NAME}.{TABLE_NAME}",
    schema_fields=[
        {'name': 'id', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'best_for_wear', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'gender', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'image_url', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'dominant_color', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'sub_color1', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'sub_color2', 'type': 'STRING', 'mode': 'NULLABLE'},
    ],
    skip_leading_rows=1, #tells airflow that row 1 has the column names and should be skipped
    write_disposition='WRITE_TRUNCATE',
    dag=dag,
)