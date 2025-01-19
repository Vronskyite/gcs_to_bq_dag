import os

from airflow import models
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


from airflow.utils.dates import days_ago

DATASET_NAME = os.environ.get("GCP_DATASET_NAME", 'addidas')
TABLE_NAME = os.environ.get("GCP_TABLE_NAME", 'shoes_dim')

dag = models.DAG(
    dag_id='shoes_dim',
    start_date=days_ago(1),
    schedule_interval=None,
    tags=['shoes_dim'],

    )

create_test_dataset = BigQueryCreateEmptyDatasetOperator(
    task_id='create_airflow_test_dataset', dataset_id=DATASET_NAME, dag=dag
)
load_csv = GCSToBigQueryOperator(
    task_id='move_shoes_dim',
    bucket='addidasdata',
    source_objects=['shoes_dim.csv'],
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
    skip_leading_rows=1,
    write_disposition='WRITE_TRUNCATE',
    dag=dag,
)