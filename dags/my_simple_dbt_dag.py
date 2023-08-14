from airflow.decorators import dag,task
from datetime import datetime
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator

@dag(
    start_date=datetime(2023, 8, 10),
    schedule=None,
    catchup=False,
    tags=['retail']
)

def retail():



    create_retail_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_retail_dataset',
        dataset_id='retail',
        gcp_conn_id='conn_id'

    )

retail()