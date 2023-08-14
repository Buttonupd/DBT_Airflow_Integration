from airflow.decorators import dag,task
from datetime import datetime
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator


@dag(
    start_date=datetime(2023,1,1),
    schedule=None,
    catchup=False,
    tags=['retail']
)

def retail():
    pass

retail()

