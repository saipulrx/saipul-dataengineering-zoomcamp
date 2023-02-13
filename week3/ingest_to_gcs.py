from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint
from prefect.tasks import task_input_hash
from datetime import timedelta
import requests
import os


@task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def write_local(dataset_file: str, dataset_url: str) -> Path:
    
    # create directory if not exist
    dir = f"./data"
    if not os.path.isdir(dir):
        os.makedirs(dir)

    # download file to folder data/
    response = requests.get(dataset_url)
    open(f"{dir}/{dataset_file}", "wb").write(response.content)

    path = Path(f"{dir}/{dataset_file}")
    return path


@task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("gcs-bucket-con")
    gcs_block.upload_from_path(from_path=path, to_path=path,timeout=9000)
    return


@flow()
def el_web_to_gcs(year: int, month: int) -> None:
    """The main ETL function"""
    dataset_file = f"fhv_tripdata_{year}-{month:02}.csv.gz"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/{dataset_file}"

    # df = fetch(dataset_url)
    path = write_local(dataset_file, dataset_url)
    write_gcs(path)


@flow()
def el_to_gcs_flow(
    months: list[int], year: int):
    for month in months:
        el_web_to_gcs(year, month)


if __name__ == "__main__":
    months = [4, 5]
    year = 2020
    el_to_gcs_flow(months, year)
