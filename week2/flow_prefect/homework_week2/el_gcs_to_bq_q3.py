from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("gcs-bucket-con")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")
    path = Path(f"../data/{gcs_path}")

    df = pd.read_parquet(path)
    return df


@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("block-gcs-credentials")

    df.to_gbq(
        destination_table="dezoomcamp.taxi",
        project_id="learning-gcp-369416",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )


@flow()
def el_gcs_to_bq(color: str, year: int, month: int) -> int:
    """Main ETL flow to load data into Big Query"""
    df = extract_from_gcs(color, year, month)
    write_bq(df)
    
    print(f"Rows transfered: {len(df)}")

    return len(df)

@flow()
def el_parent_flow(
    color: str = "yellow", year: int = 2019,month: list[int] = [2, 3]) -> None:
    total_rows = 0

    for month in months:
        total_rows += el_gcs_to_bq(color, year, month)
    
    print(f"Total data being transfered into bigQuery is: {total_rows}")

if __name__ == "__main__":
    color = "yellow"
    months = [2, 3]
    year = 2019
    el_parent_flow(color, year, months)
