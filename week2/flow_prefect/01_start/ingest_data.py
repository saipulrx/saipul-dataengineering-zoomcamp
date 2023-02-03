#!/usr/bin/env python
# coding: utf-8
import os
import argparse
from time import time
import pandas as pd
from sqlalchemy import create_engine
import requests
from prefect import flow, task

@task(log_prints=True, retries=3)
def ingest_data(user, password, host, port, db, table_name, url):

    postgres_url = f'postgresql://{user}:{password}@{host}:{port}/{db}'
    engine = create_engine(postgres_url)

    df_iter = pd.read_csv(url, iterator=True, chunksize=100000)
    df = next(df_iter)

    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    df.to_sql(name=table_name, con=engine, if_exists='append')


    while True: 

        try:
            t_start = time()
            
            df = next(df_iter)

            df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
            df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

            df.to_sql(name=table_name, con=engine, if_exists='append')

            t_end = time()

            print('inserted another chunk, took %.3f second' % (t_end - t_start))

        except StopIteration:
            print("Finished ingesting data into the postgres database")
            break

@flow(name="Ingest Flow")
def main_flow():
    user = "root"
    password = "root"
    host = "localhost"
    port = "5433"
    db = "ny_taxi"
    table_name = "green_taxi_trips"
    csv_url = "https://storage.googleapis.com/de_zoomcamp_data_lake_learning-gcp-369416/dataset/green_tripdata_2019-01.csv"

    ingest_data(user, password, host, port, db, table_name, csv_url)

if __name__ == '__main__':
    main_flow()