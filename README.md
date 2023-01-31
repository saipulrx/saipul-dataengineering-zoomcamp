
# A Repository for join Data Engineering Zoomcamp

For detail material Data Engineering Zoomcamp please see https://github.com/DataTalksClub/data-engineering-zoomcamp

## Run Docker postgres
```
docker run -it \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v $(pwd)/ny_taxi_postgres_data:/var/lib/postgresql/data \
  -p 5432:5432 \
  postgres:13
```

or you can run docker-compose
```
docker-compose up
```

## Ingestion Data to Posgres locally
Run this script in terminal

```
CSV="yellow_tripdata_2021-01.csv"

python ingest_data.py \
  --user=root \
  --password=root \
  --host=localhost \
  --port=5433 \
  --db=ny_taxi \
  --table_name=yellow_taxi_trips \
  --csv_file=${CSV}
  ```
