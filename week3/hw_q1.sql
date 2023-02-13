-- create new external table in bigquery
CREATE OR REPLACE EXTERNAL TABLE `learning-gcp-369416.dezoomcamp_fhv_tripdata.fhv_tripdata`

OPTIONS (
  format = 'CSV',
  uris = ['gs://de_zoomcamp_data_lake_learning-gcp-369416/data/fhv/2019/fhv_tripdata_2019-*.csv.gz']
);

-- query count external table
SELECT COUNT(*) FROM `learning-gcp-369416.dezoomcamp_fhv_tripdata.fhv_tripdata` ;