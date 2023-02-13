CREATE OR REPLACE TABLE
  `learning-gcp-369416.dezoomcamp_fhv_tripdata.tripdata_partitioned_custered`
PARTITION BY
  DATE(pickup_datetime)
CLUSTER BY
  affiliated_base_number AS
SELECT
  *
FROM
  `learning-gcp-369416.dezoomcamp_fhv_tripdata.fhv_tripdata`; 