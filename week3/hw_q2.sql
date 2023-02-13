-- Create new table non partition from external table
CREATE OR REPLACE TABLE `learning-gcp-369416.dezoomcamp_fhv_tripdata.fhv_tripdata_nonpartition`
AS SELECT * FROM `learning-gcp-369416.dezoomcamp_fhv_tripdata.fhv_tripdata`;

-- check estimation of data before run query in bigquery 
-- external table
SELECT COUNT(DISTINCT(Affiliated_base_number)) FROM `learning-gcp-369416.dezoomcamp_fhv_tripdata.fhv_tripdata`;

-- materialize table
SELECT COUNT(DISTINCT(Affiliated_base_number)) FROM `learning-gcp-369416.dezoomcamp_fhv_tripdata.fhv_tripdata_nonpartition` ;

