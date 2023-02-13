SELECT
  COUNT(*)
FROM
  `learning-gcp-369416.dezoomcamp_fhv_tripdata.fhv_tripdata_nonpartition`
WHERE
  PUlocationID IS NULL
  AND DOlocationID IS NULL ;