### Homework Week 3

Download all data required for year 2019 in here then copy all of them to gcs bucket using gsutil copy
```
gsutil -m cp fhv_tripdata_2019* gs://de_zoomcamp_data_lake_learning-gcp-369416/data/fhv/2019
```
Before running the gsutil copy command, make sure that you have already installed gcloud and setup your gcloud user-account.
```
gcloud info
gcloud auth list

# login with this command
gcloud auth login

```
After finish run command gsutil copy, all data are displayed in gcs bucket
![image](https://github.com/saipulrx/saipul-dataengineering-zoomcamp/blob/main/week3/images/list_data_gcs.png)

Run query bellow to create external table 
```
CREATE OR REPLACE EXTERNAL TABLE `learning-gcp-369416.dezoomcamp_fhv_tripdata.fhv_tripdata`

OPTIONS (
  format = 'CSV',
  uris = ['gs://de_zoomcamp_data_lake_learning-gcp-369416/data/fhv/2019/fhv_tripdata_2019-*.csv.gz']
);
![image](https://user-images.githubusercontent.com/22763010/218494782-02a3ae98-1163-45cd-9ee9-80e0192789cc.png)
```

#### Question 1: What is count for fhv vehicle records for year 2019?*
#### Answer : 43,244,696
Explanation :
Run this query in BigQuery 
```
SELECT COUNT(*) FROM `learning-gcp-369416.dezoomcamp_fhv_tripdata.fhv_tripdata` ;
```
Result

![image](https://github.com/saipulrx/saipul-dataengineering-zoomcamp/blob/main/week3/images/result_query_q1.png)

#### Question 2: What is the estimated amount of data that will be read when you execute your query on the External Table and the Materialized Table?
#### Answer : 0 MB for the External Table and 317.94MB for the Materialized Table
Explanation :
Before estimate amount of data when execute query for materialized table, please run below query for create materialized table :
```
CREATE OR REPLACE TABLE `learning-gcp-369416.dezoomcamp_fhv_tripdata.fhv_tripdata_nonpartition`
AS SELECT * FROM `learning-gcp-369416.dezoomcamp_fhv_tripdata.fhv_tripdata`
```
Type bellow query in BigQuery for external table but don’t run it. Check estimated amount of data in BigQuery

![image](https://github.com/saipulrx/saipul-dataengineering-zoomcamp/blob/main/week3/images/estimate_external_table_q2.png)
Estimated amount of data when run query is 0 B for external table

Type bellow query in BigQuery for materialized table but don’t run it. Check estimated amount of data in BigQuery
![image](https://github.com/saipulrx/saipul-dataengineering-zoomcamp/blob/main/week3/images/estimate_materialize_table_q2.png)

Estimated amount of data when run query is 317.94 MB for materialized table

Question 3: How many records have both a blank (null) PUlocationID and DOlocationID in the entire dataset?

Answer : 717,748
Explanation :
Run bellow query in BigQuery
```
SELECT
  COUNT(*)
FROM
  `learning-gcp-369416.dezoomcamp_fhv_tripdata.fhv_tripdata_nonpartition`
WHERE
  PUlocationID IS NULL
  AND DOlocationID IS NULL ;
```

Result

![image](https://github.com/saipulrx/saipul-dataengineering-zoomcamp/blob/main/week3/images/result_query_q3.png)

#### Question 4: What is the best strategy to make an optimized table in Big Query if your query will always filter by pickup_datetime and order by affiliated_base_number?
#### Answer : Partition by pickup_datetime Cluster on affiliated_base_number

#### Question 5:  
Write a query to retrieve the distinct affiliated_base_number between pickup_datetime 
03/01/2019 and 03/31/2019 (inclusive)

Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values? 

#### Answer: 647.87 MB for non-partitioned table and 23.06 MB for the partitioned table

Explanation:
Create new partition and clustering table from external table
```
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
```
Estimation of data when run query from non partition table : 647.87 MB
![image](https://github.com/saipulrx/saipul-dataengineering-zoomcamp/blob/main/week3/images/result_query_nonpartition_q5.png)

Estimation of data when run query from partition and clustering table : 23.05 MB
![image](https://github.com/saipulrx/saipul-dataengineering-zoomcamp/blob/main/week3/images/result_query_partition_q5.png)

#### Question 6: Where is the data stored in the External Table you created?
#### Answer : GCP Bucket

#### Question 7: It is best practice in Big Query to always cluster your data.
#### Answer : True












