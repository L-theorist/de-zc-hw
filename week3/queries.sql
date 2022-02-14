--Creating one table for all 2019 fhv data
CREATE OR REPLACE EXTERNAL TABLE `trips_data_all.external_fhv_tripdata`
OPTIONS (
  format = 'parquet',
  uris = ['gs://dtc_data_lake_de-zoomcamp-week1/raw/fhv_tripdata_2019-*.parquet']
);
 
 

--Question 1
 SELECT COUNT(*) FROM `de-zoomcamp-week1.trips_data_all.external_fhv_tripdata`;
 --42084899

--Question 2
 SELECT COUNT(DISTINCT(dispatching_base_num)) FROM `de-zoomcamp-week1.trips_data_all.external_fhv_tripdata`;
 --792


--Question 3, corr. query, partition by DATE (NOT! datetime) and cluster by dispatching_base_num
CREATE OR REPLACE TABLE `trips_data_all.fhv_partitioned_clustered_tripdata`
PARTITION BY DATE(dropoff_datetime)
CLUSTER BY dispatching_base_num AS (
    SELECT * FROM `trips_data_all.external_fhv_tripdata`
                            );


 /*Question 4: What is the count, estimated and actual data processed for query which counts trip between 
 2019/01/01 and 2019/03/31 for dispatching_base_num B00987, B02060, B02279 */
--Formulation very confusing. Already in Q3.

SELECT COUNT(*) FROM  `trips_data_all.fhv_partitioned_clustered_tripdata`
WHERE DATE(dropoff_datetime) BETWEEN '2019-01-01' AND '2019-03-31' 
AND dispatching_base_num IN ('B00987', 'B02279', 'B02060');


--Question 5 related
CREATE OR REPLACE TABLE `trips_data_all.fhv_double_clustered_tripdata`
CLUSTER BY dispatching_base_num, SR_Flag AS (
    SELECT * FROM `trips_data_all.external_fhv_tripdata`
                            );

SELECT COUNT(*) FROM  `de-zoomcamp-week1.trips_data_all.fhv_double_clustered_tripdata`
WHERE SR_Flag IN (38, 1, 40)
AND dispatching_base_num IN ('B00987', 'B02279', 'B02060');
--estimated data proc. 365 MB, actually processed 4 MB !