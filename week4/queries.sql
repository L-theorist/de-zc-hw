--Creating one table for all 2019/2020 green taxi data
CREATE OR REPLACE EXTERNAL TABLE `trips_data_all.green_tripdata`
OPTIONS (
  format = 'parquet',
  uris = ['gs://dtc_data_lake_de-zoomcamp-week1/raw/green_tripdata_2019-*.parquet', 'gs://dtc_data_lake_de-zoomcamp-week1/raw/green_tripdata_2020-*.parquet']
);
 
