--check column names
SELECT table_schema, table_name, column_name, data_type 
    FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE table_name = 'yellow_taxi_trips';


--count trips on 15.01 (any year)
SELECT COUNT(*) 
    FROM yellow_taxi_trips 
    WHERE (EXTRACT(MONTH FROM tpep_pickup_datetime) = '01' 
            AND EXTRACT(DAY FROM tpep_pickup_datetime) = '15');

--create VIEW with day, month, year entry of pickup timestamp
CREATE TEMP VIEW trips_view AS SELECT *, EXTRACT(YEAR FROM tpep_pickup_datetime) AS year, 
         EXTRACT(MONTH FROM tpep_pickup_datetime) AS month, 
         EXTRACT(DAY FROM tpep_pickup_datetime) AS day from yellow_taxi_trips;

--find maximal tip amount grouped by day in January (any year)
SELECT year, month, day, max(tip_amount) AS max_tip_amount 
    FROM trips_view 
    WHERE month = 1 
    GROUP BY year, month, day 
    ORDER BY max_tip_amount DESC;

--find the location id of the _zone_ Central Park: 43
SELECT "LocationID" 
    FROM taxi_zones 
    WHERE "Zone" = 'Central Park';

/*find the most popular destination of trips on 14.01 (any year) with pickup location
--Central Park */
SELECT COUNT(*), b."Zone"  
    FROM trips_view AS a 
        JOIN taxi_zones as b 
        on a."DOLocationID" = b."LocationID" 
    WHERE (month = 1 AND day = 14 AND a."PULocationID" = 43) 
    GROUP BY b."Zone" 
    ORDER BY COUNT DESC 
    LIMIT 1;

--find the pair of LocationID's with maximal average total amount: 4/265
SELECT "PULocationID", "DOLocationID",  AVG(total_amount) 
    FROM yellow_taxi_trips 
    GROUP BY "PULocationID", "DOLocationID" 
    ORDER BY AVG DESC 
    LIMIT 1;

--find the Zone names for the pair 4/265 of LocationID's
SELECT "Zone", "LocationID" 
    FROM taxi_zones 
    WHERE "LocationID" = 4 OR "LocationID" = 265;