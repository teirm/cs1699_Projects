--Author: Cyrus Ramavarapu
--Date:   13 November 2016
--Class: CS1699

--Drop table and start from scratch
DROP TABLE temperature_raw_data;
DROP TABLE temperature_data;
DROP TABLE max_temperatures;

--Create table for holding just strings
CREATE TABLE temperature_raw_data (
    raw_data    STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\n';

--Load data into table as just strings
LOAD DATA LOCAL INPATH './input/'
OVERWRITE INTO TABLE temperature_raw_data;

--DEBUG STATEMENT: Get data from old table 
--SELECT * FROM temperature_raw_data; 

--add cutload python script
add FILE cutload.py;

--Create new table for holding actual data
CREATE TABLE temperature_data (
    station_id  STRING,
    year        INT,
    month       INT,
    day         INT,
    temperature INT,
    quality     INT
) 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t';

--Use cutload python script to insert appropriate data
--from old table
INSERT OVERWRITE TABLE temperature_data
SELECT
    TRANSFORM (raw_data)
    USING 'python cutload.py'
    AS (station_id, year, month, day, temperature, quality)
FROM temperature_raw_data;

---DEBUG STATEMENT: Print out the processed data
--SELECT * FROM temperature_data;

--Create a table to store max temperatures
CREATE TABLE max_temperatures (
    station_id STRING,
    year       INT,
    month      INT,
    day        INT,
    max_temp   INT
);

--Get Max temperatures based on groups
INSERT OVERWRITE TABLE max_temperatures
        SELECT station_id, year, month, day, MAX(temperature)
        FROM temperature_data
        WHERE temperature != 9999 AND quality in (0, 1, 4, 5, 9)
        GROUP BY station_id, year, month, day;

---DEBUG STATEMENT: Just check if it worked
--SELECT * FROM max_temperatures;

--Get max mean temperature results
SELECT station_id, month, day, AVG(max_temp)
FROM max_temperatures
GROUP BY station_id, month, day;
