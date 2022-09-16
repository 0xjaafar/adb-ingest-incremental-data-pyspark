-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1. Create tables for CSV files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create circuits table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.circuits;
CREATE EXTERNAL TABLE IF NOT EXISTS f1_raw.circuits(
circuitId INT,
circuitRef STRING,
name STRING,
location STRING,
country STRING,
lat DOUBLE,
lng DOUBLE,
alt INT,
url STRING
)
USING csv
LOCATION "/mnt/adbtrainingsadls/raw/circuits.csv"
OPTIONS (header true)


-- COMMAND ----------

SELECT * FROM f1_raw.circuits;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create races table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.races;
CREATE EXTERNAL TABLE IF NOT EXISTS f1_raw.races(
raceId INT,
year INT,
round INT,
circuitId INT,
name STRING,
date DATE,
time STRING,
url STRING
)
USING csv
LOCATION "/mnt/adbtrainingsadls/raw/races.csv"
OPTIONS (header true)


-- COMMAND ----------

SELECT * FROM f1_raw.races;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2. Create tables for JSON files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create constructors table
-- MAGIC - Single Line JSON
-- MAGIC - Simple Structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.constructors;
CREATE EXTERNAL TABLE IF NOT EXISTS f1_raw.constructors(
constructorId INT,
constructorRef STRING,
name STRING,
nationality STRING,
url STRING
)
USING json
LOCATION "/mnt/adbtrainingsadls/raw/constructors.json"


-- COMMAND ----------

SELECT * FROM f1_raw.constructors

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create drivers table
-- MAGIC - Single Line JSON
-- MAGIC - Complexe Structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers;
CREATE EXTERNAL TABLE IF NOT EXISTS f1_raw.drivers(
driverId INT,
driverRef STRING,
number INT,
code STRING,
name STRUCT<forename: STRING, surname: STRING>,
dob DATE,
nationality STRING,
url STRING
)
USING json
LOCATION "/mnt/adbtrainingsadls/raw/drivers.json"

-- COMMAND ----------

SELECT * FROM f1_raw.drivers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create results table
-- MAGIC - Single Line JSON
-- MAGIC - Simple Structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.results;
CREATE EXTERNAL TABLE IF NOT EXISTS f1_raw.results(
resultId INT,
raceId INT,
driverId INT,
constructorId INT,
number INT,
grid INT,
position INT,
positionText STRING,
positionOrder INT,
points FLOAT,
laps INT,
time STRING,
milliseconds INT,
fastestLap INT,
rank INT,
fastestLapTime STRING,
fastestLapSpeed STRING,
statusId INT
)
USING json
LOCATION "/mnt/adbtrainingsadls/raw/results.json"


-- COMMAND ----------

SELECT * FROM f1_raw.results

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create pit_stops table
-- MAGIC - Single Line JSON
-- MAGIC - Complexe Structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.pit_stops;
CREATE EXTERNAL TABLE IF NOT EXISTS f1_raw.pit_stops(
driverId INT,
duration STRING,
lap INT,
milliseconds INT,
raceID INT,
stop INT,
time STRING
)
USING json
LOCATION "/mnt/adbtrainingsadls/raw/pit_stops.json"
OPTIONS (multiLine true)

-- COMMAND ----------

SELECT * FROM f1_raw.pit_stops;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 3. Create table for list of files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create lap_times table
-- MAGIC - Csv file
-- MAGIC - Multiple files

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.lap_times;
CREATE EXTERNAL TABLE IF NOT EXISTS f1_raw.lap_times(
raceID INT,
driverId INT,
lap INT,
position INT,
time STRING,
milliseconds INT
)
USING csv
LOCATION "/mnt/adbtrainingsadls/raw/lap_times"

-- COMMAND ----------

SELECT * FROM f1_raw.lap_times;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create qualifying table
-- MAGIC - Multiline JSON file 
-- MAGIC - Multiple files

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;
CREATE EXTERNAL TABLE IF NOT EXISTS f1_raw.qualifying(
constructorId INT,
driverId INT,
number INT,
position INT,
q1 STRING,
q2 STRING,
q3 STRING,
qualifyId INT,
raceId INT
)
USING json
LOCATION "/mnt/adbtrainingsadls/raw/qualifying"
OPTIONS (multiLine true)

-- COMMAND ----------

SELECT * FROM f1_raw.qualifying;

-- COMMAND ----------

