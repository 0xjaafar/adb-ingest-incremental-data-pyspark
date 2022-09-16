-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC ### Lesson objectives
-- MAGIC 
-- MAGIC 1. Spark SQL Doc
-- MAGIC 2. Create db demo
-- MAGIC 3. Data tab in the UI
-- MAGIC 4. Show cmd
-- MAGIC 5. Desc cmd
-- MAGIC 6. Find the current db

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS demo;

-- COMMAND ----------

SHOW DATABASES;

-- COMMAND ----------

DESCRIBE DATABASE demo;

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED demo;

-- COMMAND ----------

SELECT CURRENT_DATABASE();

-- COMMAND ----------

USE demo;

-- COMMAND ----------

SELECT CURRENT_DATABASE();

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Internal table

-- COMMAND ----------

-- MAGIC %run "../includes/config"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").saveAsTable("demo.race_results_python")

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

DESCRIBE TABLE EXTENDED demo.race_results_python

-- COMMAND ----------

SELECT * FROM demo.race_results_python LIMIT 10;

-- COMMAND ----------

CREATE TABLE race_results_sql
AS 
SELECT * FROM demo.race_results_python LIMIT 100;

-- COMMAND ----------

show tables;

-- COMMAND ----------

SELECT * FROM race_results_sql;

-- COMMAND ----------

DROP TABLE race_results_sql;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### External table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").option("path", f"{presentation_folder_path}/race_results_ext_py").saveAsTable("demo.race_results_ext_py")

-- COMMAND ----------

DESCRIBE TABLE EXTENDED demo.race_results_ext_py

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Views on table

-- COMMAND ----------

show tables;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_race_results
AS 
SELECT *
  FROM demo.race_results_python
  WHERE race_year = 2019;

-- COMMAND ----------

show tables;

-- COMMAND ----------

SELECT * FROM v_race_results

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW gv_race_results
AS 
SELECT *
  FROM demo.race_results_python
  WHERE race_year = 2019;

-- COMMAND ----------

show tables IN global_temp;

-- COMMAND ----------

show tables;

-- COMMAND ----------

CREATE OR REPLACE VIEW pv_race_results
AS 
SELECT *
  FROM demo.race_results_python
  WHERE race_year = 2000;

-- COMMAND ----------

show tables;

-- COMMAND ----------

DESCRIBE TABLE EXTENDED pv_race_results;

-- COMMAND ----------

