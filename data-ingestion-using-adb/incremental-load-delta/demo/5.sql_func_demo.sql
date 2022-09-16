-- Databricks notebook source
USE f1_processed;

-- COMMAND ----------

SELECT COUNT(*) FROM drivers;

-- COMMAND ----------

SELECT MAX(dob)
FROM drivers

-- COMMAND ----------

SELECT nationality, COUNT(*) AS total
FROM drivers 
GROUP BY nationality 
ORDER BY total desc;

-- COMMAND ----------

SELECT nationality, COUNT(*) AS total
FROM drivers 
GROUP BY nationality 
HAVING total >= 50
ORDER BY total desc;

-- COMMAND ----------

 SELECT nationality, name, dob, RANK() OVER(PARTITION BY nationality ORDER BY dob asc) AS age_rank FROM drivers
 ORDER BY nationality, age_rank

-- COMMAND ----------

