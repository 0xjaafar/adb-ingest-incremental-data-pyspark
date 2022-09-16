-- Databricks notebook source
SHOW DATABASES;

-- COMMAND ----------

USE f1_presentation;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

SELECT * FROM constructor_standings;

-- COMMAND ----------

SELECT team, SUM(wins) AS total_wins
  FROM constructor_standings
GROUP BY team
ORDER BY total_wins DESC
       

-- COMMAND ----------

SELECT team_name,
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       AVG(calculated_points) AS avg_points
  FROM f1_presentation.calculated_race_results
  WHERE race_year BETWEEN 2001 AND 2011
GROUP BY team_name
HAVING total_races >= 100
ORDER BY avg_points DESC

-- COMMAND ----------

