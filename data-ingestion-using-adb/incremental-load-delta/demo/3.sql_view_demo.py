# Databricks notebook source
# MAGIC %run "../includes/config"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

race_results_df.createOrReplaceTempView("v_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM v_race_results WHERE race_year = 2020

# COMMAND ----------

race_year = 2020
race_results_2020_df = spark.sql(f"SELECT * FROM v_race_results WHERE race_year = {race_year}")

# COMMAND ----------

display(race_results_2020_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW tables

# COMMAND ----------

race_results_df.createOrReplaceGlobalTempView("gv_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW tables in global_temp;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM global_temp.gv_race_results;

# COMMAND ----------

