# Databricks notebook source
dbutils.notebook.help()

# COMMAND ----------

v_result = dbutils.notebook.run("1.ingest_circuits.csv_file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-04-18"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("2.ingest_races.csv_file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-04-18"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("3.ingest_construtors.json_file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-04-18"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("4.ingest_drivers.json_file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-04-18"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("5.ingest_results.json_file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-04-18"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("6.ingest_pit_stops.json_file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-04-18"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("7.ingest_lap_times.csv_file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-04-18"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("8.ingest_qualifying.json_file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-04-18"})
v_result

# COMMAND ----------

