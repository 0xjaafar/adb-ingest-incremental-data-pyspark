# Databricks notebook source
# MAGIC %run "../includes/config"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

results_df = spark.read.format("delta").load(f"{processed_folder_path}/results") \
.filter(f"file_date = '{v_file_date}'") \
.withColumnRenamed("time", "race_time") \
.withColumnRenamed("race_id", "result_race_id") \
.withColumnRenamed("file_date", "result_file_date") 

# COMMAND ----------

circuits_df = spark.read.format("delta").load(f"{processed_folder_path}/circuits").withColumnRenamed("location", "circuit_location")

# COMMAND ----------

constructors_df = spark.read.format("delta").load(f"{processed_folder_path}/constructors").withColumnRenamed("name", "team")

# COMMAND ----------

races_df = spark.read.format("delta").load(f"{processed_folder_path}/races").withColumnRenamed("name", "race_name") \
                                                               .withColumnRenamed("race_timestamp", "race_date")

# COMMAND ----------

drivers_df = spark.read.format("delta").load(f"{processed_folder_path}/drivers").withColumnRenamed("name", "driver_name") \
                                                                   .withColumnRenamed("number", "driver_number") \
                                                                   .withColumnRenamed("nationality", "driver_nationality") 

# COMMAND ----------

circuit_race_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "inner") \
.select(races_df.race_id, races_df.race_year, races_df.race_name, races_df.race_date, circuits_df.circuit_location)

# COMMAND ----------

# MAGIC %md
# MAGIC #### join result to all dataframes

# COMMAND ----------

race_result_df = results_df.join(circuit_race_df, results_df.result_race_id == circuit_race_df.race_id)\
                           .join(drivers_df, results_df.driver_id == drivers_df.driver_id) \
                           .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = race_result_df.select("race_id","race_year", "race_name", "race_date", "circuit_location", "driver_name", "driver_number", "driver_nationality", 
                                 "team", "grid", "fastest_lap", "race_time", "points", "position", "result_file_date") \
                                 .withColumn("create_date", current_timestamp()) \
                                 .withColumnRenamed("result_file_date", "file_date")

# COMMAND ----------

display(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Save the ouput to the destination

# COMMAND ----------

#final_df.write.parquet(path=f"{presentation_folder_path}/race_results", mode="overwrite")
#final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.race_results")
#overwrite_partition(final_df, "f1_presentation", "race_results", "race_id")

# COMMAND ----------

merge_condition = "tgt.driver_name = src.driver_name AND tgt.race_id = src.race_id"
merge_delta_data(final_df,"f1_presentation", "race_results", merge_condition, presentation_folder_path, "race_id")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.race_results;

# COMMAND ----------

