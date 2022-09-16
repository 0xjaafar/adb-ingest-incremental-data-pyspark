# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest drivers.json file

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step1 - Read the json file using spark dataframe reader

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/config"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

results_schema = "resultId INT, raceId INT, driverId INT, constructorId INT, \
                  number INT, grid INT, position INT, positionText STRING, \
                  positionOrder INT, points FLOAT, laps INT, time STRING, \
                  milliseconds INT, fastestLap INT, rank INT, fastestLapTime STRING,\
                  fastestLapSpeed STRING, statusId INT"


# COMMAND ----------

results_df = spark.read.json(path=f"{raw_folder_path}/{v_file_date}/results.json", schema=results_schema)

# COMMAND ----------

display(results_df)
results_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step2 - Rename and drop unwanted cols and add ingestion date col

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col, lit

# COMMAND ----------

results_final_df = results_df.withColumnRenamed("resultId", "result_id") \
                                .withColumnRenamed("raceId", "race_id") \
                                .withColumnRenamed("driverId", "driver_id") \
                                .withColumnRenamed("constructorId", "constructor_id") \
                                .withColumnRenamed("positionText", "position_text") \
                                .withColumnRenamed("positionOrder", "position_order") \
                                .withColumnRenamed("fastestLap", "fastest_lap") \
                                .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
                                .withColumnRenamed("fastestLapSpeed", "fastestlap_speed") \
                                .withColumn("data_source", lit(v_data_source)) \
                                .withColumn("file_date", lit(v_file_date)) \
                                .drop(col("statusId"))

# COMMAND ----------

results_final_df = add_ingestion_date(results_final_df)

# COMMAND ----------

results_deduped_df = results_final_df.dropDuplicates(["race_id", "driver_id"])

# COMMAND ----------

display(results_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step3 - write the data to the destionation and partition by race_id

# COMMAND ----------

# MAGIC %md 
# MAGIC ###### Method 1 - Increment data using ALTER STATEMENT (not very efficient since we need to DROP Parition each time)

# COMMAND ----------

# for race_id_list in results_final_df.select('race_id').distinct().collect():
#     if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#         spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})")

# COMMAND ----------

#results_final_df.write.partitionBy("race_id").parquet(path=f"{processed_folder_path}/results", mode="overwrite")
#results_final_df.write.partitionBy("race_id").mode("overwrite").format("parquet").saveAsTable("f1_processed.results")


# COMMAND ----------

# MAGIC %md 
# MAGIC ###### Method 2 - Increment data using INSERT INTO (dynamic partition)

# COMMAND ----------

# overwrite_partition(results_final_df, "f1_processed", "results", "race_id")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Method3 - Increment data using delta

# COMMAND ----------

merge_condition = "tgt.result_id = src.result_id AND tgt.race_id = src.race_id"
merge_delta_data(results_deduped_df,"f1_processed", "results", merge_condition, processed_folder_path, "race_id")

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(1) FROM f1_processed.results GROUP BY race_id

# COMMAND ----------



# COMMAND ----------

