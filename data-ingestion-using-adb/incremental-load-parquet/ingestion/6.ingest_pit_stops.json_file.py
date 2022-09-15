# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

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

pit_stops_schema = StructType([StructField("raceId", IntegerType(), False),
                               StructField("driverId", IntegerType(), True),
                               StructField("stop", StringType(), True),
                               StructField("lap", IntegerType(), True),
                               StructField("time", StringType(), True),
                               StructField("duration", StringType(), True),
                               StructField("milliseconds", IntegerType(), True)
,])

# COMMAND ----------

pit_stops_df = spark.read.json(path=f"{raw_folder_path}/{v_file_date}/pit_stops.json", schema=pit_stops_schema, multiLine=True)

# COMMAND ----------

display(pit_stops_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step2 - Rename cols and add ingestion date col

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col, lit

# COMMAND ----------

pit_stops_final_df = pit_stops_df.withColumnRenamed("raceId", "race_id") \
                                 .withColumnRenamed("driverId", "driver_id") \
                                 .withColumn("data_source", lit(v_data_source)) \
                                 .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

pit_stops_final_df = add_ingestion_date(pit_stops_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step3 - write the data to the destionation

# COMMAND ----------

#pit_stops_final_df.write.parquet(path=f"{processed_folder_path}/pit_stops", mode="overwrite")
# pit_stops_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.pit_stops")

# COMMAND ----------

overwrite_partition(pit_stops_final_df, "f1_processed", "pit_stops", "race_id")

# COMMAND ----------

display(spark.read.parquet("/mnt/adbpipeadls/processed/pit_stops"))

# COMMAND ----------

dbutils.notebook.exit("success")