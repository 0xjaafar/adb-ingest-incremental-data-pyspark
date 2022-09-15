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

lap_times_schema = StructType([StructField("raceId", IntegerType(), False),
                               StructField("driverId", IntegerType(), False),
                               StructField("lap", IntegerType(), False),
                               StructField("position", IntegerType(), True),
                               StructField("time", StringType(), True),
                               StructField("milliseconds", IntegerType(), True)
,])

# COMMAND ----------

lap_times_df = spark.read.schema(lap_times_schema).csv(path=f"{raw_folder_path}/{v_file_date}/lap_times/lap_times_split_*.csv")

# COMMAND ----------

display(lap_times_df)
lap_times_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step2 - Rename cols and add ingestion date col

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col, lit

# COMMAND ----------

lap_times_final_df = lap_times_df.withColumnRenamed("raceId", "race_id") \
                                 .withColumnRenamed("driverId", "driver_id") \
                                 .withColumn("data_source", lit(v_data_source)) \
                                 .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

lap_times_final_df = add_ingestion_date(lap_times_final_df)

# COMMAND ----------

display(lap_times_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step3 - write the data to the destionation

# COMMAND ----------

#lap_times_final_df.write.parquet(path=f"{processed_folder_path}/lap_times", mode="overwrite")
#lap_times_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.lap_times")

# COMMAND ----------

overwrite_partition(lap_times_final_df, "f1_processed", "lap_times", "race_id")

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1), race_id FROM lap_times GROUP BY race_id;

# COMMAND ----------

