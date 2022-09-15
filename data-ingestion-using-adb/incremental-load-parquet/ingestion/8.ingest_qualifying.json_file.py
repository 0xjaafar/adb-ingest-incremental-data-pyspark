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

qualifying_schema = StructType([StructField("qualifyId", IntegerType(), False),
                               StructField("raceId", IntegerType(), False),
                               StructField("driverId", IntegerType(), False),
                               StructField("constructorId", IntegerType(), False),
                               StructField("number", IntegerType(), True),
                               StructField("position", IntegerType(), True),
                               StructField("q1", StringType(), True),
                               StructField("q2", StringType(), True),
                               StructField("q3", StringType(), True)
,])

# COMMAND ----------

qualifying_df = spark.read.json(path=f"{raw_folder_path}/{v_file_date}/qualifying/qualifying_split_*.json", 
                                   schema=qualifying_schema,
                                   multiLine=True)

# COMMAND ----------

display(qualifying_df)
qualifying_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step2 - Rename cols and add ingestion date col

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col, lit

# COMMAND ----------

qualifying_final_df = qualifying_df.withColumnRenamed("qualifyId", "qualify_id") \
                                   .withColumnRenamed("raceId", "race_id") \
                                   .withColumnRenamed("driverId", "driver_id") \
                                   .withColumnRenamed("constructorId", "constructor_id") \
                                   .withColumn("data_source", lit(v_data_source)) \
                                   .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

qualifying_final_df = add_ingestion_date(qualifying_final_df)

# COMMAND ----------

display(qualifying_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step3 - write the data to the destionation

# COMMAND ----------

#qualifying_final_df.write.parquet(path=f"{processed_folder_path}/qualifying", mode="overwrite")
#qualifying_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.qualifying")

# COMMAND ----------

overwrite_partition(qualifying_final_df, "f1_processed", "qualifying", "race_id")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1), race_id FROM f1_processed.qualifying GROUP BY race_id;

# COMMAND ----------

dbutils.notebook.exit("success")