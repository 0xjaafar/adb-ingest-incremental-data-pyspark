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

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/config"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

name_schema = StructType(fields=[StructField("forename", StringType(), True),
                                 StructField("surname", StringType(), True)
])

# COMMAND ----------

drivers_schema = StructType(fields=[StructField("driverId", IntegerType(), False),
                                    StructField("driverRef", StringType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("code", StringType(), True),
                                    StructField("name", name_schema),
                                    StructField("dob", DateType(), True),
                                    StructField("nationality", StringType(), True),
                                    StructField("url", StringType(), True),
])

# COMMAND ----------

drivers_df = spark.read.json(path=f"{raw_folder_path}/{v_file_date}/drivers.json", schema=drivers_schema)

# COMMAND ----------

display(drivers_df)
drivers_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step2 - Rename cols and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import col, concat, current_timestamp, lit

# COMMAND ----------

drivers_final_df = drivers_df.withColumnRenamed("driverId", "driver_id") \
                             .withColumnRenamed("driverRef", "driver_ref") \
                             .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname"))) \
                             .withColumn("data_source", lit(v_data_source)) \
                             .withColumn("file_date", lit(v_file_date)) \
                             .drop("url")

# COMMAND ----------

drivers_final_df = add_ingestion_date(drivers_final_df)

# COMMAND ----------

display(drivers_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step3 - Write data to the destination

# COMMAND ----------

#drivers_final_df.write.parquet(path=f"{processed_folder_path}/drivers", mode="overwrite")
# drivers_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.drivers")
drivers_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.drivers")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.drivers;

# COMMAND ----------

