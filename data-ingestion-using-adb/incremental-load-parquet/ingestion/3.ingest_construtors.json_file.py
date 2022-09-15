# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest constructors.json file

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

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df = spark.read.json(path=f"{raw_folder_path}/{v_file_date}/constructors.json", schema=constructors_schema)

# COMMAND ----------

display(constructor_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step2 - Drop unwanted cols

# COMMAND ----------

from pyspark.sql.functions import col
constructor_dropped_df = constructor_df.drop(col("url"))

# COMMAND ----------

constructor_dropped_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step3 - Rename cols and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit
constructor_final_df = constructor_dropped_df.withColumnRenamed("constructorId", "constructor_id") \
                                             .withColumnRenamed("constructorRef", "constructor_ref") \
                                             .withColumn("data_source", lit(v_data_source)) \
                                             .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

constructor_final_df = add_ingestion_date(constructor_final_df)

# COMMAND ----------

display(constructor_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step4 - Write data to the destination

# COMMAND ----------

#constructor_final_df.write.parquet(path=f"{processed_folder_path}/constructors", mode="overwrite")
constructor_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.constructors")

# COMMAND ----------

dbutils.notebook.exit("success")