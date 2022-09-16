# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest circuits file

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

# MAGIC %md
# MAGIC #### Step1 - Read the csv file using the spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                    StructField("circuitRef", StringType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("location", StringType(), True),
                                     StructField("country", StringType(), True),
                                     StructField("lat", DoubleType(), True),
                                     StructField("lng", DoubleType(), True),
                                     StructField("alt", IntegerType(), True),
                                     StructField("url", StringType(), True),
                                    ])

# COMMAND ----------

circuits_df = spark.read.csv(path=f"{raw_folder_path}/{v_file_date}/circuits.csv", header=True, schema=circuits_schema)

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step2 - Select only required cols

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

circuits_selected_df = circuits_df.select(col("circuitId"),
                                         col("circuitRef"),
                                          col("name"),
                                          col("location"),
                                          col("country"),
                                          col("lat"),
                                          col("lng"),
                                          col("alt"),
                                         )

# COMMAND ----------

display(circuits_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step3 - Rename cols as required

# COMMAND ----------

from pyspark.sql.functions import lit

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id") \
                                          .withColumnRenamed("circuitRef", "circuit_ref") \
                                          .withColumnRenamed("lat", "latitude") \
                                          .withColumnRenamed("lng", "longitude") \
                                          .withColumnRenamed("alt", "altitude") \
                                          .withColumn("data_source", lit(v_data_source)) \
                                          .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

display(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Step4 - Add ingestion date col to the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

circuits_final_df = add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

display(circuits_final_df)
circuits_final_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step5 - Write data to the destination

# COMMAND ----------

#circuits_final_df.write.parquet(path=f"{processed_folder_path}/circuits", mode="overwrite")
# circuits_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.circuits")
circuits_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.circuits")

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.circuits;

# COMMAND ----------

