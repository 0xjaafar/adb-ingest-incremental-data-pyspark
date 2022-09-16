# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest races file

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step1 - Read the csv file using the spark dataframe reader

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

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType

# COMMAND ----------

races_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                 StructField("year", IntegerType(), True),
                                 StructField("round", IntegerType(), True),
                                 StructField("circuitId", IntegerType(), False),
                                 StructField("name", StringType(), True),
                                 StructField("date", StringType(), True),
                                 StructField("time", StringType(), True),
                                 StructField("url", StringType(), True)])

# COMMAND ----------

races_df = spark.read.csv(path=f"{raw_folder_path}/{v_file_date}/races.csv", header=True, schema=races_schema)

# COMMAND ----------

display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step2 - Select only required fields

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

races_selected_df = races_df.select(col("raceId"),
                                         col("year"),
                                          col("round"),
                                          col("circuitId"),
                                          col("name"),
                                          col("date"),
                                          col("time"),
                                         )

# COMMAND ----------

display(races_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step3 - Rename cols as required

# COMMAND ----------

from pyspark.sql.functions import lit
races_renamed_df = races_selected_df.withColumnRenamed("raceId", "race_id") \
                                          .withColumnRenamed("year", "race_year") \
                                          .withColumnRenamed("circuitId", "circuit_id") \
                                          .withColumn("data_source", lit(v_data_source)) \
                                          .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

display(races_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step4 - Add race_timestamp + ingestion_date cols

# COMMAND ----------

from pyspark.sql.functions import col, concat, lit, current_timestamp

# COMMAND ----------

races_final_df = races_renamed_df.withColumn("race_timestamp", concat(col("date"), lit(" "), col("time")).cast(TimestampType())) \
                                 .drop("time", "date")

# COMMAND ----------

races_final_df = add_ingestion_date(races_final_df)

# COMMAND ----------

display(races_final_df)
races_final_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step5 - Save the ouput to the destination

# COMMAND ----------

#races_final_df.write.parquet(path=f"{processed_folder_path}/races", mode="overwrite")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step6 - Partition data at rest

# COMMAND ----------

#races_final_df.write.partitionBy("race_year").parquet(path=f"{processed_folder_path}/races", mode="overwrite")
# races_final_df.write.partitionBy("race_year").mode("overwrite").format("parquet").saveAsTable("f1_processed.races")
races_final_df.write.partitionBy("race_year").mode("overwrite").format("delta").saveAsTable("f1_processed.races")

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.races

# COMMAND ----------

