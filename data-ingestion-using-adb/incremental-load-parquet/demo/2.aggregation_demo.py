# Databricks notebook source
# MAGIC %run "../includes/config"

# COMMAND ----------

 race_results = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

demo_df = race_results.filter("race_year = 2020")

# COMMAND ----------

display(demo_df)

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct, sum, count_distinct

# COMMAND ----------

demo_df.select(count("race_name")).show()

# COMMAND ----------

demo_df.select(count_distinct("race_name")).show()

# COMMAND ----------

demo_df.select(sum("points")).show()

# COMMAND ----------

demo_df.filter("driver_name = 'Max Verstappen'").select(sum("points")).show()

# COMMAND ----------

demo_df.groupBy("driver_name")\
       .agg(sum("points").alias("total_points"), countDistinct("race_name").alias("number_of_races")) \
       .show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### window functions

# COMMAND ----------

demo_df = race_results.filter("race_year in (2020,2019)")

# COMMAND ----------

display(demo_df)

# COMMAND ----------

demo_groupped_df = demo_df.groupBy("driver_name", "race_year")\
                           .agg(sum("points").alias("total_points"), countDistinct("race_name").alias("number_of_races"))

# COMMAND ----------

display(demo_groupped_df)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

# COMMAND ----------

driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"))
result = demo_groupped_df.withColumn("rank", rank().over(driver_rank_spec))

# COMMAND ----------

display(result)