# Databricks notebook source
dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/config"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results") \
.filter(f"file_date = '{v_file_date}'")

# COMMAND ----------

race_year_list = df_column_to_list(race_results_df, 'race_year')

# COMMAND ----------

from pyspark.sql.functions import col

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results") \
.filter(col("race_year").isin(race_year_list))

# COMMAND ----------

from pyspark.sql.functions import sum, when, col, count

# COMMAND ----------

constructor_standings_df = race_results_df.groupBy("race_year", "team") \
                                         .agg(
                                             sum("points").alias("total_points"), 
                                             count(when(col("position") == 1, True)).alias("wins")
    )

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import rank, desc

# COMMAND ----------

constructor_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"))
result = constructor_standings_df.withColumn("rank", rank().over(constructor_rank_spec))

# COMMAND ----------

display(result.filter("race_year = 2020"))

# COMMAND ----------

#result.write.parquet(path=f"{presentation_folder_path}/constructor_standings", mode="overwrite")
#result.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.constructor_standings")
overwrite_partition(result, "f1_presentation", "constructor_standings", "race_year" )

# COMMAND ----------

