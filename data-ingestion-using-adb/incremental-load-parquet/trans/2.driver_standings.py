# Databricks notebook source
dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/config"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Find race years for which the data need to be processed

# COMMAND ----------

race_results_list = spark.read.parquet(f"{presentation_folder_path}/race_results") \
.filter(f"file_date = '{v_file_date}'") \
.select("race_year") \
.distinct() \
.collect()

# COMMAND ----------

race_year_list = []
for race_year in race_results_list:
    race_year_list.append(race_year.race_year)

# COMMAND ----------

from pyspark.sql.functions import col

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results") \
.filter(col("race_year").isin(race_year_list))

# COMMAND ----------

from pyspark.sql.functions import sum, when, col, count

# COMMAND ----------

driver_standings_df = race_results_df.groupBy("driver_name", "race_year", "driver_nationality", "team") \
                                    .agg(
                                         sum("points").alias("total_points"), 
                                         count(when(col("position") == 1, True)).alias("wins")
)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import rank, desc

# COMMAND ----------

driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"))
result = driver_standings_df.withColumn("rank", rank().over(driver_rank_spec))

# COMMAND ----------

#result.write.parquet(path=f"{presentation_folder_path}/driver_standings", mode="overwrite")
#result.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.driver_standings")
overwrite_partition(result, "f1_presentation", "driver_standings", "race_year")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.driver_standings;

# COMMAND ----------

