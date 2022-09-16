# Databricks notebook source
from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

def add_ingestion_date(input_df: DataFrame):
    return input_df.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# Spark expect the partitioned columns to be the last the reason why we need this func
def re_arrange_partition_cols(input_df, partition_col):
    col_list = []
    for colname in input_df.schema.names:
        if colname != partition_col:
            col_list.append(colname)
    col_list.append(partition_col)
    return input_df.select(col_list)

# COMMAND ----------

# Parquet only
def overwrite_partition(input_df, db_name, table_name, partition_col):
    output_df = re_arrange_partition_cols(input_df, partition_col)
    spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
    if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
        output_df.write.mode("overwrite").insertInto(f"{db_name}.{table_name}")
    else:
        output_df.write.mode("overwrite").partitionBy(partition_col).format("parquet").saveAsTable(f"{db_name}.{table_name}")

# COMMAND ----------

def df_column_to_list(input_df, colname):
    df_row_list = input_df.select(colname).distinct().collect()
    col_value_list = [row[colname] for row in df_row_list]
    return col_value_list

# COMMAND ----------

def merge_delta_data(input_df, db_name, table_name, merge_condition, folder_path, partition_col):
    from delta.tables import DeltaTable
    spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning", "true")
    if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
        deltaTable = DeltaTable.forPath(spark, f"{folder_path}/{table_name}")
        deltaTable.alias("tgt").merge(input_df.alias("src"), merge_condition) \
                                     .whenMatchedUpdateAll() \
                                     .whenNotMatchedInsertAll() \
                                     .execute()
    else:
        input_df.write.mode("overwrite").partitionBy(partition_col).format("delta").saveAsTable(f"{db_name}.{table_name}")

# COMMAND ----------

