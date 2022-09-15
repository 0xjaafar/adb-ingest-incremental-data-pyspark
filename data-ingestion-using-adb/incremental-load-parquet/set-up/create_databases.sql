-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_raw
LOCATION "/mnt/adbpipeadls/raw"

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "/mnt/adbpipeadls/processed"

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_presentation
LOCATION "/mnt/adbpipeadls/presentation"

-- COMMAND ----------

