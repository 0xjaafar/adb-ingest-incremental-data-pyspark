# Databricks notebook source
# MAGIC %fs
# MAGIC ls

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.list("project-scope")

# COMMAND ----------

tenant_id            = dbutils.secrets.get(scope="project-scope", key="tenant-id")
adb_sp_client_id     = dbutils.secrets.get(scope="project-scope", key="client-id")
adb_sp_client_secret = dbutils.secrets.get(scope="project-scope", key="client-secret")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
       "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
       "fs.azure.account.oauth2.client.id": f"{adb_sp_client_id}",
       "fs.azure.account.oauth2.client.secret": f"{adb_sp_client_secret}",
       "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token",}

# COMMAND ----------

storage_account_name = "adbpipeadls"

# COMMAND ----------

def mount_adls(container_name: str):
    dbutils.fs.mount(
    source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
    mount_point = f"/mnt/{storage_account_name}/{container_name}",
    extra_configs = configs)

# COMMAND ----------

mount_adls("raw")

# COMMAND ----------

mount_adls("processed")

# COMMAND ----------

mount_adls("presentation")

# COMMAND ----------

mount_adls("demo")

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

dbutils.fs.ls("/mnt/adbpipeadls/raw")

# COMMAND ----------

# dbutils.fs.unmount('/mnt/adbtrainingsadls/raw')
# dbutils.fs.unmount('/mnt/adbtrainingsadls/processed')

# COMMAND ----------

