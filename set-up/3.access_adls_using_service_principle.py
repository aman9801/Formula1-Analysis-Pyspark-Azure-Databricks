# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using Service Principle
# MAGIC 1. Register Azure AD Application/ Service Principle
# MAGIC 2. Generate a secret/password for the application
# MAGIC 3. Set Spark Config with App/ Client Id, Tenant Id & Secret
# MAGIC 4. Assign role 'Storage Blob Data Contributor' to the Data Lake

# COMMAND ----------

client_id = "8f30645e-65ef-4af3-b38b-21c40f6a8a01"
tenant_id = "11f13d5b-89c9-4c1d-a305-5958a69c2e07"
client_secret = "aAJ8Q~EeBGAQRXtaXE3daVPzaFKvB~otzE31-ddc"

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.auth.type.formula1dl9801.dfs.core.windows.net", 
    "OAuth"
)

spark.conf.set(
    "fs.azure.account.oauth.provider.type.formula1dl9801.dfs.core.windows.net", 
    "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
)

spark.conf.set(
    "fs.azure.account.oauth2.client.id.formula1dl9801.dfs.core.windows.net", 
    client_id
)

spark.conf.set(
    "fs.azure.account.oauth2.client.secret.formula1dl9801.dfs.core.windows.net", 
    client_secret
)

spark.conf.set(
    "fs.azure.account.oauth2.client.endpoint.formula1dl9801.dfs.core.windows.net", 
    f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dl9801.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dl9801.dfs.core.windows.net/circuits.csv"))