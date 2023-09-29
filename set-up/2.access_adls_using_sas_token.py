# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using SAS Token
# MAGIC 1. Set the spark config for SAS Token
# MAGIC 1. List files from demo container
# MAGIC 1. Read data from circuits.csv file

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.auth.type.formula1dl9801.dfs.core.windows.net", 
    "SAS"
)

spark.conf.set(
    "fs.azure.sas.token.provider.type.formula1dl9801.dfs.core.windows.net", 
    "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider"
)

spark.conf.set(
    "fs.azure.sas.fixed.token.formula1dl9801.dfs.core.windows.net", 
    "sp=rl&st=2023-09-28T15:41:55Z&se=2023-09-28T23:41:55Z&spr=https&sv=2022-11-02&sr=c&sig=x7VsOZ7%2Fivs3JYzB25YUJ5YUgnn1OxrUszhxZOD9En4%3D"
)


# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dl9801.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dl9801.dfs.core.windows.net/circuits.csv"))