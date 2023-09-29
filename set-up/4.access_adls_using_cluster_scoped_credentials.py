# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using Cluster Scoped Credentials
# MAGIC 1. Set the spark config fs.azure.account.key in the cluster
# MAGIC 1. List files from demo container
# MAGIC 1. Read data from circuits.csv file

# COMMAND ----------

''' Put this in Advanced setting of cluster
spark.conf.set(
    "fs.azure.account.key.formula1dl9801.dfs.core.windows.net", 
    "FU7N7qoMYr7/Glp53tEJnZRynVIYA/oTXdMOINpj07euXUc58L45G6ZP1fIxiAzwGmJBvto25P/G+AStxr1N0g=="
)
'''


# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dl9801.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dl9801.dfs.core.windows.net/circuits.csv"))