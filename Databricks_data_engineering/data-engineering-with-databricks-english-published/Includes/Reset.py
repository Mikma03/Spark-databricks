# Databricks notebook source
# MAGIC %run ./_utility-methods

# COMMAND ----------

DA = DBAcademyHelper()
DA.init()

# COMMAND ----------

rows = spark.sql(f"show databases").collect()
for row in rows:
    db_name = row[0]
    if db_name.startswith(DA.db_name_prefix):
        print(db_name)
        spark.sql(f"DROP DATABASE {db_name} CASCADE")

# COMMAND ----------

if DA.paths.exists(DA.hidden.working_dir_root):
    print(f"Removing {DA.hidden.working_dir_root}")
    dbutils.fs.rm(DA.hidden.working_dir_root, True)

# COMMAND ----------

if DA.paths.exists(DA.paths.datasets):
    print(f"Removing {DA.paths.datasets}")
    dbutils.fs.rm(DA.paths.datasets, True)

