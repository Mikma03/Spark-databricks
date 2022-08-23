# Databricks notebook source
# MAGIC %run ../../Includes/_utility-methods

# COMMAND ----------

DA = DBAcademyHelper()
DA.cleanup()
DA.init(create_db=False)
DA.conclude_setup()

# COMMAND ----------

# TODO
my_name = None

# COMMAND ----------

example_df = spark.range(16)

