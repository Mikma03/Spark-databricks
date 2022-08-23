# Databricks notebook source
# MAGIC %run ./_utility-methods

# COMMAND ----------

DA = DBAcademyHelper()
DA.cleanup()
DA.init(create_db=False)
DA.conclude_setup()

