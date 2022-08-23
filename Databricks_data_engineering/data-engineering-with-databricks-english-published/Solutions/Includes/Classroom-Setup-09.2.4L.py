# Databricks notebook source
# MAGIC %run ./_utility-methods

# COMMAND ----------

DA = DBAcademyHelper(lesson="jobs_lab_92")
# Don't reset our database or other assets
# DA.cleanup()
DA.init(create_db=False)
DA.conclude_setup()

