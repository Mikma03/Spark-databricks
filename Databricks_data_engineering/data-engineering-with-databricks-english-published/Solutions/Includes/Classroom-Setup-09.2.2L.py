# Databricks notebook source
# MAGIC %run ./_utility-methods

# COMMAND ----------

DA = DBAcademyHelper(lesson="jobs_lab_92")
# Don't reset our database or other assets
# DA.cleanup()
DA.init(create_db=False)

DA.paths.stream_path = f"{DA.paths.working_dir}/stream"
DA.data_factory = DltDataFactory(DA.paths.stream_path)

DA.conclude_setup()

