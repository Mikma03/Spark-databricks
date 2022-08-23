# Databricks notebook source
# MAGIC %run ./_utility-methods

# COMMAND ----------

# Continues where 8.1.1 picks up, don't remove assets
DA = DBAcademyHelper(lesson="dlt_demo_81")
# DA.cleanup()
DA.init()

DA.paths.stream_path = f"{DA.paths.working_dir}/stream"
DA.paths.storage_location = f"{DA.paths.working_dir}/storage"

DA.data_factory = DltDataFactory(DA.paths.stream_path)

DA.conclude_setup()

