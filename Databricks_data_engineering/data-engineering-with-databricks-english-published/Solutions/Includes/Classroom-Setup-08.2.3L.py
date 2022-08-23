# Databricks notebook source
# MAGIC %run ./_utility-methods

# COMMAND ----------

DA = DBAcademyHelper(lesson="dlt_lab_82")
# Continuing from 8.2.3L, do not remove assets
# DA.cleanup()
DA.init()

DA.paths.stream_path = f"{DA.paths.working_dir}/stream"
DA.paths.storage_location = f"{DA.paths.working_dir}/storage"

DA.data_factory = DltDataFactory(DA.paths.stream_path)

DA.conclude_setup()

