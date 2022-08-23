# Databricks notebook source
# MAGIC %run ./_utility-methods

# COMMAND ----------

DA = DBAcademyHelper(lesson="jobs_demo_91")
DA.cleanup()
DA.init()

DA.paths.stream_path = f"{DA.paths.working_dir}/stream"
DA.data_factory = DltDataFactory(DA.paths.stream_path)

DA.conclude_setup()

