# Databricks notebook source
# MAGIC %run ./_utility-methods

# COMMAND ----------

DA = DBAcademyHelper()
DA.cleanup()
DA.init(create_db=False)

# I think the copy is unnecissary, especially if it is used as read-only 
# copy_source_dataset(f"{DA.paths.datasets}/weather/data", f"{DA.paths.working_dir}/weather", "parquet", "weather")

DA.conclude_setup()

