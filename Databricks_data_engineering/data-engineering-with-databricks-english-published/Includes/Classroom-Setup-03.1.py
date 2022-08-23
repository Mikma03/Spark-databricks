# Databricks notebook source
# MAGIC %run ./_utility-methods

# COMMAND ----------

DA = DBAcademyHelper()
DA.cleanup()
DA.init(create_db=False)

# I think the copy is unnecissary, especially if it is used as read-only 
# copy_source_dataset(f"{DA.paths.datasets}/flights/departuredelays.csv", f"{DA.paths.working_dir}/flights/departuredelays.csv", "csv", "flights")

DA.conclude_setup()

