# Databricks notebook source
# MAGIC %run ./_utility-methods

# COMMAND ----------

DA = DBAcademyHelper()
DA.cleanup()
DA.init()

# Clean out the global_temp database.
for row in spark.sql("SHOW TABLES IN global_temp").select("tableName").collect():
    table_name = row[0]
    spark.sql(f"DROP TABLE global_temp.{table_name}")

# We shouldn't need this if the usage of the CSV file is truely read-only
# copy_source_dataset(f"{DA.paths.datasets}/flights/departuredelays.csv",
#                     f"{DA.paths.working_dir}/flight_delays",
#                     format="csv", name="flight_delays")

DA.conclude_setup()

