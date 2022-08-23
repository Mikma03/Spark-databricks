# Databricks notebook source
# MAGIC %run ./_utility-methods

# COMMAND ----------

DA = DBAcademyHelper()     # Create the DA object with the specified lesson
DA.cleanup(validate=False) # Remove the existing database and files
DA.init()                  # Includes DB creation
DA.install_datasets()      # Install (if necissary) and validate the datasets
DA.conclude_setup()        # Conclude the setup by printing the DA object's final state

sqlContext.setConf("spark.sql.shuffle.partitions", spark.sparkContext.defaultParallelism)

