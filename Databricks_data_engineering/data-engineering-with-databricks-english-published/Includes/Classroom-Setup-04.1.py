# Databricks notebook source
# MAGIC %run ./_utility-methods

# COMMAND ----------

DA = DBAcademyHelper()
DA.cleanup()
DA.init()

DA.paths.kafka_events = f"{DA.paths.datasets}/ecommerce/raw/events-kafka"

DA.conclude_setup()

