# Databricks notebook source
# MAGIC %run ./_utility-methods

# COMMAND ----------

DA = DBAcademyHelper()
DA.cleanup()
DA.init()

print()

clone_source_table("sales", f"{DA.paths.datasets}/ecommerce/delta", "sales_hist")
# clone_source_table("users", f"{DA.paths.datasets}/ecommerce/delta", "users_hist")
clone_source_table("events", f"{DA.paths.datasets}/ecommerce/delta", "events_hist")

# clone_source_table("users_update", f"{DA.paths.datasets}/ecommerce/delta")
clone_source_table("events_update", f"{DA.paths.datasets}/ecommerce/delta")

clone_source_table("events_raw", f"{DA.paths.datasets}/ecommerce/delta")
clone_source_table("item_lookup", f"{DA.paths.datasets}/ecommerce/delta")
    
DA.conclude_setup()

