# Databricks notebook source
# MAGIC %run ./_utility-methods

# COMMAND ----------

DA = DBAcademyHelper()
DA.cleanup()
DA.init()

print()
create_eltwss_users_update()
    
DA.conclude_setup()

