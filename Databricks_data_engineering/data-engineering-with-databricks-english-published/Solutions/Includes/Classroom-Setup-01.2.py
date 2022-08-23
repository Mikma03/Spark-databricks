# Databricks notebook source
# MAGIC %run ./_utility-methods

# COMMAND ----------

def create_demo_tmp_vw(self):
    print("Creating the temp view demo_tmp_vw")

    spark.sql("""
        CREATE OR REPLACE TEMP VIEW demo_tmp_vw(name, value) AS VALUES
        ("Yi", 1),
        ("Ali", 2),
        ("Selina", 3)
        """)

DBAcademyHelper.monkey_patch(create_demo_tmp_vw)

# COMMAND ----------

DA = DBAcademyHelper()      # Create the DA object with the specified lesson
DA.cleanup(validate=False)  # Remove the existing database and files
DA.init(create_db=False)
DA.create_demo_tmp_vw()
DA.conclude_setup()

