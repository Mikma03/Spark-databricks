# Databricks notebook source
# MAGIC %run ./_utility-methods

# COMMAND ----------

from dbacademy import dbgems

DA = DBAcademyHelper()

if dbgems.is_job():
    # Only execute this whenunder test.
    DA.cleanup()
    DA.init(create_db=True)

    print("Mocking global temp view")
    spark.sql(f"""USE {DA.db_name}""")
    spark.sql(f"""CREATE TABLE external_table USING CSV OPTIONS (path='{DA.paths.datasets}/flights/departuredelays.csv', header="true", mode="FAILFAST"
);""")
    spark.sql("""CREATE OR REPLACE GLOBAL TEMPORARY VIEW global_temp_view_dist_gt_1000 AS SELECT * FROM external_table WHERE distance > 1000;""")
    
else:
    DA.init(create_db=False)
    
print()
DA.conclude_setup()


