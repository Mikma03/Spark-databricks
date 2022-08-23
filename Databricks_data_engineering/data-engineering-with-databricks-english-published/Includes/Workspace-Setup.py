# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Workspace Setup
# MAGIC This notebook should be run by instructors to prepare the workspace for a class.
# MAGIC 
# MAGIC The key changes this notebook makes includes:
# MAGIC * Updating user-specific grants such that they can create databases/schemas against the current catalog when they are not workspace-admins.
# MAGIC * Configures three cluster policies:
# MAGIC     * **Student's All-Purpose Policy** - which should be used on clusters running standard notebooks.
# MAGIC     * **Student's Jobs-Only Policy** - which should be used on workflows/jobs
# MAGIC     * **Student's DLT-Only Policy** - which should be used on DLT piplines (automatically applied)
# MAGIC * Create or update the shared **Starter Warehouse** for use in Databricks SQL exercises
# MAGIC * Create the Instance Pool **Student's Pool** for use by students and the "student" and "jobs" policies.

# COMMAND ----------

# MAGIC %run ./_utility-methods

# COMMAND ----------

# MAGIC %md
# MAGIC # Get Class Config
# MAGIC The two variables defined by these widgets are used to configure our environment as a means of controlling class cost.

# COMMAND ----------

# students_count is the reasonable estiamte to the maximum number of students
dbutils.widgets.text("students_count", "", "Number of Students")

# event_name is the name assigned to this event/class or alternatively its class number
dbutils.widgets.text("event_name", "", "Event Name/Class Number")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Init Script & Install Datasets
# MAGIC The main affect of this call is to pre-install the datasets.
# MAGIC 
# MAGIC It has the side effect of create our DA object which includes our REST client.

# COMMAND ----------

DA = DBAcademyHelper()     # Create the DA object with the specified lesson
DA.cleanup(validate=False) # Remove the existing database and files
DA.init(create_db=False)   # True is the default
DA.install_datasets()      # Install (if necissary) and validate the datasets
DA.conclude_setup()        # Conclude the setup by printing the DA object's final state

# COMMAND ----------

# Initilize the remaining parameters for this script
from dbacademy import dbgems

# Special logic for when we are running under test.
is_smoke_test = spark.conf.get("dbacademy.smoke-test", "false") == "true"

students_count = dbutils.widgets.get("students_count").strip()
user_count = len(DA.client.scim.users.list())

students_count = max(int(students_count), user_count) if students_count.isnumeric() else user_count
    
workspace = dbgems.get_browser_host_name() if dbgems.get_browser_host_name() else dbgems.get_notebooks_api_endpoint()
workspace = DA.clean_string(workspace)

org_id = dbgems.get_tag("orgId", "unknown")
org_id = DA.clean_string(org_id)

import math
autoscale_min = 1 if is_smoke_test else math.ceil(students_count/20)
autoscale_max = 1 if is_smoke_test else math.ceil(students_count/5)

event_name = "Smoke Test" if is_smoke_test else dbutils.widgets.get("event_name")
assert event_name is not None and len(event_name) >= 3, f"The parameter event_name must be specified with min-length of 3"
event_name = DA.clean_string(event_name)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Create Class Instance Pools
# MAGIC The following cell configures the instance pool used for this class

# COMMAND ----------

# Do not change these values.
tags = [("dbacademy.event_name",     DA.clean_string(event_name)),
        ("dbacademy.students_count", DA.clean_string(students_count)),
        ("dbacademy.workspace",      DA.clean_string(workspace)),
        ("dbacademy.org_id",         DA.clean_string(org_id))]

pool = DA.client.instance_pools.create_or_update(instance_pool_name = "Student's Pool",
                                                 idle_instance_autotermination_minutes = 15, 
                                                 min_idle_instances = 0,
                                                 tags = tags)
instance_pool_id = pool.get("instance_pool_id")

# With the pool created, make sure that all users can attach to it.
DA.client.permissions.pools.update_group(instance_pool_id, "users", "CAN_ATTACH_TO")

print(instance_pool_id)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Create The Three Class-Specific Cluster Policies
# MAGIC The following cells create the various cluster policies used by the class

# COMMAND ----------

policy = DA.client.cluster_policies.create_or_update("Student's All-Purpose Policy", {
    "cluster_type": {
        "type": "fixed",
        "value": "all-purpose"
    },
    "autotermination_minutes": {
        "type": "range",
        "minValue": 1,
        "maxValue": 120,
        "defaultValue": 120,
        "hidden": False
    },
    "spark_conf.spark.databricks.cluster.profile": {
        "type": "fixed",
        "value": "singleNode",
        "hidden": False
    },
    "instance_pool_id": {
        "type": "fixed",
        "value": instance_pool_id,
        "hidden": False
    }
})
policy_id = policy.get("policy_id")

# With the pool created, make sure that all users can attach to it.
DA.client.permissions.cluster_policies.update_group(policy_id, "users", "CAN_USE")

print(policy_id)

# COMMAND ----------

policy = DA.client.cluster_policies.create_or_update("Student's Jobs-Only Policy", {
    "cluster_type": {
        "type": "fixed",
        "value": "job"
    },
    "spark_conf.spark.databricks.cluster.profile": {
        "type": "fixed",
        "value": "singleNode",
        "hidden": False
    },
    "instance_pool_id": {
        "type": "fixed",
        "value": instance_pool_id,
        "hidden": False
    },
})
policy_id = policy.get("policy_id")

# With the pool created, make sure that all users can attach to it.
DA.client.permissions.cluster_policies.update_group(policy_id, "users", "CAN_USE")

print(policy_id)

# COMMAND ----------

policy = DA.client.cluster_policies.create_or_update("Student's DLT-Only Policy", {
    "cluster_type": {
        "type": "fixed",
        "value": "dlt"
    },
    "spark_conf.spark.databricks.cluster.profile": {
        "type": "fixed",
        "value": "singleNode",
        "hidden": False
    },
    "custom_tags.dbacademy.event_name": {
        "type": "fixed",
        "value": DA.clean_string(event_name),
        "hidden": False
    },
    "custom_tags.dbacademy.students_count": {
        "type": "fixed",
        "value": DA.clean_string(students_count),
        "hidden": False
    },
    "custom_tags.dbacademy.workspace": {
        "type": "fixed",
        "value": DA.clean_string(workspace),
        "hidden": False
    },
    "custom_tags.dbacademy.org_id": {
        "type": "fixed",
        "value": DA.clean_string(org_id),
        "hidden": False
    }
})

policy_id = policy.get("policy_id")

# With the pool created, make sure that all users can attach to it.
DA.client.permissions.cluster_policies.update_group(policy_id, "users", "CAN_USE")

print(policy_id)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Create Class-Shared Databricks SQL Warehouse/Endpoint
# MAGIC Creates a single wharehouse to be used by all students.
# MAGIC 
# MAGIC The configuration is derived from the number of students specified above.

# COMMAND ----------

from dbacademy.dbrest.sql.endpoints import RELIABILITY_OPTIMIZED, CHANNEL_NAME_CURRENT, CLUSTER_SIZE_2X_SMALL

warehouse = DA.client.sql.endpoints.create_or_update(
    name = "Starter Warehouse",
    cluster_size = CLUSTER_SIZE_2X_SMALL,
    enable_serverless_compute = False, # Due to a bug with Free-Trial workspaces
    min_num_clusters = autoscale_min,
    max_num_clusters = autoscale_max,
    auto_stop_mins = 120,
    enable_photon = True,
    spot_instance_policy = RELIABILITY_OPTIMIZED,
    channel = CHANNEL_NAME_CURRENT,
    tags = {
        "dbacademy.event_name":     DA.clean_string(event_name),
        "dbacademy.students_count": DA.clean_string(students_count),
        "dbacademy.workspace":      DA.clean_string(workspace),
        "dbacademy.org_id":         DA.clean_string(org_id)
    })

warehouse_id = warehouse.get("id")

# # With the warehouse created, make sure that all users can attach to it.
DA.client.permissions.warehouses.update_group(warehouse_id, "users", "CAN_USE")

print(warehouse_id)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Configure User Entitlements
# MAGIC 
# MAGIC This task simply adds the "**databricks-sql-access**" entitlement to the "**users**" group ensuring that they can access the Databricks SQL view.

# COMMAND ----------

group = DA.client.scim.groups.get_by_name("users")
DA.client.scim.groups.add_entitlement(group.get("id"), "databricks-sql-access")
None

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Update Grants
# MAGIC This operation executes **`GRANT CREATE ON CATALOG TO users`** to ensure that students can create databases as required by this course when they are not admins.
# MAGIC 
# MAGIC Note: The implementation requires this to execute in another job and as such can take about three minutes to complete.

# COMMAND ----------

# Ensures that all users can create databases on the current catalog 
# for cases wherein the user/student is not an admin.
DA.update_user_specific_grants()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
