# Databricks notebook source
# MAGIC %run ./Classroom-Setup

# COMMAND ----------

coupons_checkpoint_path = working_dir + "/coupon-sales/checkpoint"
coupons_output_path = working_dir + "/coupon-sales/output"

delta_events_path = working_dir + "/delta/events"
delta_sales_path = working_dir + "/delta/sales"
delta_users_path = working_dir + "/delta/users"

displayHTML("")

