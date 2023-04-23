# Databricks notebook source
# MAGIC %run ./Classroom-Setup

# COMMAND ----------

coupons_checkpoint_path = f"{working_dir}/coupon-sales/checkpoint"
coupons_output_path = f"{working_dir}/coupon-sales/output"

delta_events_path = f"{working_dir}/delta/events"
delta_sales_path = f"{working_dir}/delta/sales"
delta_users_path = f"{working_dir}/delta/users"

displayHTML("")

