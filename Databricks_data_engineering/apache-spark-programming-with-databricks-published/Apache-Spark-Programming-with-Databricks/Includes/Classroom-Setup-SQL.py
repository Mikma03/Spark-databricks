# Databricks notebook source
# MAGIC %run ./Classroom-Setup

# COMMAND ----------

spark.sql(
    f"""CREATE TABLE IF NOT EXISTS events USING delta OPTIONS (path "{events_path}")"""
)
spark.sql(
    f"""CREATE TABLE IF NOT EXISTS sales USING delta OPTIONS (path "{sales_path}")"""
)
spark.sql(
    f"""CREATE TABLE IF NOT EXISTS users USING delta OPTIONS (path "{users_path}")"""
)
spark.sql(
    f"""CREATE TABLE IF NOT EXISTS products USING delta OPTIONS (path "{products_path}")"""
)

displayHTML("")

