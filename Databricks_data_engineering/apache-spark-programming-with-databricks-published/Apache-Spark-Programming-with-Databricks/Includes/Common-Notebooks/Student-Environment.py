# Databricks notebook source
#############################################
# TAG API FUNCTIONS
#############################################

# Get all tags
def get_tags() -> dict:
  return sc._jvm.scala.collection.JavaConversions.mapAsJavaMap(
    dbutils.entry_point.getDbutils().notebook().getContext().tags()
  )

# Get a single tag's value
def get_tag(tag_name: str, default_value: str = None) -> str:
  values = get_tags()[tag_name]
  try:
    if len(values) > 0:
      return values
  except:
    return default_value

#############################################
# USER, USERNAME, AND USERHOME FUNCTIONS
#############################################

# Get the user's username
def get_username() -> str:
  import uuid
  try:
    return dbutils.widgets.get("databricksUsername")
  except:
    return get_tag("user", str(uuid.uuid1()).replace("-", ""))

# Get the user's userhome
def get_userhome() -> str:
  username = get_username()
  return "dbfs:/user/{}".format(username)

def get_module_name() -> str:
  return "aspwd"

def get_lesson_name() -> str:
  # If not specified, use the notebook's name.
  return dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None).split("/")[-1]

def get_working_dir() -> str:
  import re
  lesson_name = re.sub("[^a-zA-Z0-9]", "_", get_lesson_name())
  module_name = re.sub(r"[^a-zA-Z0-9]", "_", get_module_name())
  userhome = get_userhome()
  return f"{userhome}/dbacademy/{module_name}/{lesson_name}".replace("__", "_").replace("__", "_").replace("__", "_").replace("__", "_").lower()

def get_root_dir() -> str:
  import re
  module_name = re.sub(r"[^a-zA-Z0-9]", "_", get_module_name())
  userhome = get_userhome()
  return f"{userhome}/dbacademy/{module_name}".replace("__", "_").replace("__", "_").replace("__", "_").replace("__", "_").lower()

############################################
# USER DATABASE FUNCTIONS
############################################

def get_database_name(username:str, module_name:str, lesson_name:str) -> str:
  import re
  user = re.sub("[^a-zA-Z0-9]", "_", username)
  module = re.sub("[^a-zA-Z0-9]", "_", module_name)
  lesson = re.sub("[^a-zA-Z0-9]", "_", lesson_name)
  database_name = f"dbacademy_{user}_{module}_{lesson}".replace("__", "_").replace("__", "_").replace("__", "_").replace("__", "_").lower()
  return database_name


# Create a user-specific database
def create_user_database(username:str, module_name:str, lesson_name:str) -> str:
  database_name = get_database_name(username, module_name, lesson_name)

  spark.sql("CREATE DATABASE IF NOT EXISTS {}".format(database_name))
  spark.sql("USE {}".format(database_name))

  return database_name

# ****************************************************************************
# Utility method to determine whether a path exists
# ****************************************************************************

def path_exists(path):
  try:
    dbutils.fs.ls(path)
    return True
  except:
    return False

# ****************************************************************************
# Utility method for recursive deletes
# Note: dbutils.fs.rm() does not appear to be truely recursive
# ****************************************************************************

def delete_path(path):
  files = dbutils.fs.ls(path)

  for file in files:
    deleted = dbutils.fs.rm(file.path, True)

    if deleted == False:
      if file.is_dir:
        deletePath(file.path)
      else:
        raise IOError("Unable to delete file: " + file.path)

  if dbutils.fs.rm(path, True) == False:
    raise IOError("Unable to delete directory: " + path)

# ****************************************************************************
# Utility method to clean up the workspace at the end of a lesson
# ****************************************************************************

def classroom_cleanup(drop_database:bool = True):
  import time
  
  # Stop any active streams
  if len(spark.streams.active) > 0:
    print(f"Stopping {len(spark.streams.active)} streams")
    for stream in spark.streams.active:
      try: 
        stream.stop()
        stream.awaitTermination()
      except: pass # Bury any exceptions

  database = get_database_name(get_username(), get_module_name(), get_lesson_name())

  if drop_database:
    # The database should only be dropped in a "cleanup" notebook, not "setup"
    spark.sql(f"DROP DATABASE IF EXISTS {database} CASCADE")
    # In some rare cases the files don't actually get removed.
    dbutils.fs.rm(f"dbfs:/user/hive/warehouse/{database}.db", True)
    print(f"Dropped the database {database}")
    
  else:
      # Drop all tables from the specified database
      for row in spark.sql(f"show tables from {database}").select("tableName").collect():
        table_name = row["tableName"]
        spark.sql(f"DROP TABLE if exists {database}.{table_name}")

        # In some rare cases the files don't actually get removed.
        time.sleep(1) # Give it just a second...
        
        hive_path = f"dbfs:/user/hive/warehouse/{database}.db/{table_name}"
        dbutils.fs.rm(hive_path, True)

  # Remove any files that may have been created from previous runs
  if path_exists(get_working_dir()):
    delete_path(get_working_dir())
    print(f"Deleted the working directory {get_working_dir()}")


# Utility method to delete a database
def delete_tables(database):
  spark.sql("DROP DATABASE IF EXISTS {} CASCADE".format(database))

# ****************************************************************************
# Placeholder variables for coding challenge type specification
# ****************************************************************************
class FILL_IN:
  from pyspark.sql.types import Row, StructType
  VALUE = None
  LIST = []
  SCHEMA = StructType([])
  ROW = Row()
  INT = 0
  DATAFRAME = sqlContext.createDataFrame(sc.emptyRDD(), StructType([]))

############################################
# Set up student environment
############################################

module_name = get_module_name()
username = get_username()
lesson_name = get_lesson_name()
userhome = get_userhome()

working_dir = get_working_dir()
dbutils.fs.mkdirs(working_dir)

working_dir_root = get_root_dir()
datasets_dir = f"{working_dir_root}/datasets"
spark.conf.set("com.databricks.training.aspwd.datasetsDir", datasets_dir)
database_name = create_user_database(username, module_name, lesson_name)

classroom_cleanup(drop_database=False)

# COMMAND ----------

# %scala
# val datasetsDir = spark.conf.get("com.databricks.training.aspwd.datasetsDir")

