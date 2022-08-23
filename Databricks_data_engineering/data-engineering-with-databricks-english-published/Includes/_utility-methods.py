# Databricks notebook source
# MAGIC %pip install \
# MAGIC git+https://github.com/databricks-academy/dbacademy-gems@c3032c2df47472f1600d368523f052d2920b406d \
# MAGIC git+https://github.com/databricks-academy/dbacademy-rest@e729b6dbb566de2958cba60fe4bd50e1b9e7f25b \
# MAGIC git+https://github.com/databricks-academy/dbacademy-helper@fd1619a8b6f22adb3b7b54e2897cbdc5c3f161a4 \
# MAGIC --quiet --disable-pip-version-check

# COMMAND ----------

# MAGIC %run ./_remote_files

# COMMAND ----------

from dbacademy_gems import dbgems

# COMMAND ----------

# The following attributes are externalized to make them easy
# for content developers to update with every new course.

_course_code = "dewd"
_naming_params = {"course": _course_code}
_course_name = "data-engineering-with-databricks"
_data_source_name = _course_name
_data_source_version = "v02"

_min_time = "5 min"  # The minimum amount of time to install the datasets (e.g. from Oregon)
_max_time = "15 min" # The maximum amount of time to install the datasets (e.g. from India)

# Set to true only if this course uses streaming APIs which 
# in turn creates a checkpoints path for all checkpoint files.
_enable_streaming_support = True 

# COMMAND ----------

class Paths():
    def __init__(self, working_dir, clean_lesson):
        global _enable_streaming_support
        
        self.working_dir = working_dir

        # The location of the user's database - presumes all courses have at least one DB.
        # The location of the database varies if we have a database per lesson or one database per course
        if clean_lesson: self.user_db = f"{working_dir}/{clean_lesson}.db"
        else:            self.user_db = f"{working_dir}/database.db"

        # When working with streams, it helps to put all checkpoints in their
        # own directory relative the the previously defined working_dir
        if _enable_streaming_support:
            self.checkpoints = f"{working_dir}/_checkpoints"    
            
    @staticmethod
    def exists(path):
        """
        Returns true if the specified path exists else false.
        """
        try: return len(dbutils.fs.ls(path)) >= 0
        except Exception:return False

    def print(self, padding="  ", self_name="self."):
        """
        Prints all the pathes attached to this instance of Paths
        """
        max_key_len = 0
        for key in self.__dict__: 
            max_key_len = len(key) if len(key) > max_key_len else max_key_len
        for key in self.__dict__:
            label = f"{padding}{self_name}paths.{key}: "
            print(label.ljust(max_key_len+13) + self.__dict__[key])
        
    def __repr__(self):
        return self.__dict__.__repr__().replace(", ", ",\n").replace("{","").replace("}","").replace("'","")


# COMMAND ----------

class DBAcademyHelper():
    def __init__(self, lesson=None, asynchronous=True):
        import re, time
        from dbacademy import dbgems 
        from dbacademy.dbrest import DBAcademyRestClient

        self.start = int(time.time())
        
        # Intialize from our global variables defined at the top of the notebook
        global _course_code, _course_name, _data_source_name, _data_source_version, _naming_params, _remote_files
        self.course_code = _course_code
        self.course_name = _course_name
        self.remote_files = _remote_files
        self.naming_params = _naming_params
        self.data_source_name = _data_source_name
        self.data_source_version = _data_source_version
        
        self.client = DBAcademyRestClient()

        # Are we running under test? If so we can "optimize" for parallel execution 
        # without affecting the student's runtime-experience. As in the student can
        # use one working directory and one database, but under test, we can use many
        is_smoke_test = (spark.conf.get("dbacademy.smoke-test", "false").lower() == "true")
        
        if lesson is None and asynchronous and is_smoke_test:
            # The developer did not define a lesson, we can run asynchronous, and this 
            # is a smoke test so we can define a lesson here for the sake of testing
            lesson = str(abs(hash(dbgems.get_notebook_path())) % 10000)
            
        self.lesson = None if lesson is None else lesson.lower()
        
        # Define username using the hive function (cleaner than notebooks API)
        self.username = spark.sql("SELECT current_user()").first()[0]

        # Create the database name prefix according to curriculum standards. This
        # is the value by which all databases in this course should start with.
        # Besides creating this lesson's database name, this value is used almost
        # exclusively in the Rest notebook.
        da_name, da_hash = self.get_username_hash()
        self.db_name_prefix = f"da-{da_name}@{da_hash}-{self.course_code}"      # Composite all the values to create the "dirty" database name
        self.db_name_prefix = re.sub("[^a-zA-Z0-9]", "_", self.db_name_prefix)  # Replace all special characters with underscores (not digit or alpha)
        while "__" in self.db_name_prefix: 
            self.db_name_prefix = self.db_name_prefix.replace("__", "_")        # Replace all double underscores with single underscores
        
        # This is the common super-directory for each lesson, removal of which 
        # is designed to ensure that all assets created by students is removed.
        # As such, it is not attached to the path object so as to hide it from 
        # students. Used almost exclusively in the Rest notebook.
        working_dir_root = f"dbfs:/mnt/dbacademy-users/{self.username}/{self.course_name}"

        if self.lesson is None:
            self.clean_lesson = None
            working_dir = working_dir_root         # No lesson, working dir is same as root
            self.paths = Paths(working_dir, None)  # Create the "visible" path
            self.hidden = Paths(working_dir, None) # Create the "hidden" path
            self.db_name = self.db_name_prefix     # No lesson, database name is the same as prefix
        else:
            working_dir = f"{working_dir_root}/{self.lesson}"                     # Working directory now includes the lesson name
            self.clean_lesson = re.sub("[^a-zA-Z0-9]", "_", self.lesson.lower())  # Replace all special characters with underscores
            self.paths = Paths(working_dir, self.clean_lesson)                    # Create the "visible" path
            self.hidden = Paths(working_dir, self.clean_lesson)                   # Create the "hidden" path
            self.db_name = f"{self.db_name_prefix}_{self.clean_lesson}"           # Database name includes the lesson name

        # Register the working directory root to the hidden set of paths
        self.hidden.working_dir_root = working_dir_root

        # This is where the datasets will be downloaded to and should be treated as read-only for all pratical purposes
        self.paths.datasets = f"dbfs:/mnt/dbacademy-datasets/{self.data_source_name}/{self.data_source_version}"
        
        # This is the location in our Azure data repository of the datasets for this lesson
        self.data_source_uri = f"wasbs://courseware@dbacademy.blob.core.windows.net/{self.data_source_name}/{self.data_source_version}"
    
    def get_username_hash(self, name=None):
        """
        Utility method to split the user's email address, dropping the domain, and then creating a hash based on the full email address and course_code. The primary usage of this function is in creating the user's database, but is also used in creating SQL Endpoints, DLT Piplines, etc - any place we need a short, student-specific name.
        """
        name = name if name is not None else self.username
        da_name = name.split("@")[0]                                   # Split the username, dropping the domain
        da_hash = abs(hash(f"{name}-{self.course_code}")) % 10000      # Create a has from the full username and course code
        return da_name, da_hash

    @staticmethod
    def monkey_patch(function_ref, delete=True):
        """
        This function "monkey patches" the specified function to the DBAcademyHelper class. While not 100% necissary, this pattern does allow each function to be defined in it's own cell which makes authoring notebooks a little bit easier.
        """
        import inspect
        
        signature = inspect.signature(function_ref)
        assert "self" in signature.parameters, f"""Missing the required parameter "self" in the function "{function_ref.__name__}()" """
        
        setattr(DBAcademyHelper, function_ref.__name__, function_ref)
        if delete: del function_ref        

    @staticmethod
    def is_smoke_test():
        """
        Helper method to indentify when we are running as a smoke test
        :return: Returns true if the notebook is running as a smoke test.
        """
        return dbgems.get_spark_session().conf.get("dbacademy.smoke-test", "false").lower() == "true"


# COMMAND ----------

def init(self, install_datasets=True, create_db=True):
    """
    This function aims to setup the invironment enabling the constructor to provide initialization of attributes only and thus not modifying the environment upon initialization.
    """

    spark.catalog.clearCache() # Clear the any cached values from previus lessons
    self.create_db = create_db # Flag to indicate if we are creating the database or not

    if install_datasets:
        self.install_datasets()
    
    if create_db:
        print(f"\nCreating the database \"{self.db_name}\"")
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.db_name} LOCATION '{self.paths.user_db}'")
        spark.sql(f"USE {self.db_name}")

DBAcademyHelper.monkey_patch(init)

# COMMAND ----------

def cleanup(self, validate=True):
    """
    Cleans up the user environment by stopping any active streams, dropping the database created by the call to init() and removing the user's lesson-specific working directory and any assets created in that directory.
    """

    for stream in spark.streams.active:
        print(f"Stopping the stream \"{stream.name}\"")
        stream.stop()
        try: stream.awaitTermination()
        except: pass # Bury any exceptions

    if spark.sql(f"SHOW DATABASES").filter(f"databaseName == '{self.db_name}'").count() == 1:
        print(f"Dropping the database \"{self.db_name}\"")
        spark.sql(f"DROP DATABASE {self.db_name} CASCADE")

    if self.paths.exists(self.paths.working_dir):
        print(f"Removing the working directory \"{self.paths.working_dir}\"")
        dbutils.fs.rm(self.paths.working_dir, True)

    if validate:
        print()
        self.validate_datasets(fail_fast=False)
        
DBAcademyHelper.monkey_patch(cleanup)

# COMMAND ----------

def conclude_setup(self):
    """
    Concludes the setup of DBAcademyHelper by advertising to the student the new state of the environment such as predefined path variables, databases and tables created on behalf of the student and the total setup time. Additionally, all path attributes are pushed to the Spark context for reference in SQL statements.
    """

    import time

    # Inject the user's database name
    # Add custom attributes to the SQL context here.
    spark.conf.set("da.db_name", self.db_name)
    spark.conf.set("DA.db_name", self.db_name)
    
    # Automatically add all path attributes to the SQL context as well.
    for key in self.paths.__dict__:
        spark.conf.set(f"da.paths.{key.lower()}", self.paths.__dict__[key])
        spark.conf.set(f"DA.paths.{key.lower()}", self.paths.__dict__[key])

    print("\nPredefined Paths:")
    self.paths.print(self_name="DA.")

    if self.create_db:
        print(f"\nPredefined tables in {self.db_name}:")
        tables = spark.sql(f"SHOW TABLES IN {self.db_name}").filter("isTemporary == false").select("tableName").collect()
        if len(tables) == 0: print("  -none-")
        for row in tables: print(f"  {row[0]}")

    print(f"\nSetup completed in {int(time.time())-self.start} seconds")

DBAcademyHelper.monkey_patch(conclude_setup)

# COMMAND ----------

def block_until_stream_is_ready(self, query, min_batches=2):
    """
    A utility method used in streaming notebooks to block until the stream has processed n batches. This method serves one main purpose in two different usescase
    
    The purpose is to block the current command until the state of the stream is ready and thus allowing the next command to execute against the properly initialized stream.
    
    The first use case is in jobs where where the stream is started in one cell but execution of subsequent cells start prematurely.
    
    The second use case is to slow down students who likewise attempt to execute subsequent cells before the stream is in a valid state either by invoking subsequent cells directly or by execute the Run-All Command
        
    Note: it is best to show the students this code the first time so that they understand what it is doing and why, but from that point forward, just call it via the DA object.
    """
    import time
    while len(query.recentProgress) < min_batches:
        time.sleep(5) # Give it a couple of seconds

    print(f"The stream has processed {len(query.recentProgress)} batchs")
    
DBAcademyHelper.monkey_patch(block_until_stream_is_ready)

# COMMAND ----------

def install_datasets(self, reinstall=False, repairing=False, verbose=False):
    """
    Install the datasets used by this course to DBFS.
    
    This ensures that data and compute are in the same region which subsequently mitigates performance issues when the storage and compute are, for example, on opposite sides of the world.
    """
    import time

    global _min_time, _max_time
    min_time = _min_time
    max_time = _max_time

    if not repairing: print(f"\nThe source for the datasets is\n{self.data_source_uri}/")

    if not repairing: print(f"\nYour local dataset directory is {self.paths.datasets}")
    existing = self.paths.exists(self.paths.datasets)

    if not reinstall and existing:
        print(f"\nSkipping install of existing dataset.")
        print()
        self.validate_datasets(fail_fast=False)
        # self.enumerate_copyrights(verbose=verbose)
        return 

    # Remove old versions of the previously installed datasets
    if existing:
        print(f"\nRemoving previously installed datasets from{self.paths.datasets}")
        dbutils.fs.rm(self.paths.datasets, True)

    print(f"""\nInstalling the datasets to {self.paths.datasets}""")

    print(f"""\nNOTE: The datasets that we are installing are located in Washington, USA - depending on the
          region that your workspace is in, this operation can take as little as {min_time} and 
          upwards to {max_time}, but this is a one-time operation.""")

    # Using data_source_uri is a temporary hack because it assumes we can actually 
    # reach the remote repository - in cases where it's blocked, this will fail.
    files = dbutils.fs.ls(self.data_source_uri)
    
    what = "dataset" if len(files) == 1 else "datasets"
    print(f"\nInstalling {len(files)} {what}: ")
    
    install_start = int(time.time())
    for f in files:
        start = int(time.time())
        print(f"Copying /{f.name[:-1]}", end="...")

        source_path = f"{self.data_source_uri}/{f.name}"
        target_path = f"{self.paths.datasets}/{f.name}"
        
        dbutils.fs.cp(source_path, target_path, True)
        print(f"({int(time.time())-start} seconds)")
        
    print()
    self.validate_datasets(fail_fast=True)
    # self.enumerate_copyrights(verbose=verbose)

    print(f"""\nThe install of the datasets completed successfully in {int(time.time())-install_start} seconds.""")  

DBAcademyHelper.monkey_patch(install_datasets)

# COMMAND ----------

# def enumerate_copyrights(self, verbose):
#     if not verbose:
#         return
    
#     print("\nCopyright Information:")
#     datasets = dbutils.fs.ls(DA.paths.datasets)
    
#     max_len = 0
#     for dataset in datasets: 
#         dataset_name = dataset.path.split("/")[-2]
#         max_len = len(dataset_name) if len(dataset_name) > max_len else max_len
        
#     for dataset in datasets:
#         dataset_name = dataset.path.split("/")[-2]
#         label = dataset_name.ljust(max_len)+":"
#         try:
#             readme_path = f"{dataset.path}README.md"
#             dbutils.fs.ls(readme_path)
#             print(f"...{label} {readme_path}")
#         except:
#             print(f"...{label} MISSING COPYRIGHT FILE")
    
# DBAcademyHelper.monkey_patch(enumerate_copyrights)

# COMMAND ----------

def print_copyrights(self):
    datasets = [f.path for f in dbutils.fs.ls(DA.paths.datasets)]
    for dataset in datasets:
        readme_path = f"{dataset}README.md"
        try:
            head = dbutils.fs.head(readme_path)
            lines = len(head.split("\n"))+1
            html = f"""<html><body><h1>{dataset}</h1><textarea rows="{lines}" style="width:100%; overflow-x:scroll">{head}</textarea></body></html>"""
            displayHTML(html)
        except:
            print(f"\nMISSING: {readme_path}")
        
DBAcademyHelper.monkey_patch(print_copyrights)

# COMMAND ----------

def list_r(self, path, prefix=None, results=None):
    """
    Utility method used by the dataset validation, this method performs a recursive list of the specified path and returns the sorted list of paths.
    """
    if prefix is None: prefix = path
    if results is None: results = list()
    
    try: files = dbutils.fs.ls(path)
    except: files = []
    
    for file in files:
        data = file.path[len(prefix):]
        results.append(data)
        if file.isDir(): 
            self.list_r(file.path, prefix, results)
        
    results.sort()
    return results

DBAcademyHelper.monkey_patch(list_r)

# COMMAND ----------

def enumerate_remote_datasets(self):
    """
    Development function used to enumerate the remote datasets for use in validate_datasets()
    """
    files = self.list_r(self.data_source_uri)
    files = "_remote_files = " + str(files).replace("'", "\"")
    
    displayHTML(f"""
        <p>Copy the following output and paste it in its entirety into cell of the _utility-functions notebook.</p>
        <textarea rows="10" style="width:100%">{files}</textarea>
    """)

DBAcademyHelper.monkey_patch(enumerate_remote_datasets)

# COMMAND ----------

def enumerate_local_datasets(self):
    """
    Development function used to enumerate the local datasets for use in validate_datasets()
    """
    files = self.list_r(self.paths.datasets)
    files = "_remote_files = " + str(files).replace("'", "\"")
    
    displayHTML(f"""
        <p>Copy the following output and paste it in its entirety into cell of the _utility-functions notebook.</p>
        <textarea rows="10" style="width:100%">{files}</textarea>
    """)

DBAcademyHelper.monkey_patch(enumerate_local_datasets)

# COMMAND ----------

def do_validate(self):
    """
    Utility method to compare local datasets to the registered list of remote files.
    """
    
    local_files = self.list_r(self.paths.datasets)
    
    for file in local_files:
        if file not in self.remote_files:
            print(f"\n  - Found extra file: {file}")
            print(f"  - This problem can be fixed by reinstalling the datasets")
            return False

    for file in self.remote_files:
        if file not in local_files:
            print(f"\n  - Missing file: {file}")
            print(f"  - This problem can be fixed by reinstalling the datasets")
            return False
        
    return True

DBAcademyHelper.monkey_patch(do_validate)

# COMMAND ----------

def validate_datasets(self, fail_fast:bool):
    """
    Validates the "install" of the datasets by recursively listing all files in the remote data repository as well as the local data repository, validating that each file exists but DOES NOT validate file size or checksum.
    """
    import time
    start = int(time.time())
    print(f"Validating the local copy of the datsets", end="...")
    
    result = self.do_validate()
    print(f"({int(time.time())-start} seconds)")
    
    if not result:
        if fail_fast:
            raise Exception("Validation failed - see previous messages for more information.")
        else:
            print("\nAttempting to repair local dataset...\n")
            self.install_datasets(reinstall=True, repairing=True)

DBAcademyHelper.monkey_patch(validate_datasets)

# COMMAND ----------

def clean_string(self, value):
    import re
    value = re.sub("[^a-zA-Z0-9]", "_", str(value))
    while "__" in value: value = value.replace("__", "_")
    return value
  
DBAcademyHelper.monkey_patch(clean_string)

# COMMAND ----------

def update_user_specific_grants(self):
    from dbacademy import dbgems
    from dbacademy.dbrest import DBAcademyRestClient
    
    job_name = f"DA-{DA.course_code}-Configure-Permissions"
    DA.client.jobs().delete_by_name(job_name, success_only=False)

    notebook_path = f"{dbgems.get_notebook_dir()}/Configure-Permissions"

    params = {
        "name": job_name,
        "tags": {
            "dbacademy.course": self.clean_string(DA.course_name),
            "dbacademy.source": self.clean_string(DA.course_name)
        },
        "email_notifications": {},
        "timeout_seconds": 7200,
        "max_concurrent_runs": 1,
        "format": "MULTI_TASK",
        "tasks": [
            {
                "task_key": "Configure-Permissions",
                "description": "Configure all users's permissions for user-specific databases.",
                "libraries": [],
                "notebook_task": {
                    "notebook_path": notebook_path,
                    "base_parameters": []
                },
                "new_cluster": {
                    "num_workers": 0,
                    "cluster_name": "",
                    "spark_conf": {
                        "spark.master": "local[*]",
                        "spark.databricks.acl.dfAclsEnabled": "true",
                        "spark.databricks.repl.allowedLanguages": "sql,python",
                        "spark.databricks.cluster.profile": "serverless"             
                    },
                    "runtime_engine": "STANDARD",
                    "spark_env_vars": {
                        "WSFS_ENABLE_WRITE_SUPPORT": "true"
                    },
                },
            },
        ],
    }
    cluster_params = params.get("tasks")[0].get("new_cluster") # This doesn't work with Photon for some reason.
    cluster_params["spark_version"] = DA.client.clusters().get_current_spark_version().replace("-photon-", "-")
    
    
    if DA.client.clusters().get_current_instance_pool_id() is not None:
        cluster_params["instance_pool_id"] = DA.client.clusters().get_current_instance_pool_id()
    else:
        cluster_params["node_type_id"] = DA.client.clusters().get_current_node_type_id()
               
    create_response = DA.client.jobs().create(params)
    job_id = create_response.get("job_id")

    run_response = DA.client.jobs().run_now(job_id)
    run_id = run_response.get("run_id")

    final_response = DA.client.runs().wait_for(run_id)
    
    final_state = final_response.get("state").get("result_state")
    assert final_state == "SUCCESS", f"Expected the final state to be SUCCESS, found {final_state}"
    
    DA.client.jobs().delete_by_name(job_name, success_only=False)
    
    print()
    print("Update completed successfully.")

DBAcademyHelper.monkey_patch(update_user_specific_grants)

# COMMAND ----------

@DBAcademyHelper.monkey_patch
def update_cluster_params(self, params: dict, task_indexes: list):

    if not self.is_smoke_test():
        return params
    
    for task_index in task_indexes:
        # Need to modify the parameters to run run as a smoke-test.
        task = params.get("tasks")[task_index]
        del task["existing_cluster_id"]

        cluster_params =         {
            "num_workers": "0",
            "spark_version": self.client.clusters().get_current_spark_version(),
            "spark_conf": {
              "spark.master": "local[*]"
            },
        }

        instance_pool_id = self.client.clusters().get_current_instance_pool_id()
        if instance_pool_id is not None: cluster_params["instance_pool_id"] = self.client.clusters().get_current_instance_pool_id()
        else:                            cluster_params["node_type_id"] = self.client.clusters().get_current_node_type_id()

        task["new_cluster"] = cluster_params
        
    return params


# COMMAND ----------

# def install_source_dataset(source_uri, reinstall, subdir):
#     target_dir = f"{DA.working_dir_prefix}/source/{subdir}"

# #     if reinstall and DA.paths.exists(target_dir):
# #         print(f"Removing existing dataset at {target_dir}")
# #         dbutils.fs.rm(target_dir, True)
    
#     if DA.paths.exists(target_dir):
#         print(f"Skipping install to \"{target_dir}\", dataset already exists")
#     else:
#         print(f"Installing datasets to \"{target_dir}\"")
#         dbutils.fs.cp(source_uri, target_dir, True)
        
#     return target_dir

# COMMAND ----------

# def install_eltwss_datasets(reinstall):
#     source_uri = "wasbs://courseware@dbacademy.blob.core.windows.net/elt-with-spark-sql/v02/small-datasets"
#     DA.paths.datasets = install_source_dataset(source_uri, reinstall, "eltwss")

# COMMAND ----------

def clone_source_table(table_name, source_path, source_name=None):
    import time
    start = int(time.time())

    source_name = table_name if source_name is None else source_name
    print(f"Cloning the {table_name} table from {source_path}/{source_name}", end="...")
    
    spark.sql(f"""
        CREATE OR REPLACE TABLE {table_name}
        SHALLOW CLONE delta.`{source_path}/{source_name}`
        """)

    total = spark.read.table(table_name).count()
    print(f"({int(time.time())-start} seconds / {total:,} records)")

# COMMAND ----------

def load_eltwss_tables():
    clone_source_table("events", f"{DA.paths.datasets}/ecommerce/delta")
    clone_source_table("sales", f"{DA.paths.datasets}/ecommerce/delta")
    clone_source_table("users", f"{DA.paths.datasets}/ecommerce/delta")
    clone_source_table("transactions", f"{DA.paths.datasets}/ecommerce/delta")

# COMMAND ----------

def copy_source_dataset(src_path, dst_path, format, name):
    import time
    start = int(time.time())
    print(f"Creating the {name} dataset", end="...")
    
    dbutils.fs.cp(src_path, dst_path, True)

    total = spark.read.format(format).load(dst_path).count()
    print(f"({int(time.time())-start} seconds / {total:,} records)")


# COMMAND ----------

# lesson: Writing delta 
def create_eltwss_users_update():
    import time
    start = int(time.time())
    print(f"Creating the users_dirty table", end="...")

    # REFACTORING - Making lesson-specific copy
#     dbutils.fs.cp(f"{DA.paths.datasets}/ecommerce/raw/users-30m",
#                   f"{DA.paths.working_dir}/users-30m", True)
    
    spark.sql(f"""
        CREATE OR REPLACE TABLE users_dirty AS
        SELECT *, current_timestamp() updated 
        FROM parquet.`{DA.paths.datasets}/ecommerce/raw/users-30m`
    """)
    
    spark.sql("INSERT INTO users_dirty VALUES (NULL, NULL, NULL, NULL), (NULL, NULL, NULL, NULL), (NULL, NULL, NULL, NULL)")
    
    total = spark.read.table("users_dirty").count()
    print(f"({int(time.time())-start} seconds / {total:,} records)")

# COMMAND ----------

class DltDataFactory:
    def __init__(self, stream_path):
        self.stream_path = stream_path
        self.source = f"{DA.paths.datasets}/healthcare/tracker/streaming"
        try:
            self.curr_mo = 1 + int(max([x[1].split(".")[0] for x in dbutils.fs.ls(self.stream_path)]))
        except:
            self.curr_mo = 1
    
    def load(self, continuous=False):
        if self.curr_mo > 12:
            print("Data source exhausted\n")
        elif continuous == True:
            while self.curr_mo <= 12:
                curr_file = f"{self.curr_mo:02}.json"
                target_dir = f"{self.stream_path}/{curr_file}"
                print(f"Loading the file {curr_file} to the {target_dir}")
                dbutils.fs.cp(f"{self.source}/{curr_file}", target_dir)
                self.curr_mo += 1
        else:
            curr_file = f"{str(self.curr_mo).zfill(2)}.json"
            target_dir = f"{self.stream_path}/{curr_file}"
            print(f"Loading the file {curr_file} to the {target_dir}")

            dbutils.fs.cp(f"{self.source}/{curr_file}", target_dir)
            self.curr_mo += 1

