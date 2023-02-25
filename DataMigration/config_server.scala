// Databricks notebook source
import org.apache.spark.sql.functions._

// COMMAND ----------

// Get list of all scopes
val mysecrets = dbutils.secrets.listScopes()

// Loop through list
//mysecrets.foreach { println }

dbutils.secrets.get(scope = "my_secret_scope", key = "kv-secret-postgres-sql-con")

val url_postgres = dbutils.secrets.get(scope = "my_secret_scope", key = "kv-secret-postgres-sql-con")
val url_sql = dbutils.secrets.get(scope = "my_secret_scope", key = "kv-secret-azure-sql-con")

val driver = "org.postgresql.Driver"
//val table = "config.TableEntity"
//val database_host = "<database-host-url>"
//val database_port = "5432" # update if you use a non-default port
//val database_name = "<database-name>"
//val user = "<username>"
//val password = "<password>"

// COMMAND ----------

// MAGIC %python
// MAGIC # Python code to mount and access Azure Data Lake Storage Gen2 Account to Azure Databricks with Service Principal and 0Auth
// MAGIC 
// MAGIC # Define the variables used for creating connection strings
// MAGIC adlsAccountName = "myadlsgen2satya"
// MAGIC adlsContainerName = "input"
// MAGIC # adlsFolderName = ""
// MAGIC mountPoint = "/mnt/"
// MAGIC 
// MAGIC # Application (Client) ID
// MAGIC applicationId = dbutils.secrets.get(scope="my_secret_scope",key="ClientId")
// MAGIC 
// MAGIC # Application (Client) Secret Key
// MAGIC authenticationKey = dbutils.secrets.get(scope="my_secret_scope",key="Value")
// MAGIC 
// MAGIC # Directory (Tenant) ID
// MAGIC tenandId = dbutils.secrets.get(scope="my_secret_scope",key="TenantId")
// MAGIC 
// MAGIC endpoint = "https://login.microsoftonline.com/" + tenandId + "/oauth2/token"
// MAGIC source = "abfss://" + adlsContainerName + "@" + adlsAccountName + ".dfs.core.windows.net/"
// MAGIC 
// MAGIC # Connecting using Service Principal secrets and OAuth
// MAGIC configs = {"fs.azure.account.auth.type": "OAuth",
// MAGIC            "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
// MAGIC            "fs.azure.account.oauth2.client.id": applicationId,
// MAGIC            "fs.azure.account.oauth2.client.secret": authenticationKey,
// MAGIC            "fs.azure.account.oauth2.client.endpoint": endpoint}
// MAGIC 
// MAGIC # Mounting ADLS Storage to DBFS
// MAGIC 
// MAGIC # Mount only if the directory is not already mounted
// MAGIC if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
// MAGIC   dbutils.fs.mount(
// MAGIC     source = source,
// MAGIC     mount_point = mountPoint,
// MAGIC     extra_configs = configs)

// COMMAND ----------

// MAGIC %python
// MAGIC #os is for Miscellaneous operating system interfaces
// MAGIC #datetime is used to manipulate datetime values
// MAGIC import os
// MAGIC from datetime import datetime
// MAGIC 
// MAGIC def get_dir_content(pPath):
// MAGIC     for dir_path in dbutils.fs.ls(pPath):
// MAGIC         if dir_path.isFile():
// MAGIC             #os.stat gets statistics on a path. st_mtime gets the most recent content modification date time
// MAGIC             filename, extension = os.path.splitext(dir_path.path)
// MAGIC             #print(extension)
// MAGIC             if extension ==".csv":
// MAGIC                 yield [os.path.basename(dir_path.path),dir_path.path,datetime.fromtimestamp(os.stat('/' + dir_path.path.replace(':','')).st_ctime), datetime.fromtimestamp(os.stat('/' + dir_path.path.replace(':','')).st_mtime)]
// MAGIC         elif dir_path.isDir() and pPath != dir_path.path:
// MAGIC             #if the path is a directory, call the function on it again to check its contents
// MAGIC             yield from get_dir_content(dir_path.path)
// MAGIC 
// MAGIC def get_latest_modified_file_from_directory(pDirectory):
// MAGIC     
// MAGIC     #Call get_dir_content to get a list of all files in this directory and the last modified date time of each
// MAGIC     vDirectoryContentsList = list(get_dir_content(pDirectory))
// MAGIC 
// MAGIC     df = spark.createDataFrame(vDirectoryContentsList,['FileName','FullFilePath','CreateDate','LastModifiedDateTime'])
// MAGIC     
// MAGIC     #return the file name that was last modifed in the given directory
// MAGIC     return df

// COMMAND ----------

// MAGIC %python
// MAGIC #to get all latest file
// MAGIC df = get_latest_modified_file_from_directory('/mnt/' )
// MAGIC spark.catalog.dropTempView("df_FileList")
// MAGIC df.createTempView("df_FileList")
