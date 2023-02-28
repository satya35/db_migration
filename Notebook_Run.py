# Databricks notebook source
#os is for Miscellaneous operating system interfaces
#datetime is used to manipulate datetime values
import os
from datetime import datetime

def get_dir_content(pPath):
    for dir_path in dbutils.fs.ls(pPath):
        if dir_path.isFile():
            #os.stat gets statistics on a path. st_mtime gets the most recent content modification date time
            filename, extension = os.path.splitext(dir_path.path)
            #print(extension)
            if extension ==".scala":
                yield [os.path.basename(dir_path.path),dir_path.path,datetime.fromtimestamp(os.stat('/' + dir_path.path.replace(':','')).st_ctime), datetime.fromtimestamp(os.stat('/' + dir_path.path.replace(':','')).st_mtime)]
        elif dir_path.isDir() and pPath != dir_path.path:
            #if the path is a directory, call the function on it again to check its contents
            yield from get_dir_content(dir_path.path)

def get_latest_modified_file_from_directory(pDirectory):
    
    #Call get_dir_content to get a list of all files in this directory and the last modified date time of each
    vDirectoryContentsList = list(get_dir_content(pDirectory))

    df = spark.createDataFrame(vDirectoryContentsList,['FileName','FullFilePath','CreateDate','LastModifiedDateTime'])
    
    #return the file name that was last modifed in the given directory
    return df

# COMMAND ----------

# Default location for os commands is the local filesystem
import os

ls = os.listdir('/dbfs/dbfs/mnt/')

#to get all latest file
df = get_latest_modified_file_from_directory('/dbfs/mnt/')

for row_iterator in df.collect():
    notebook_name = "DataMigration/" + row_iterator['FileName'].replace("scala","")
    returned_table = dbutils.notebook.run(notebook_name, 60)
