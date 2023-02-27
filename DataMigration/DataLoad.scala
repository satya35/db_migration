// Databricks notebook source
// MAGIC %run ./job_run

// COMMAND ----------

// MAGIC %run ./transformation

// COMMAND ----------

//to get all tables details to load
var df_TableEntity = get_load_entity()

var df = df_TableEntity.filter(df_TableEntity("isactive") === true)

//get all file path
var df_FileList = spark.sql("select * from df_FileList")
  
for (row <- df.rdd.collect)
{   

    var sourcesystem = row.get(row.fieldIndex("sourcesystem"))
    var destinationsystem = row.get(row.fieldIndex("destinationsystem"))
    var sourceschema = row.get(row.fieldIndex("sourceschema"))
    var destinationschema = row.get(row.fieldIndex("destinationschema"))
    var sourcetable = row.get(row.fieldIndex("sourcetable"))
    var destinationtable = row.get(row.fieldIndex("destinationtable"))
    var loadtype = row.get(row.fieldIndex("loadtype"))
    var tableselect = row.get(row.fieldIndex("tableselect")).toString
    var tableqry = row.get(row.fieldIndex("tableqry"))

    var sql_table = destinationschema + """.""" + destinationtable
  
    var df_filepath = df_FileList.filter(df_FileList("FileName") === sourcetable).select(col("FullFilePath").cast("string"))
  
     for (row <- df_filepath.rdd.collect)
     {     
        var filepath = row.get(row.fieldIndex("FullFilePath"))  
        if(sourcesystem == "AzureDatalake")
         {
            //get start date and time
            var loadstarttime = java.sql.Timestamp.from(java.time.Instant.now) 

            //get source data
            var df_sourcedata = get_dataframe_file(filepath.toString)
           
            spark.catalog.dropTempView("tbl_filedata")
            df_sourcedata.createTempView("tbl_filedata")
  
            //var df_transsourcedata = set_dataconversion(tableselect, df_sourcedata )
            
            //display (df_transsourcedata)
        }
     }
 }
