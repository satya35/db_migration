// Databricks notebook source
// MAGIC %run ./job_run

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
    var createtable = row.get(row.fieldIndex("createtable"))
    var tableqry = row.get(row.fieldIndex("tableqry"))

    var postgressql_table = destinationschema + """.""" + destinationtable
  
    var df_filepath = df_FileList.filter(df_FileList("FileName") === sourcetable).select(col("FullFilePath").cast("string"))

    var filepath= type(df_filepath) 
  
    if(sourcesystem == "AzureDatalake")
     {
        if (createtable == true)
        {
          var tabledrop = """ Drop Table If EXISTS """ + postgressql_table
          var createtableqry = """ Create Table """ + postgressql_table + """ ( """ + tableqry + """)"""
          
          //drop table if exists
          var dropquerystatus = jdbcrunquery(tabledrop)
          
          //create table from job table field entity
          var createquerystatus = jdbcrunquery(createtableqry) //comment this line if want create the table from source data frame or use existing schema
        }
       
        //get start date and time
        var loadstarttime = java.sql.Timestamp.from(java.time.Instant.now) 
       
        //get source data
        //var sourcedata = get_dataframe_file(filepath)
     }
}
