// Databricks notebook source
// MAGIC %run ./job_run

// COMMAND ----------

// MAGIC %python
// MAGIC #to get all latest file
// MAGIC df = get_latest_modified_file_from_directory('/mnt/' )
// MAGIC spark.catalog.dropTempView("df_FileList")
// MAGIC df.createTempView("df_FileList")

// COMMAND ----------

var df = spark.sql("select * from df_FileList")
var df1 = sqlContext.table("df_FileList")
display(df1)

// COMMAND ----------

//to get all tables details to load
var df_TableEntity = get_load_entity()

// COMMAND ----------

var df = df_TableEntity.filter(df_TableEntity("isactive") === true)

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
  
    if(sourcesystem == "AzureSQL")
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
       
        var sourcedata = spark.read
                 .format("jdbc")
                 .option("url",url_sql)
                 .option("dbtable",sourceschema + "." + sourcetable)
                 .load()
       
        if(sourcedata.count() > 0) //check count of sql table
         {
           sourcedata.write
                 .format("jdbc")
                 .mode("append")  //comment this line if want create the table from source data frame or use existing schema
                 .option("url",url_postgres)
                 .option("dbtable",postgressql_table)
                 .option("schemaCheckEnabled", true) 
                 .save() 
         }
          var loadendtime =  java.sql.Timestamp.from(java.time.Instant.now)
       
        //audit log query
      var auditquery = Array(sourcetable,destinationtable,loadstarttime,loadendtime,sourcedata.count(),sourcedata.count(),"Success")
          
      //load audit details
      var dropquerystatus = auditlog_db(auditquery)
       
     } 
}
