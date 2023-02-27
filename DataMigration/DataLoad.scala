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
           
              if (createtable == true)
              {
                var tabledrop = """ Drop Table If EXISTS """ + sql_table
                var createtableqry = """ Create Table """ + sql_table + """ ( """ + tableqry + """)"""

                //drop table if exists
                var dropquerystatus = jdbcrunquery(tabledrop)

                //create table from job table field entity
                var createquerystatus = jdbcrunquery(createtableqry) 
              }
           
            //get source data
            var df_sourcedata = get_dataframe_file(filepath.toString)
           
            spark.catalog.dropTempView("tbl_filedata")
            df_sourcedata.createTempView("tbl_filedata")
  
            var df_transsourcedata = set_dataconversion(tableselect, df_sourcedata )
            
             if(df_transsourcedata.count() > 0) //check count of sql table
             {
               df_transsourcedata.write
                     .format("jdbc")
                     .mode("append") //comment this line if want create the table from source data frame oruse existing schema
                     .option("url",url_postgres)
                     .option("dbtable",sql_table)
                     .option("schemaCheckEnabled", true) 
                     .save() 
             }
              var loadendtime =  java.sql.Timestamp.from(java.time.Instant.now)

            //audit log query
          var auditquery = Array(sourcetable,destinationtable,loadstarttime,loadendtime,
                                 df_transsourcedata.count(),df_transsourcedata.count(),"Success")

          //load audit details
          var dropquerystatus = auditlog_db(auditquery)
        }
     }
 }
