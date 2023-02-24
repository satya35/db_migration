// Databricks notebook source
// MAGIC %run ./config_server

// COMMAND ----------

import org.apache.spark.sql.functions
import org.apache.spark.sql.DataFrame

// COMMAND ----------

def get_dataframe = (query: String) =>
{
    try
    {
        val df_TableEntity = spark.read
                              .format("jdbc")
                              .option("driver", driver)
                              .option("url", url_postgres)
                              .option("dbtable", query)
                              .load()

         //return df_TableEntity
    }
    catch 
    {
       case unknown: Exception => {
        println(s"Unknown exception: $unknown")
        None
      }
    }
}


// COMMAND ----------

//to run any ddl or dml, custome query 
def jdbcrunquery = (query: String) =>
{
   try
    {
      var conn = java.sql.DriverManager.getConnection(url_postgres)

      var exec_statement = conn.prepareCall(query)
      exec_statement.execute()

      exec_statement.close()
      conn.close()
    }
    catch 
    {
       case unknown: Exception => {
        println(s"Unknown exception: $unknown")
        none
      }
    }
}

// COMMAND ----------

//to audit log in db
def auditlog_db = (args: Array[Any]) =>
{
   try
    {
         // register an UDF that creates a random UUID
      val generateUUID = java.util.UUID.randomUUID
      
      var auditqry = """Insert Into config.LoadAudit (RunId,SourceTable,DestinationTable,LoadStartDate,LoadEndDate ,SourceDataCount,DestinationDataCount,Status) VALUES ( """ + """'""" + generateUUID + """','""" + args(0) + """','""" + args(1)+ """','""" +  args(2) + """','""" +  args(3) + """',""" +  args(4) + """,""" +  args(5) + """,'""" + args(6) + """' ) """
      
      //load audit
      var loadaudit = jdbcrunquery(auditqry)
    }
    catch 
    {
       case unknown: Exception => {
        println(s"Unknown exception: $unknown")
        None
      }
    }
}

// COMMAND ----------

//to run any ddl or dml, custome query 
//def get_load_entity = () =>
//{
  
  //to get table details with columns to create table 
  val pushdown_query = """(Select t.Id,t.SourceSystem,t.DestinationSystem,t.SourceSchema,t.DestinationSchema,t.SourceTable,
  t.DestinationTable,t.MainContainer,t.SubContainer,t.LoadType,t.CreateTable,t.SequenceofLoad,t.IsActive,
  STRING_AGG(Concat(DestinationColumn ,' ', DestinationDataType), ',' ORDER BY ColumnSequence)  AS tableqry 
  FROM config.TableEntity t Left Join config.TableFieldEntity f On t.SourceSchema = f.SourceSchema 
  And t.DestinationSchema = f.DestinationSchema 
  And t.SourceTable =f.SourceTable
  And t.DestinationTable =f.DestinationTable 
  Group by t.Id,t.SourceSystem,t.DestinationSystem,t.SourceSchema,t.DestinationSchema,t.SourceTable,t.DestinationTable,
  t.MainContainer,t.SubContainer,t.LoadType,t.CreateTable,t.SequenceofLoad,t.IsActive Order by t.SequenceofLoad Asc) as    TableEntity"""
  
  var df = get_dataframe(pushdown_query)

//}
