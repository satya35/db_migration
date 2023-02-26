// Databricks notebook source
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

// COMMAND ----------

def set_dataconversion(query_select: String,df_data: DataFrame) : DataFrame = {
  
  spark.catalog.dropTempView("df_data")
  df_data.createTempView("df_filedata")
  
  var query = "select " + query_select + " from df_FileList"
  
  var df_filedata = spark.sql(query)
  
  return df_filedata  
}
