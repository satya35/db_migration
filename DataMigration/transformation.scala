// Databricks notebook source
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import spark.sqlContext.implicits._

// COMMAND ----------

def set_dataconversion(query_select: String,df_data: DataFrame) : DataFrame = {
  
  spark.catalog.dropTempView("tbl_filedata")
  df_data.createTempView("tbl_filedata")
  
  var query = "select " + query_select + " from tbl_filedata"
  
  var df_filedata = spark.sql(query)
  
  return df_filedata  
}
