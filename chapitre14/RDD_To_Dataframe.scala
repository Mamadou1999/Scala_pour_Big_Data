// Databricks notebook source
val myData = sc.textFile("/FileStore/tables/sample_file.csv")

// COMMAND ----------

case class DataSchema(id:Int, name:String, language:String,
country:String)

// COMMAND ----------

val myDF = myData.map(x=>x.split(",")).map(x=>DataSchema(x(0).toInt, x(1), x(2), x(3))).toDF

// COMMAND ----------

myDF.select("id").show()

// COMMAND ----------

myDF.filter("language == 'java'").show()

// COMMAND ----------

myDF.createOrReplaceTempView("df_temp")

// COMMAND ----------

spark.sql("select * from df_temp where country = 'pakistan'").show()

// COMMAND ----------


