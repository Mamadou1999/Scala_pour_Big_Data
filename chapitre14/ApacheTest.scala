// Databricks notebook source
sc

// COMMAND ----------

val myRdd = sc.parallelize(List(1,2,3,4,5,6,7,8,9,10))

// COMMAND ----------

myRdd.map(x=>x*2).collect()

// COMMAND ----------

def giveEvens(givenNumber:Int):Boolean = {
  givenNumber % 2 == 0
}

// COMMAND ----------

myRdd.filter(x=>giveEvens(x)).collect()

// COMMAND ----------

myRdd.filter(x=>giveEvens(x)).collect().reduce((x,y) => x + y)

// COMMAND ----------

myRdd.collect().foreach(x => println(x))

// COMMAND ----------

val days = sc.parallelize(List("Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"))

// COMMAND ----------

days.collect().zipWithIndex.foreach {
    case(day, count) => println(s"$count is $day")
}

// COMMAND ----------

val numbers = sc.parallelize(List(1, 2, 2, 3, 4, 5, 5, 6))

// COMMAND ----------

numbers.collect().take(4)

// COMMAND ----------


