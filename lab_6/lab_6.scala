// Databricks notebook source
val filePath = "dbfs:/FileStore/tables/names.csv"
val namesDf = spark.read.format("csv")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)

display(namesDf)

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window



// COMMAND ----------

val newDf = namesDf.select("height","children","divorces","reason_of_death")
display(newDf)

// COMMAND ----------

// DBTITLE 1,Lag
//Lag odnosi się do poprzednicj wierszy
val windowSpec = Window.partitionBy("reason_of_death").orderBy("children")
val lagCol = lag(col("children"), 3)

display(newDf.withColumn("Lag", lagCol.over(windowSpec)))

// COMMAND ----------

// DBTITLE 1,Lead
//Lead odnos się do wartości z następnych wierszy
val windowSpec = Window.partitionBy("reason_of_death").orderBy(desc("divorces"))
val leadCol = lead(col("children"), 1)

display(newDf.withColumn("Lead_children",leadCol.over(windowSpec)))

// COMMAND ----------

// DBTITLE 1,First
// First odnosi się do pierwszeego wystąpienia w grupie. 

val windowSpec = Window.partitionBy("divorces").orderBy(desc("children")).rowsBetween(-2, Window.currentRow)
val windowSpec2 = Window.partitionBy("divorces").orderBy(desc("children")).rangeBetween(-5, Window.currentRow)

val Df = newDf.select(col("divorces"),col("children"),col("height"),
                      first("height").over(windowSpec).alias("first_height_rows"),
                      first("height").over(windowSpec2).alias("first_height_range"))
             
display(Df)

// COMMAND ----------

// DBTITLE 1,Last
// Last odnosi się do ostatniego wystąpienia w grupie. 
val windowSpec = Window.partitionBy("divorces")
val windowSpec2 = Window.partitionBy("divorces")

val Df = newDf.select(col("divorces"),col("children"),col("height"),last("height").over(windowSpec).alias("last")
                      ).orderBy(desc("divorces"),desc("children"))
             
display(Df)

// COMMAND ----------

// DBTITLE 1,Row_number
//row_number - sekwencyjnie numeruje wiersze według partycji
val windowSpec  = Window.partitionBy("reason_of_death").orderBy("height")
  
val Df = newDf.select(
  col("height"), 
  col("reason_of_death"),
  row_number().over(windowSpec).as("rows number"),
  ).orderBy(desc("height"))

display(Df)

// COMMAND ----------

// DBTITLE 1,Dense_rank
//dense_rank - dodaje range bez zostawiania luk
val windowSpec  = Window.partitionBy("reason_of_death").orderBy("height")

val Df = newDf.select(
  col("height"), 
  col("reason_of_death"),
  dense_rank().over(windowSpec).as("dense rank"),
  ).orderBy(desc("dense rank"))

display(Df)

// COMMAND ----------

// MAGIC %md
// MAGIC <h1> Joins <h1>

// COMMAND ----------

val filePath = "dbfs:/FileStore/tables/actors.csv"
val actorsDf = spark.read.format("csv")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)

display(actorsDf)

// COMMAND ----------

val smallNamesDf = namesDf.select("imdb_name_id", "name",  "height")
display(smallNamesDf)

// COMMAND ----------

val smallActorsDf = actorsDf.select("imdb_title_id", "imdb_name_id",  "category")
display(smallActorsDf)

// COMMAND ----------

val joinExpression = smallActorsDf.col("imdb_name_id") === smallNamesDf.col("imdb_name_id")

// COMMAND ----------

// DBTITLE 1,Left semi join
//Wiersze występujące w lewej tabeli i jednocześnie występujące w prawej
smallNamesDf.join(smallActorsDf, joinExpression, "left_semi").show()
smallNamesDf.join(smallActorsDf, joinExpression, "left_semi").explain()

// COMMAND ----------

// DBTITLE 1,Left anti join
//Wiersze które są w lewej tabeli ale nie występują w prawej
smallNamesDf.join(smallActorsDf, joinExpression, "left_anti").show()
smallNamesDf.join(smallActorsDf, joinExpression, "left_anti").explain()

// COMMAND ----------

// MAGIC %md
// MAGIC <h1> Duplicates </h1>
// MAGIC How to solve the problem of duplicated columns?

// COMMAND ----------

// DBTITLE 1,Join
smallNamesDf.join(smallActorsDf, joinExpression).show()

// COMMAND ----------

// DBTITLE 1,Solution I
smallNamesDf.join(smallActorsDf, "imdb_name_id").show()
smallNamesDf.join(smallActorsDf, "imdb_name_id").explain()

// COMMAND ----------

// DBTITLE 1,Solution II
val newDf = smallNamesDf.join(smallActorsDf, joinExpression).drop(smallActorsDf("imdb_name_id"))
display(newDf)

// COMMAND ----------

// DBTITLE 1,Broadcast join
val shortActorsDf = smallActorsDf.limit(20)

shortActorsDf.join(broadcast(smallNamesDf),"imdb_name_id").explain()
