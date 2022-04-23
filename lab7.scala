// Databricks notebook source
// DBTITLE 1,Indeksy w Hive
// MAGIC %md
// MAGIC Hive wspiera indeksy. Ich użycie może znacznie przyspieszyć wykonanie zapytania na kolumnach tabeli. Jeżeli zapytanie będzie wykonywane na tabeli bez indeksów, wtedy będzie miało miejsce filtrowanie z użyciem klauzuli WHERE (zostanie wczytana cała tabela i będą procesowane wszystkie wiersze). Jeżeli zapytanie zostanie wykonane na tabeli z indeksami, wtedy najpierw ma miejsce sprawdzenie na któych kolumnach zostały założone indeksy i możliwe jest wczytanie jedynie danej kolumny i wykonanie na niej zapytania. Indeksowanie jest użyeczne w przydku zapytań które są bardzo często przeprowadzane a ich czas wykonania jest długi. Minusem indeksowania jest tworzenie na dysku tabeli indeksów, dlatego nieprzemyślane tworzenie indeksów może nie skrócić, a wręcz wydłużyć czas wykonania zapytania.

// COMMAND ----------

spark.catalog.listDatabases().show()


// COMMAND ----------

// DBTITLE 1,Creating a database
spark.sql("create database freblogg")

// COMMAND ----------

// DBTITLE 1,Creating a Table
val filePath = "dbfs:/FileStore/tables/Files/names.csv"
val df = spark.read.format("csv")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)
df.write.mode("overwrite").saveAsTable("freblogg.names")
df.write.mode("overwrite").saveAsTable("freblogg.names2")

// COMMAND ----------

spark.catalog.listTables("freblogg").show()
spark.catalog.tableExists("freblogg","names")

// COMMAND ----------

// DBTITLE 1,Function that removes all contents from tables in the database 
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

def drop_all(database: String){
  
  val tabele = spark.catalog.listTables(s"$database")
  val names=tabele.select("name").as[String].collect.toList
  var i = List()
  for( i <- names){
    spark.sql(s"DELETE FROM $database.$i")
  }
  
}

drop_all("freblogg")
