// Databricks notebook source
// MAGIC %md 
// MAGIC Wykożystaj dane z bazy 'bidevtestserver.database.windows.net'
// MAGIC ||
// MAGIC |--|
// MAGIC |SalesLT.Customer|
// MAGIC |SalesLT.ProductModel|
// MAGIC |SalesLT.vProductModelCatalogDescription|
// MAGIC |SalesLT.ProductDescription|
// MAGIC |SalesLT.Product|
// MAGIC |SalesLT.ProductModelProductDescription|
// MAGIC |SalesLT.vProductAndDescription|
// MAGIC |SalesLT.ProductCategory|
// MAGIC |SalesLT.vGetAllCategories|
// MAGIC |SalesLT.Address|
// MAGIC |SalesLT.CustomerAddress|
// MAGIC |SalesLT.SalesOrderDetail|
// MAGIC |SalesLT.SalesOrderHeader|

// COMMAND ----------

// DBTITLE 1,Wczytanie tabel
//INFORMATION_SCHEMA.TABLES

val jdbcHostname = "bidevtestserver.database.windows.net"
val jdbcPort = 1433
val jdbcDatabase = "testdb"

val tabela = spark.read
  .format("jdbc")
  .option("url",s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}")
  .option("user","sqladmin")
  .option("password","$3bFHs56&o123$")
  .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
  .option("query","SELECT * FROM INFORMATION_SCHEMA.TABLES")
  .load()
display(tabela)

// COMMAND ----------

// MAGIC %md
// MAGIC 1. Pobierz wszystkie tabele z schematu SalesLt i zapisz lokalnie bez modyfikacji w formacie delta

// COMMAND ----------

// DBTITLE 1,Zapisanie tabel wszystkich
val SalesLT = tabela.where("TABLE_SCHEMA == 'SalesLT'")

val names=SalesLT.select("TABLE_NAME").as[String].collect.toList

var i = List()
for( i <- names){
  val tab = spark.read
  .format("jdbc")
  .option("url",s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}")
  .option("user","sqladmin")
  .option("password","$3bFHs56&o123$")
  .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
  .option("query",s"SELECT * FROM SalesLT.$i")
  .load()
  
  tab.write.format("delta").mode("overwrite").saveAsTable(i)
  
}

// COMMAND ----------

// MAGIC %md
// MAGIC  Uzycie Nulls, fill, drop, replace, i agg
// MAGIC  * W każdej z tabel sprawdź ile jest nulls w rzędach i kolumnach
// MAGIC  * Użyj funkcji fill żeby dodać wartości nie występujące w kolumnach dla wszystkich tabel z null
// MAGIC  * Użyj funkcji drop żeby usunąć nulle, 
// MAGIC  * wybierz 3 dowolne funkcje agregujące i policz dla TaxAmt, Freight, [SalesLT].[SalesOrderHeader]
// MAGIC  * Użyj tabeli [SalesLT].[Product] i pogrupuj według ProductModelId, Color i ProductCategoryID i wylicz 3 wybrane funkcje agg() 
// MAGIC    - Użyj conajmniej dwóch overloded funkcji agregujących np z (Map)

// COMMAND ----------

// DBTITLE 1,Funkcja do zliczania Nulli
import org.apache.spark.sql.functions.{col,when, count}
import org.apache.spark.sql.Column

def countCols(columns:Array[String]):Array[Column]={
    columns.map(c=>{
      count(when(col(c).isNull,c)).alias(c)
    })
}

// COMMAND ----------

// DBTITLE 1,Nulle w kolumnach
var i = List()
val names_lower=names.map(x => x.toLowerCase())
for( i <- names_lower){
  val filePath = s"dbfs:/user/hive/warehouse/$i"
  val df = spark.read.format("delta")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)
print(i)
df.select(countCols(df.columns):_*).show()
  
}

// COMMAND ----------

// DBTITLE 1,Wypełnienie Nulli zerami
for( i <- names_lower){
  val filePath = s"dbfs:/user/hive/warehouse/$i"
  val df = spark.read.format("delta")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)
  val col = df.columns
  val df_without_nulls = df.na.fill("0", col)
  
}


// COMMAND ----------

// DBTITLE 1,Tabela SalesOrderHeader
val filePath = s"dbfs:/user/hive/warehouse/salesorderheader"
 val df = spark.read.format("delta")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)

display(df)

// COMMAND ----------

// DBTITLE 1,Funkcje agregujące
// wybierz 3 dowolne funkcje agregujące i policz dla TaxAmt, Freight, [SalesLT].[SalesOrderHeader]

df.select(kurtosis($"TaxAmt")).show()
df.select(skewness($"Freight")).show()
df.select(avg($"Freight")).show()
// Kurtoza ujemna, więc rozkład kolumny TaxAmt jest węższy niż normalny
// Skośność dodatnia, rozkład prawostronnie skośny


// COMMAND ----------

// DBTITLE 1,Wczytanie tabeli Product
 val filePath = s"dbfs:/user/hive/warehouse/product"
 val df2 = spark.read.format("delta")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)
val df=df2.withColumn("ListPrice",round($"ListPrice"))
display(df)

// COMMAND ----------

// DBTITLE 1,Utworzenie UDF
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val euro = (s: Double) => {
  s*0.91
}
val mean = (s: Integer, v:Integer) =>
{
 (s+v)/2
  
}

val PascalCase = (s: String) =>
{
 s.split("_").map(_.capitalize).mkString("")
  
}

spark.udf.register("euro", euro)
spark.udf.register("mean", mean)
spark.udf.register("PascalCase", PascalCase)


// COMMAND ----------

// DBTITLE 1,Wykonanie UDF
 val euro = udf((s: Double) => s*0.91)
 val mean = udf((s: Integer, v:Integer) => (s+v)/2 )
 val PascalCase = udf((s: String) => s.split("_").map(_.capitalize).mkString("") )

val cost_in_euro = df.select(euro($"StandardCost") as "Cost in Euro", mean($"ListPrice",$"ProductCategoryID") as "mean",PascalCase($"ThumbnailPhotoFileName") as "PascalCase")
display(cost_in_euro)

// COMMAND ----------

// DBTITLE 1,Grupowanie na tabeli Product
// Użyj tabeli [SalesLT].[Product] i pogrupuj według ProductModelId, Color i ProductCategoryID i wylicz 3 wybrane funkcje agg()
display(df.groupBy("ProductModelId").mean("StandardCost"))



// COMMAND ----------

display(df.groupBy("ProductCategoryID").avg("ListPrice"))

// COMMAND ----------

display(df.groupBy("Color").count())

// COMMAND ----------

// DBTITLE 1,Funkcja agregująca z map
// Użyj conajmniej dwóch overloded funkcji agregujących np z (Map)

df.groupBy($"ProductModelId").agg(Map("ListPrice" -> "mean","StandardCost" -> "sum")).show()
