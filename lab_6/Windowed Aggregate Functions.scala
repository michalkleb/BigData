// Databricks notebook source
// MAGIC  %md
// MAGIC  <h1> SQL -> Scala <h1>

// COMMAND ----------

// MAGIC %md
// MAGIC <b> Creating a database.  <b>

// COMMAND ----------

// MAGIC %sql
// MAGIC Create database if not exists Sample

// COMMAND ----------

spark.sql("Create database if not exists Sample")

// COMMAND ----------

// MAGIC %md
// MAGIC <b> Creating a tables  <b>

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC CREATE TABLE IF NOT EXISTS Sample.Transactions ( AccountId INT, TranDate DATE, TranAmt DECIMAL(8, 2));
// MAGIC 
// MAGIC CREATE TABLE IF NOT EXISTS Sample.Logical (RowID INT,FName VARCHAR(20), Salary SMALLINT);

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC INSERT INTO Sample.Transactions VALUES 
// MAGIC ( 1, '2011-01-01', 500),
// MAGIC ( 1, '2011-01-15', 50),
// MAGIC ( 1, '2011-01-22', 250),
// MAGIC ( 1, '2011-01-24', 75),
// MAGIC ( 1, '2011-01-26', 125),
// MAGIC ( 1, '2011-01-28', 175),
// MAGIC ( 2, '2011-01-01', 500),
// MAGIC ( 2, '2011-01-15', 50),
// MAGIC ( 2, '2011-01-22', 25),
// MAGIC ( 2, '2011-01-23', 125),
// MAGIC ( 2, '2011-01-26', 200),
// MAGIC ( 2, '2011-01-29', 250),
// MAGIC ( 3, '2011-01-01', 500),
// MAGIC ( 3, '2011-01-15', 50 ),
// MAGIC ( 3, '2011-01-22', 5000),
// MAGIC ( 3, '2011-01-25', 550),
// MAGIC ( 3, '2011-01-27', 95 ),
// MAGIC ( 3, '2011-01-30', 2500)

// COMMAND ----------

// MAGIC %sql
// MAGIC INSERT INTO Sample.Logical
// MAGIC VALUES (1,'George', 800),
// MAGIC (2,'Sam', 950),
// MAGIC (3,'Diane', 1100),
// MAGIC (4,'Nicholas', 1250),
// MAGIC (5,'Samuel', 1250),
// MAGIC (6,'Patricia', 1300),
// MAGIC (7,'Brian', 1500),
// MAGIC (8,'Thomas', 1600),
// MAGIC (9,'Fran', 2450),
// MAGIC (10,'Debbie', 2850),
// MAGIC (11,'Mark', 2975),
// MAGIC (12,'James', 3000),
// MAGIC (13,'Cynthia', 3000),
// MAGIC (14,'Christopher', 5000);

// COMMAND ----------

val columns_transactions = Seq("AccountId","TranDate", "TranAmt")

val data_transactions = Seq(( 1, "2011-01-01", 500),
( 1, "2011-01-15", 50),
( 1, "2011-01-22", 250),
( 1, "2011-01-24", 75),
( 1, "2011-01-26", 125),
( 1, "2011-01-28", 175),
( 2, "2011-01-01", 500),
( 2, "2011-01-15", 50),
( 2, "2011-01-22", 25),
( 2, "2011-01-23", 125),
( 2, "2011-01-26", 200),
( 2, "2011-01-29", 250),
( 3, "2011-01-01", 500),
( 3, "2011-01-15", 50 ),
( 3, "2011-01-22", 5000),
( 3, "2011-01-25", 550),
( 3, "2011-01-27", 95 ),
( 3, "2011-01-30", 2500))

val rdd = spark.sparkContext.parallelize(data_transactions)
val transactionsDf = rdd.toDF(columns_transactions:_*)

display(transactionsDf)


// COMMAND ----------

val columns_logical = Seq("RowID","FName", "Salary")
val data_logical = Seq(
(1,"George", 800),
(2,"Sam", 950),
(3,"Diane", 1100),
(4,"Nicholas", 1250),
(5,"Samuel", 1250),
(6,"Patricia", 1300),
(7,"Brian", 1500),
(8,"Thomas", 1600),
(9,"Fran", 2450),
(10,"Debbie", 2850),
(11,"Mark", 2975),
(12,"James", 3000),
(13,"Cynthia", 3000),
(14,"Christopher", 5000))

val rdd = spark.sparkContext.parallelize(data_logical)
val logicalDf = rdd.toDF(columns_logical:_*)

display(logicalDf)


// COMMAND ----------

// MAGIC %md 
// MAGIC Totals based on previous row

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SELECT AccountId,
// MAGIC TranDate,
// MAGIC TranAmt,
// MAGIC -- running total of all transactions
// MAGIC SUM(TranAmt) OVER (PARTITION BY AccountId ORDER BY TranDate) as RunTotalAmt
// MAGIC FROM Sample.Transactions ORDER BY AccountId, TranDate;

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

// COMMAND ----------

val windowSpec = Window.partitionBy("AccountId").orderBy("TranDate")
val sumTransactionsDf = transactionsDf.withColumn("RunTotalAmt",sum("TranAmt").over(windowSpec)).orderBy("AccountId", "TranDate")
display(sumTransactionsDf)

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT AccountId,
// MAGIC TranDate,
// MAGIC TranAmt,
// MAGIC -- running average of all transactions
// MAGIC AVG(TranAmt) OVER (PARTITION BY AccountId ORDER BY TranDate) as RunAvg,
// MAGIC -- running total # of transactions
// MAGIC COUNT(*) OVER (PARTITION BY AccountId ORDER BY TranDate) as RunTranQty,
// MAGIC -- smallest of the transactions so far
// MAGIC MIN(TranAmt) OVER (PARTITION BY AccountId ORDER BY TranDate) as RunSmallAmt,
// MAGIC -- largest of the transactions so far
// MAGIC MAX(TranAmt) OVER (PARTITION BY AccountId ORDER BY TranDate) as RunLargeAmt,
// MAGIC -- running total of all transactions
// MAGIC SUM(TranAmt) OVER (PARTITION BY AccountId ORDER BY TranDate) RunTotalAmt
// MAGIC FROM Sample.Transactions 
// MAGIC ORDER BY AccountId,TranDate;

// COMMAND ----------

val windowSpec = Window.partitionBy("AccountId").orderBy("TranDate")
val sumTransactionsDf = transactionsDf.
withColumn("RunAvg",avg("TranAmt").over(windowSpec)).
withColumn("RunTranQty",count("TranAmt").over(windowSpec)).
withColumn("RunSmallAmt",min("TranAmt").over(windowSpec)).
withColumn("RunLargeAmt",max("TranAmt").over(windowSpec)).
withColumn("RunTotalAmt",sum("TranAmt").over(windowSpec)).orderBy("AccountId", "TranDate")


display(sumTransactionsDf)

// COMMAND ----------

// MAGIC %md 
// MAGIC * Calculating Totals Based Upon a Subset of Rows

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT AccountId,
// MAGIC TranDate,
// MAGIC TranAmt,
// MAGIC -- average of the current and previous 2 transactions
// MAGIC AVG(TranAmt) OVER (PARTITION BY AccountId ORDER BY TranDate ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as SlideAvg,
// MAGIC -- total # of the current and previous 2 transactions
// MAGIC COUNT(*) OVER (PARTITION BY AccountId ORDER BY TranDate ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as SlideQty,
// MAGIC -- smallest of the current and previous 2 transactions
// MAGIC MIN(TranAmt) OVER (PARTITION BY AccountId ORDER BY TranDate ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as SlideMin,
// MAGIC -- largest of the current and previous 2 transactions
// MAGIC MAX(TranAmt) OVER (PARTITION BY AccountId ORDER BY TranDate ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as SlideMax,
// MAGIC -- total of the current and previous 2 transactions
// MAGIC SUM(TranAmt) OVER (PARTITION BY AccountId ORDER BY TranDate ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as SlideTotal,
// MAGIC ROW_NUMBER() OVER (PARTITION BY AccountId ORDER BY TranDate) AS RN
// MAGIC FROM Sample.Transactions 
// MAGIC ORDER BY AccountId, TranDate, RN

// COMMAND ----------

val windowSpec = Window.partitionBy("AccountId").orderBy("TranDate").rowsBetween(-2,Window.currentRow)
val windowSpec2 = Window.partitionBy("AccountId").orderBy("TranDate")
val sumTransactionsDf = transactionsDf.
  withColumn("SlideAvg",avg("TranAmt").over(windowSpec))
  .withColumn("SlideQty",count("*").over(windowSpec))
  .withColumn("SlideMin",min("TranAmt").over(windowSpec))
  .withColumn("SlideMax",max("TranAmt").over(windowSpec))
  .withColumn("SlideTotal",sum("TranAmt").over(windowSpec))
  .withColumn("RN", row_number().over(windowSpec2))
  .orderBy("AccountId", "TranDate")
display(sumTransactionsDf)

// COMMAND ----------

// MAGIC %md
// MAGIC * Logical Window

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT RowID,
// MAGIC FName,
// MAGIC Salary,
// MAGIC SUM(Salary) OVER (ORDER BY Salary ROWS UNBOUNDED PRECEDING) as SumByRows,
// MAGIC SUM(Salary) OVER (ORDER BY Salary RANGE UNBOUNDED PRECEDING) as SumByRange,
// MAGIC 
// MAGIC FROM Sample.Logical
// MAGIC ORDER BY RowID;

// COMMAND ----------

val windowSpec1 = Window.orderBy("Salary").rowsBetween(Window.unboundedPreceding, Window.currentRow)
val windowSpec2 = Window.orderBy("Salary").rangeBetween(Window.unboundedPreceding, Window.currentRow)
val logicalDf2 = logicalDf.
  withColumn("SumByRows",sum("Salary").over(windowSpec1))
  .withColumn("SumByRange",sum("Salary").over(windowSpec2))
  .orderBy("RowID")
display(logicalDf2)

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT TOP 10
// MAGIC AccountNumber,
// MAGIC OrderDate,
// MAGIC TotalDue,
// MAGIC ROW_NUMBER() OVER (PARTITION BY AccountNumber ORDER BY OrderDate) AS RN
// MAGIC FROM Sales.SalesOrderHeader
// MAGIC ORDER BY AccountNumber; 

// COMMAND ----------

val windowSpec1 = Window.partitionBy("TranAmt").orderBy("TranDate")
val logicalDf2 = transactionsDf.
  withColumn("RN",row_number().over(windowSpec1))
  .orderBy("TranDate", "RN")
  .head(10)
display(logicalDf2)
