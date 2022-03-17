// Databricks notebook source
// MAGIC %md Names.csv 
// MAGIC * Dodaj kolumnę z wartością czasu wykonania notatnika w formacie epoch
// MAGIC * Dodaj kolumnę w której wyliczysz wzrost w stopach (feet)
// MAGIC * Odpowiedz na pytanie jakie jest najpopularniesze imię?
// MAGIC * Dodaj kolumnę i policz wiek aktorów 
// MAGIC * Usuń kolumny (bio, death_details)
// MAGIC * Zmień nazwy kolumn - dodaj kapitalizaję i usuń _
// MAGIC * Posortuj dataframe po imieniu rosnąco

// COMMAND ----------

val filePath = "dbfs:/FileStore/tables/Files/names.csv"
val namesDf = spark.read.format("csv")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)

display(namesDf)

// COMMAND ----------

// DBTITLE 1,Dodanie Kolumn
import org.apache.spark.sql.functions._
val newNamesDf = namesDf
     .withColumn("height_in_feets", $"height"*0.032808399)
     .withColumn("current epoch time",unix_timestamp().as("current epoch time"))

display(newNamesDf)

// COMMAND ----------

// DBTITLE 1,Najpopularniejsze imię
val onlyNamesDf = newNamesDf.select(split(col("name")," ").as("NameArray"))
val onlyFirstNamesDf = onlyNamesDf.selectExpr("NameArray[0]").withColumnRenamed("NameArray[0]", "FirstName")

val popularNames =  onlyFirstNamesDf.groupBy("FirstName").count().sort($"count".desc)
display(popularNames)


// COMMAND ----------

// DBTITLE 1,Wiek aktorów
val ageDf = newNamesDf
  .withColumn("current_date",current_date())
  .withColumn("birth",to_date($"date_of_birth", "dd.MM.yyyy"))
  .withColumn("death",to_date($"date_of_death", "dd.MM.yyyy"))



val diffAgeDf = ageDf
  .withColumn("diff",when($"death".isNull , floor(datediff($"birth",$"current_date")/(-365))).otherwise(floor(datediff($"birth",$"death")/(-365))))
  


       
display( diffAgeDf)

// COMMAND ----------

// DBTITLE 1,Usunięcie kolumn
val dropNamesDf = newNamesDf.drop("bio","death_details")
display(dropNamesDf)

// COMMAND ----------

// DBTITLE 1,Sortowanie
val sortedNamesDf = newNamesDf.orderBy($"name".asc)
display(sortedNamesDf)

// COMMAND ----------

// DBTITLE 1,Zmiana nazw kolumn
//WERSJA Z DUŻĄ LITERĄ TYLKO NA POCZĄTKU I SPACJAMI
//val allColumnNames = newNamesDf.columns
//val ColToUpperCase = allColumnNames.map(xs => xs.toLowerCase.capitalize)
//val CleanedCol = ColToUpperCase.map(xs => xs.replace("_"," "))

//WERSJA Z CAMMELCASE
val allColumnNames = newNamesDf.columns
val CapitalizedCol = allColumnNames.map(x => x.split('_').map(_.capitalize).mkString(""))
val newColNames= newNamesDf.toDF( CapitalizedCol: _*)
display(newColNames)

// COMMAND ----------

// MAGIC %md Movies.csv
// MAGIC * Dodaj kolumnę z wartością czasu wykonania notatnika w formacie epoch
// MAGIC * Dodaj kolumnę która wylicza ile lat upłynęło od publikacji filmu
// MAGIC * Dodaj kolumnę która pokaże budżet filmu jako wartość numeryczną, (trzeba usunac znaki walut)
// MAGIC * Usuń wiersze z dataframe gdzie wartości są null

// COMMAND ----------

val filePath = "dbfs:/FileStore/tables/Files/movies.csv"
val moviesDf = spark.read.format("csv")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)

display(moviesDf)

// COMMAND ----------

// DBTITLE 1,Dodanie kolumn
import org.apache.spark.sql.functions._
val newMoviesDf = moviesDf
     .withColumn("current_epoch_time",unix_timestamp())
     .withColumn("current_time",current_date())
     .withColumn("date",to_date($"year", "yyyy"))
     .withColumn("time_from_publication",floor(datediff($"date",$"current_time")/(-365)))
     .withColumn("new_budget",regexp_replace($"budget","\\D",""))

     .drop("date","current_time")


//df_pres.select($"pres_name",translate($"pres_name","J","Z").as("new_name")).show()
display(newMoviesDf)

// COMMAND ----------

// MAGIC %md ratings.csv
// MAGIC * Dodaj kolumnę z wartością czasu wykonania notatnika w formacie epoch
// MAGIC * Dla każdego z poniższych wyliczeń nie bierz pod uwagę `nulls` 
// MAGIC * Dodaj nowe kolumny i policz mean i median dla wartości głosów (1 d 10)
// MAGIC * Dla każdej wartości mean i median policz jaka jest różnica między weighted_average_vote
// MAGIC * Kto daje lepsze oceny chłopcy czy dziewczyny dla całego setu
// MAGIC * Dla jednej z kolumn zmień typ danych do `long` 

// COMMAND ----------

val filePath = "dbfs:/FileStore/tables/Files/ratings.csv"
val ratingsDf = spark.read.format("csv")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)

display(ratingsDf)

// COMMAND ----------

// DBTITLE 1,Dodanie kolumny
import org.apache.spark.sql.functions._
val newRatingsDf = ratingsDf
     .withColumn("current_epoch_time",unix_timestamp())
     .na.drop()

//display(newRatingsDf)

// COMMAND ----------

// DBTITLE 1,Mean i median
//Dodaj nowe kolumny i policz mean i median dla wartości głosów (1 d 10)
val ratingsMeanMedian = newRatingsDf.
display(ratings)


// COMMAND ----------

// DBTITLE 1,Głosy według płci
//Kto daje lepsze oceny chłopcy czy dziewczyny dla całego setu
val ratings = newRatingsDf.select("females_allages_avg_vote","males_allages_avg_vote")
val ratingsAvg = ratings.agg(avg("females_allages_avg_vote") as "female",avg("males_allages_avg_vote") as "male")
display(ratingsAvg)

//Średnio we wszystkich grupach wiekowych kobiety dają wyższe oceny filmom niż mężczyźni

// COMMAND ----------

// DBTITLE 1,Zmiana typu
//Dla jednej z kolumn zmień typ danych do long
import org.apache.spark.sql.types._
val newTypeRatings = newRatingsDf.withColumn("mean_vote",col("mean_vote").cast(LongType))
display(newTypeRatings)

// COMMAND ----------

// DBTITLE 1,Porównanie explain() z explain() + groupBy()
val MoviesDf2 = newMoviesDf.select($"title",$"year").explain()

//Dodanie operacji groupby i explain
val MoviesDf3 = newMoviesDf.select($"title",$"year").groupBy("year").count().explain()



// COMMAND ----------

// DBTITLE 1,UI Spark
//Jobs - informuje o statusie jobów (complited, active, failed), czasie trwania i ich progresie
//Stages - pokazuje aktualny stan wszystkich etapów dla poszczególnych zadań w aplikacji Spark
//Storage - pokazuje stan pamięci, wypisuje utworzone partycje i ich rozmiar 
//Environment - zawiera informacje o zmiennych środowiskowych
//Executors - mówi ile jest Executorów jobów i informuje o ich stanie (pamięć, zużycie dysku, zadania)
//SQL - zawiera wszystkie informacje na temat komend sql (czas trwania, id, czas rozpoczecia, fizyczny i logiczny plan działania)
//JDBC/ODBC Server - informuje o statystykach sesji i sql
//Structured Streaming - wyświetla statystyki dotyczące trwających i zakończonych zapytań, dostepne tylko w trybie micro-batch

// COMMAND ----------

// DBTITLE 1,SQL Server
val jdbcDF = sqlContext.read
      .format("jdbc")
      .option("driver" , "com.microsoft.sqlserver.jdbc.SQLServerDriver")
      .option("url", "jdbc:sqlserver://bidevtestserver.database.windows.net:1433;database=testdb")
      .option("dbtable", "(SELECT table_name FROM information_schema.tables) tmp")
      .option("user", "sqladmin")
      .option("password", "$3bFHs56&o123$")
      .load()

display(jdbcDF)
