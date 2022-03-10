// Databricks notebook source
// DBTITLE 1,Wczytanie danych ze schematem
// Wybierz jeden z plików csv z poprzednich ćwiczeń i stwórz ręcznie schemat danych. Stwórz DataFrame wczytując plik z użyciem schematu.
//wczytanie actors.csv i podglad zawartosci tabeli
val filePath = "dbfs:/FileStore/tables/actors.csv"
val actorsDf = spark.read.format("csv")
            .option("header","true")
            .option("inferSchema","true")
            .load(filePath)
display(actorsDf)

import org.apache.spark.sql.types._

val schemat = StructType(Array(
  StructField("imdb_title_id", StringType, true),
  StructField("ordering", IntegerType, true),
  StructField("imdb_name_id", StringType, true),
  StructField("category", StringType, true),
  StructField("job", StringType, true),
  StructField("characters", StringType, true))
)

val actorsSchemaDf = sqlContext.read.format("csv")
  .option("header", "true")
  .schema(schemat)
  .load(filePath)

display(actorsSchemaDf)

//wyswietlenie schematu za pomoca funkcji
actorsDf.printSchema

// COMMAND ----------

// DBTITLE 1,Wczytanie danych do DF z pliku JSON
// Użyj kilku rzędów danych z jednego z plików csv i stwórz plik json. Stwórz schemat danych do tego pliku. Przydatny tool to sprawdzenie formatu danych. https://jsonformatter.curiousconcept.com/

val actorsJson = """
    [{
    "imdb_title_id": "tt0000009",
    "ordering": 1,
    "imdb_name_id": "nm0063086",
    "category": "actress",
    "job": "null",
    "characters": [
      "Miss Geraldine Holbrook (Miss Jerry)"
      ]
  },
  {
    "imdb_title_id": "tt0000009",
    "ordering": 2,
    "imdb_name_id": "nm0183823",
    "category": "actor",
    "job": "null",
    "characters": [
      "Mr. Hamilton"
      ]
  },
  {
    "imdb_title_id": "tt0002844",
    "ordering": 4,
    "imdb_name_id": "nm0137288",
    "category": "actress",
    "job": "null",
    "characters": [
      "Lady Beltham",
      "maîtresse de Fantômas"
      ]
  }]
  """

val miniDf =actorsDf.limit(5)

miniDf.write.json("dbfs:/FileStore/tables/miniDf.json")
val miniDf_with_schema = spark.read.schema(schemat).json("dbfs:/FileStore/tables/miniDf.json")
display(miniDf_with_schema)

// COMMAND ----------

// DBTITLE 1,Użycie Read Modes
//Wykorzystaj posiadane pliki bądź dodaj nowe i użyj wszystkich typów oraz ‘badRecordsPath’, zapisz co się dzieje. Jeśli jedna z opcji nie da //żadnych efektów, trzeba popsuć dane.

val record1 = "{ppppp}"

val record2 = "{'imdb_title_id': 'tt00003', 'ordering': 1, 'imdb_name_id': 'nm0063486', 'category': 'actress', 'job': 'null', 'characters': ['Miss Geraldine Holbrook (Miss Jerry)']}"

val record3 = "{'imdb_title_id': 'tt0110009', 'ordering': 2, 'imdb_name_id': 'nm003086', 'category': 1, 'job': 'null', 'characters': 'Miss Geraldine Holbrook (Miss Jerry)'}"

Seq(record1, record2, record3).toDF().write.mode("overwrite").text("/FileStore/tables/small_df.json")

val miniDFDropMalFormed = spark.read.format("json")
  .schema(schemat)
  .option("mode", "DROPMALFORMED")
  .load("/FileStore/tables/small_df.json")
val miniDFBadRecord = spark.read.format("json")
  .schema(schemat)
  .option("badRecordsPath", "/FileStore/tables/badrecords")
  .load("/FileStore/tables/small_df.json")
val miniDFPermissive = spark.read.format("json")
  .schema(schemat)
  .option("mode", "PERMISSIVE")
  .load("/FileStore/tables/small_df.json")

val miniDFFailFast = spark.read.format("json")
  .schema(schemat)
  .option("mode", "FAILFAST")
  .load("/FileStore/tables/small_df.json")

display(miniDFDropMalFormed)

// COMMAND ----------

// DBTITLE 1,Użycie DataFrameWriter
//Użycie DataFrameWriter.
//Zapisz jeden z wybranych plików do formatów (‘.parquet’, ‘.json’). Sprawdź, czy dane są zapisane poprawnie, użyj do tego DataFrameReader. //Opisz co widzisz w docelowej ścieżce i otwórz używając DataFramereader.

actorsDf.write.format("parquet").mode("overwrite").save("/FileStore/tables/actors_par.parquet")
val actors_par = spark.read.format("parquet").load("/FileStore/tables/actors_par.parquet")
display(actors_par)
//format parquet grupuje rekordy podczas zapisu df, twprzy dzieki temu mniej plikow w folderze,  
//parquet jest nieczytelny dla czlowieka w przeciwienstwie do jsona

