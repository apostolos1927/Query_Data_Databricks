-- Databricks notebook source
-- DBTITLE 0,--i18n-a0d28fb8-0d0f-4354-9720-79ce468b5ea8
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC # Databricks PySpark basics
-- MAGIC

-- COMMAND ----------

-- DBTITLE 0,--i18n-9abfecfc-df3f-4697-8880-bd3f0b58a864
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Query a Single File
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df1 = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/apostolos1927@gmail.com/testdata.csv")
-- MAGIC df1.display()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df2 = spark.read.format("json").option("multiline","true").load("dbfs:/FileStore/shared_uploads/apostolos1927@gmail.com/data_json.json")
-- MAGIC df2.display()

-- COMMAND ----------

-- DBTITLE 0,--i18n-0f45ecb7-4024-4798-a9b8-e46ac939b2f7
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Query a Directory from DataLake

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.conf.set(
-- MAGIC     "fs.azure.account.key.apostorageaccount.dfs.core.windows.net",
-- MAGIC     "QPfuDpirnesR0tC9MHNC3fLhYlt27xCxQRt4RR4G9b1sHHFzlkI321tpGDX5CzpnCuli0DhQCoKG+AStnr5sGA==")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.read.format('csv').option("header", "true").load("abfss://input@apostorageaccount.dfs.core.windows.net/testdata.csv")
-- MAGIC df.display()

-- COMMAND ----------

SELECT * FROM csv.`abfss://input@apostorageaccount.dfs.core.windows.net/testdata.csv`

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS test_csv
 (ID INT, AGE INT, CITY VARCHAR(20))
USING CSV
OPTIONS (
  header = "true",
  delimiter = ","
)
LOCATION "dbfs:/FileStore/shared_uploads/apostolos1927@gmail.com/testdata.csv"

-- COMMAND ----------

SELECT * FROM test_csv

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql(f"""
-- MAGIC CREATE TABLE IF NOT EXISTS test_csv2
-- MAGIC   (id INT, age INT)
-- MAGIC USING CSV
-- MAGIC OPTIONS (
-- MAGIC   header = "true",
-- MAGIC   delimiter = ","
-- MAGIC )
-- MAGIC LOCATION "dbfs:/FileStore/shared_uploads/apostolos1927@gmail.com/testdata.csv"
-- MAGIC """)

-- COMMAND ----------

SELECT * FROM test_csv2

-- COMMAND ----------

-- DBTITLE 0,--i18n-035ddfa2-76af-4e5e-a387-71f26f8c7f76
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Create References to Files

-- COMMAND ----------

CREATE OR REPLACE VIEW view_test
AS SELECT * FROM csv.`dbfs:/FileStore/shared_uploads/apostolos1927@gmail.com/testdata.csv`

-- COMMAND ----------

SELECT * FROM view_test

-- COMMAND ----------

-- DBTITLE 0,--i18n-efd0c0fc-5346-4275-b083-4ee96ce8a852
-- MAGIC %md
-- MAGIC ## Create Temporary References to Files

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW temp_view
AS SELECT * FROM csv.`dbfs:/FileStore/shared_uploads/apostolos1927@gmail.com/testdata.csv`

-- COMMAND ----------

-- DBTITLE 0,--i18n-a9f9827b-2258-4481-a9d9-6fecf55aeb9b
-- MAGIC %md
-- MAGIC
-- MAGIC Temporary views exists only for the current SparkSession. On Databricks, this means they are isolated to the current notebook, job, or DBSQL query.

-- COMMAND ----------

SELECT * FROM temp_view

-- COMMAND ----------

-- DBTITLE 0,--i18n-dcfaeef2-0c3b-4782-90a6-5e0332dba614
-- MAGIC %md
-- MAGIC ## Apply CTEs for Reference within a Query 
-- MAGIC Common table expressions (CTEs) 

-- COMMAND ----------

WITH cte_json
AS (SELECT * FROM csv.`dbfs:/FileStore/shared_uploads/apostolos1927@gmail.com/testdata.csv`)
SELECT * FROM cte_json

-- COMMAND ----------

SELECT * FROM cte_json

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Extracting Data from SQL Databases
-- MAGIC
-- MAGIC
-- MAGIC <strong><code>
-- MAGIC CREATE TABLE <jdbcTable><br/>
-- MAGIC USING JDBC<br/>
-- MAGIC OPTIONS (<br/>
-- MAGIC &nbsp; &nbsp; url = "jdbc:{databaseServerType}://{jdbcHostname}:{jdbcPort}",<br/>
-- MAGIC &nbsp; &nbsp; dbtable = "{jdbcDatabase}.table",<br/>
-- MAGIC &nbsp; &nbsp; user = "{jdbcUsername}",<br/>
-- MAGIC &nbsp; &nbsp; password = "{jdbcPassword}"<br/>
-- MAGIC )
-- MAGIC </code></strong>
-- MAGIC
-- MAGIC <img src="https://files.training.databricks.com/images/icon_warn_24.png"> **WARNING**: The backend-configuration of the JDBC server assumes you are running this notebook on a single-node cluster. If you are running on a cluster with multiple workers, the client running in the executors will not be able to connect to the driver.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dfdb = (spark.read
-- MAGIC   .format("sqlserver")
-- MAGIC   .option("host", "aposervertest.database.windows.net")
-- MAGIC   .option("port", "1433") 
-- MAGIC   .option("user", "")
-- MAGIC   .option("password", "")
-- MAGIC   .option("database", "apodbtest")
-- MAGIC   .option("dbtable", "dbo.Persons") 
-- MAGIC   .load()
-- MAGIC )
-- MAGIC dfdb.display()

-- COMMAND ----------


CREATE SCHEMA IF NOT EXISTS test123;
USE test123;
DROP TABLE IF EXISTS Apotest123;
CREATE TABLE Apotest123
USING sqlserver
OPTIONS (
  dbtable 'dbo.Persons',
  host 'aposervertest.database.windows.net',
  port '1433',
  database 'apodbtest',
  user '',
  password ''
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC DOES NOT SUPPORT UPDATES/DELETES

-- COMMAND ----------

UPDATE Apotest123
SET City='Nashville'
WHERE PersonID=1

-- COMMAND ----------

DELETE FROM Apotest123 WHERE PersonID=1

-- COMMAND ----------

SELECT * FROM Apotest123

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Inspect Missing Data
-- MAGIC

-- COMMAND ----------

SELECT count_if(City IS NULL) FROM Apotest123;
SELECT count(*) FROM Apotest123 WHERE City IS NULL;

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC from pyspark.sql.functions import col
-- MAGIC usersDF = spark.read.table("Apotest123")
-- MAGIC usersDF.where(col("City").isNull()).count()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC   
-- MAGIC ## Deduplicate Rows Based on Specific Columns with Group By

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW deduped_Apo AS 
SELECT PersonID, min(LastName) AS LastName
FROM Apotest123
WHERE City IS NOT NULL
GROUP BY PersonID;

SELECT * FROM deduped_Apo;

-- COMMAND ----------

SELECT * FROM (
SELECT *, ROW_NUMBER() OVER (PARTITION BY PersonID ORDER BY LastName ASC) AS RN
FROM Apotest123
) WHERE RN=1

--SELECT * FROM deduped_Apo;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import max
-- MAGIC dedupedDF = (dfdb
-- MAGIC     .where(col("City").isNotNull())
-- MAGIC     .groupBy("PersonID")
-- MAGIC     .agg(max("LastName").alias("LastName"))
-- MAGIC     )
-- MAGIC
-- MAGIC dedupedDF.display()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Validate Datasets

-- COMMAND ----------

SELECT max(row_count) <= 1 no_duplicate_ids FROM (
  SELECT PersonID, count(*) AS row_count
  FROM deduped_Apo
  GROUP BY PersonID)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import count
-- MAGIC
-- MAGIC display(dedupedDF
-- MAGIC     .groupBy("PersonID")
-- MAGIC     .agg(count("*").alias("row_count"))
-- MAGIC     .select((max("row_count") <= 1).alias("no_duplicate_ids")))
