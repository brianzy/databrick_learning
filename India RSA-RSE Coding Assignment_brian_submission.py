# Databricks notebook source
# MAGIC %md # **RSE/RSA Coding Challengs**
# MAGIC 
# MAGIC ## Exam Instructions
# MAGIC 
# MAGIC  1. Sign up to this databricks community edition in https://community.cloud.databricks.com. Import this html notebook to your home folder. It will automatically be imported as a Scala notebook.
# MAGIC  1. This exam has various questions in 9 different Sections. Unless otherwise instructed, solve as many of the problems below as you can within the alloted time frame. Some of the challenges are more advanced than others and are expected to take more time to solve. Please note that Section 9 is Mandatory to answer.
# MAGIC  1. You can create as many notebooks as you would like to answer the challenges 
# MAGIC  1. Notebooks should be presentable and should be able to execute succesfully with `Run All`
# MAGIC  1. Notebooks should be idempotent as well. Ideally, you'll also clean up after yourself (i.e. drop your tables)
# MAGIC  1. Once completed, export your notebook(s) as *.html file with full results and email to ``vgiri@databricks.com`` (Giri Varatharajan) cc the Recruiting and Hiring team.
# MAGIC  1. Please note answers won't be accpeted if they were copied as it is from online sources.

# COMMAND ----------

# MAGIC %md  # Tips and Instructions
# MAGIC Read through the Databricks Guide for Notebook Usage and Other usage instructions in http://docs.databricks.com/

# COMMAND ----------

# MAGIC %md ####  Available  Context objects
# MAGIC spark object is available in databricks notebook by default. Please do not explicitly create sc, spark or sqlContext.

# COMMAND ----------

# MAGIC %scala
# MAGIC spark

# COMMAND ----------

# MAGIC %md #### Using SQL in your cells
# MAGIC 
# MAGIC You can change to native SQL mode in your cells using the `%sql` prefix, demonstrated in the example below. Note that these include visualizations by default.

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from diamonds limit 50

# COMMAND ----------

# MAGIC %md #### Using Python in your cells

# COMMAND ----------

# MAGIC %python
# MAGIC import numpy as np

# COMMAND ----------

# MAGIC %md #### Using Scala in your cells

# COMMAND ----------

# MAGIC %scala
# MAGIC val a = sc.parallelize(1 to 5).take(5)

# COMMAND ----------

# MAGIC %md #### Creating Visualizations from non-SQL Cells
# MAGIC 
# MAGIC When you needs to create a visualization from a cell where you are not writing native SQL, use the `display` function, as demonstrated below.

# COMMAND ----------

# MAGIC %scala
# MAGIC val same_query_as_above = spark.sql("select cut, count(color) as cnt from diamonds group by cut, color ")
# MAGIC display(same_query_as_above)

# COMMAND ----------

# MAGIC %md ##                                       Coding Challenges Starts Here!!..Wish you All the Best!!
# MAGIC ---
# MAGIC 
# MAGIC # Section 1 = > TPC-H Dataset
# MAGIC You're provided with a TPCH data set. The data is located in `/databricks-datasets/tpch/data-001/`. You can see the directory structure below:

# COMMAND ----------

# MAGIC %scala
# MAGIC display(dbutils.fs.ls("/databricks-datasets/tpch/data-001/"))

# COMMAND ----------

# MAGIC %python
# MAGIC #//reading readme file
# MAGIC with  open("/dbfs/databricks-datasets/tpch/data-001/README.md", "r") as f_read:
# MAGIC   for line in f_read:
# MAGIC     print(line)

# COMMAND ----------

# MAGIC %md As you can see above, this dataset consists of 8 different folders with different datasets. The schema of each dataset is demonstrated below: 

# COMMAND ----------

# MAGIC %md ![test](http://kejser.org/wp-content/uploads/2014/06/image_thumb2.png)

# COMMAND ----------

# MAGIC %md You can take a quick look at each dataset by running the following Spark commmand. Feel free to explore and get familiar with this dataset

# COMMAND ----------

# MAGIC %scala
# MAGIC //have peak in partsupp data
# MAGIC sc.textFile("/databricks-datasets/tpch/data-001/partsupp/").take(100).foreach(println)

# COMMAND ----------

# MAGIC %scala
# MAGIC //have peak in part data
# MAGIC sc.textFile("/databricks-datasets/tpch/data-001/part/").take(100).foreach(println)

# COMMAND ----------

# MAGIC %md #### **Question #1**: Joins in Core Spark
# MAGIC Pick any two datasets and join them using Spark's API. Feel free to pick any two datasets. For example: `PART` and `PARTSUPP`. The goal of this exercise is not to derive anything meaningful out of this data but to demonstrate how to use Spark to join two datasets. For this problem you're **NOT allowed to use SparkSQL**. You can only use RDD API. You can use either Python or Scala to solve this problem.

# COMMAND ----------

# MAGIC %python
# MAGIC # Answer here for Section1/Question#1 or create required cells here 
# MAGIC 
# MAGIC 
# MAGIC partsupp = spark.read.option("sep", "|").csv("/databricks-datasets/tpch/data-001/partsupp/")
# MAGIC # drop off the last null column do not have time to tuning read csv command
# MAGIC partsupp = partsupp.drop('_c5')
# MAGIC #partsupp.show(100)
# MAGIC part = spark.read.option("sep", "|").csv("/databricks-datasets/tpch/data-001/part/")
# MAGIC part = part.drop('_c9')
# MAGIC #part.show(100)
# MAGIC 
# MAGIC join_part = partsupp.join(part,part["_c0"] == partsupp["_c0"])
# MAGIC join_part.show(100)

# COMMAND ----------

# MAGIC %md #### **Question #2**: Joins With Spark SQL
# MAGIC Pick any two datasets and join them using SparkSQL API. Feel free to pick any two datasets. For example: PART and PARTSUPP. The goal of this exercise is not to derive anything meaningful out of this data but to demonstrate how to use Spark to join two datasets. For this problem you're **NOT allowed to use the RDD API**. You can only use SparkSQL API. You can use either Python or Scala to solve this problem. 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Answer here for Section1/Question#2 or create required cells here 
# MAGIC CREATE TABLE partsupp_temp
# MAGIC USING CSV
# MAGIC OPTIONS (path="/databricks-datasets/tpch/data-001/partsupp/", header = "false", delimiter='|')

# COMMAND ----------

# MAGIC %sql
# MAGIC Drop table part_temp;
# MAGIC CREATE TABLE part_temp
# MAGIC USING CSV
# MAGIC OPTIONS (path="/databricks-datasets/tpch/data-001/part/", header = "false", delimiter='|');

# COMMAND ----------

# MAGIC %sql
# MAGIC Drop table partsupp;
# MAGIC CREATE TABLE partsupp
# MAGIC AS
# MAGIC SELECT _c0, _c1, _c2, _c3
# MAGIC FROM partsupp_temp;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE part;
# MAGIC CREATE TABLE part
# MAGIC AS
# MAGIC SELECT _c0, _c1, _c2, _c3, _c4, _c5, _c6, _c7, _c8
# MAGIC FROM part_temp;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM part
# MAGIC JOIN partsupp
# MAGIC ON part._c0 = partsupp._c0

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*)
# MAGIC from partsupp

# COMMAND ----------

# MAGIC %md #### **Question #3**: Alternate Data Formats
# MAGIC The given dataset above is in raw text storage format. What other data storage format can you suggest to optimize the performance of our Spark workload if we were to frequently scan and read this dataset. Please come up with a code example and explain why you decide to go with this approach. Please note that there's no completely correct answer here. We're interested to hear your thoughts and see the implementation details.shell/1282

# COMMAND ----------

# MAGIC %md
# MAGIC if we are talking about frequently scan and read this dataset, I probably will choose to store those file into parquet format instead of storing them into raw text file. 
# MAGIC parquet is column-oriented binary file format By their very nature, column-oriented datastores are optimized for read-heavy analytical workloads. if the requirement is heavily wirting file
# MAGIC I will choose Avro format instead of parquet
# MAGIC for how to convert into parquet file. I will still using readcsv function because this raw text file still contain delimiter, change into pyspark dataframe and write into parquet file

# COMMAND ----------

# MAGIC %fs rm -r /tmp/out/df.parquet

# COMMAND ----------

# MAGIC %python
# MAGIC # //Answer here for Section1/Question#3 or create required cells here 
# MAGIC df = spark.read.option("sep", "|").csv("/databricks-datasets/tpch/data-001/customer/")
# MAGIC 
# MAGIC df.write.parquet("/tmp/out/df.parquet") 

# COMMAND ----------

# MAGIC %md # Section 2 = > Baby Names Dataset
# MAGIC 
# MAGIC This dataset comes from a website referenced by [Data.gov](http://catalog.data.gov/dataset/baby-names-beginning-2007). It lists baby names used in the state of NY from 2007 to 2012. Use this JSON file as an input and answer the 3 questions accordingly.
# MAGIC 
# MAGIC https://health.data.ny.gov/api/views/jxy9-yhdk/rows.json

# COMMAND ----------

# MAGIC %md #### **Question #1**: Spark SQL's Native JSON Support
# MAGIC Use Spark SQL's native JSON support to create a temp table you can use to query the data (you'll use the `registerTempTable` operation). Show a simple sample query.

# COMMAND ----------

# MAGIC %python
# MAGIC # -- //Answer here for Section2/Question#1 or create required cells here 
# MAGIC import urllib.request
# MAGIC import json 
# MAGIC import pandas as pd
# MAGIC 
# MAGIC 
# MAGIC url = 'https://health.data.ny.gov/api/views/jxy9-yhdk/rows.json'
# MAGIC with urllib.request.urlopen(url) as urls:
# MAGIC     data = json.load(urls)
# MAGIC data =  data['data']
# MAGIC headers = ['sid','id','position','created_at','created_meta','updated_at','updated_meta','meta','Year','First Name','county','sex','count']
# MAGIC pdf = pd.DataFrame(data = data, columns=headers)
# MAGIC pdf['count'] = pd.to_numeric(pdf['count'])
# MAGIC pdf['Year'] = pd.to_numeric(pdf['Year'])
# MAGIC temp = spark.createDataFrame(pdf)
# MAGIC temp.show(10)
# MAGIC temp.registerTempTable('temp')

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from temp
# MAGIC limit 10

# COMMAND ----------

# MAGIC %md #### **Question #2**: Working with Nested Data
# MAGIC What does the nested schema of this dataset look like? How can you bring these nested fields up to the top level in a DataFrame?

# COMMAND ----------

# MAGIC %md
# MAGIC ## Answer #2
# MAGIC This json file have two part, meta field and data field. the nested json schema fields only for meta field, the real data we care about is located in data field. This data showed as list in list. we have to find a way to parsing those data. in my answer in first section, I have to change those data from list in list into pandas dataframe. And then changed back to pyspark dataframe. The reason is I have to put all the json data into dictionary because the `spark.read.json()` function cannot take the whole url json data. I have to change it to other format to sucessfully covert into pyspark dataframe.

# COMMAND ----------

# MAGIC %md #### **Question #3**: Executing Full Data Pipelines
# MAGIC Create a second version of the answer to Question 2, and make sure one of your queries makes the original web call every time a query is run, while another version only executes the web call one time.

# COMMAND ----------

# MAGIC %python
# MAGIC # //Answer here for Section2/Question#3 or create required cells here 
# MAGIC ## version_2 try only web call once for the very first time
# MAGIC import urllib.request
# MAGIC import json 
# MAGIC import pandas as pd
# MAGIC 
# MAGIC def parse_json_to_df(url,headers):
# MAGIC   with urllib.request.urlopen(url) as urls:
# MAGIC     data = json.load(urls)
# MAGIC   data =  data['data']
# MAGIC   pdf = pd.DataFrame(data = data, columns=headers)
# MAGIC   pdf['count'] = pd.to_numeric(pdf['count'])
# MAGIC   pdf['Year'] = pd.to_numeric(pdf['Year'])
# MAGIC   temp = spark.createDataFrame(pdf)
# MAGIC   # Convert the created_meta column to a string data type
# MAGIC   temp = temp.withColumn("created_meta", col("created_meta").cast("string"))
# MAGIC   temp = temp.withColumn("updated_meta", col("updated_meta").cast("string"))
# MAGIC   return temp
# MAGIC 
# MAGIC def df_to_parquet(df,path):
# MAGIC   df.write.parquet(path)
# MAGIC   
# MAGIC def read_parquet(path):
# MAGIC   temp =  spark.read.parquet(path)
# MAGIC   return temp
# MAGIC 
# MAGIC def check_file(path):
# MAGIC     try:
# MAGIC         dbutils.fs.ls(path)
# MAGIC         return True
# MAGIC     except Exception as e:
# MAGIC         return False
# MAGIC 
# MAGIC   
# MAGIC   
# MAGIC url = 'https://health.data.ny.gov/api/views/jxy9-yhdk/rows.json'
# MAGIC headers = ['sid','id','position','created_at','created_meta','updated_at','updated_meta','meta','Year','First Name','county','sex','count']
# MAGIC path = '/tmp/out/json_p.parquet'
# MAGIC if check_file(path):
# MAGIC   print(f'pipeline reading from parquet file')
# MAGIC   temp = read_parquet(path)
# MAGIC   temp.show(10)
# MAGIC else:
# MAGIC   print(f'pipeline reading from webcall')
# MAGIC   temp = parse_json_to_df(url,headers)
# MAGIC   temp.show(10)
# MAGIC   df_to_parquet(temp,path)

# COMMAND ----------

# MAGIC %md #### **Question #4**: Analyzing the Data
# MAGIC 
# MAGIC Using the tables you created, create a simple visualization that shows what is the most popular first letters baby names to start with in each year.

# COMMAND ----------

# MAGIC %python
# MAGIC # //Answer here for Section2/Question#4 or create required cells here 
# MAGIC from pyspark.sql.functions import col
# MAGIC 
# MAGIC temp_first = temp.withColumn("first_letter", col("`first name`").substr(1,1))
# MAGIC #print(temp_first.schema)
# MAGIC sum_count = temp_first.groupby('Year','first_letter').sum('Count').withColumnRenamed('sum(Count)', 'l_count').sort('Year')
# MAGIC max_count = sum_count.groupby('Year').max('l_count').withColumnRenamed('max(l_count)', 'max_count').withColumnRenamed('Year', 'Year1')
# MAGIC final =sum_count.join(max_count).where(sum_count['l_count'] == max_count['max_count']).select(sum_count['Year'],sum_count['first_letter'], max_count['max_count']).sort('year')
# MAGIC final.show()

# COMMAND ----------

# MAGIC %md # Section 3 => Log Processing
# MAGIC 
# MAGIC The following data comes from the _Learning Spark_ book.

# COMMAND ----------

# MAGIC %scala
# MAGIC display(dbutils.fs.ls("/databricks-datasets/learning-spark/data-001/fake_logs"))

# COMMAND ----------

# MAGIC %scala
# MAGIC println(dbutils.fs.head("/databricks-datasets/learning-spark/data-001/fake_logs/log1.log"))

# COMMAND ----------

# MAGIC %scala
# MAGIC sc.textFile("/databricks-datasets/learning-spark/data-001/fake_logs/log2.log/").take(100).foreach(println)

# COMMAND ----------

# MAGIC %md #### **Question #1**: Parsing Logs
# MAGIC Parse the logs in to a DataFrame/Spark SQL table that can be queried. This should be done using the Dataset API.

# COMMAND ----------

# MAGIC %python
# MAGIC ! pip install apachelogs
# MAGIC #//Answer here for Section3/Question#1 or create required cells here 
# MAGIC from apachelogs import LogParser
# MAGIC from pyspark.sql.functions import udf
# MAGIC from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, TimestampType
# MAGIC 
# MAGIC log_raw =  spark.read.text('/databricks-datasets/learning-spark/data-001/fake_logs/log1.log')
# MAGIC log_raw_2 = spark.read.text('/databricks-datasets/learning-spark/data-001/fake_logs/log2.log')
# MAGIC log_raw = log_raw.union(log_raw_2)
# MAGIC 
# MAGIC def read_log(log_text):
# MAGIC     """
# MAGIC     Function to parse log
# MAGIC     """
# MAGIC     log_format = "%h %l %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-Agent}i\""
# MAGIC     parser = LogParser(log_format)
# MAGIC     log = parser.parse(log_text)
# MAGIC     return (log.remote_host,
# MAGIC             log.request_time,
# MAGIC             log.request_line,
# MAGIC             log.final_status,
# MAGIC             log.bytes_sent,
# MAGIC             log.headers_in["User-Agent"])
# MAGIC 
# MAGIC 
# MAGIC # Schema
# MAGIC log_schema = StructType([
# MAGIC     StructField("remote_host", StringType(), False),
# MAGIC     StructField("request_time", TimestampType(), False),
# MAGIC     StructField("request_line", StringType(), False),
# MAGIC     StructField("final_status", IntegerType(), False),
# MAGIC     StructField("bytes_sent", LongType(), False),
# MAGIC     StructField("user_agent", StringType(), False)
# MAGIC ])
# MAGIC udf_read_log = udf(read_log, log_schema)
# MAGIC 
# MAGIC # Now use UDF to transform Spark DataFrame
# MAGIC 
# MAGIC df = log_raw.withColumn('log', udf_read_log(log_raw['value']))
# MAGIC df = df.withColumnRenamed('value', 'raw_log')
# MAGIC df_log = df.select("raw_log", 'log.*')
# MAGIC print(df.schema)
# MAGIC df_log.show()

# COMMAND ----------

# MAGIC %md #### **Question #2**: Analysis
# MAGIC Generate some insights from the log data.

# COMMAND ----------

# MAGIC %python
# MAGIC #//Answer here for Section3/Question#2 or create required cells here
# MAGIC # log status code count
# MAGIC df_log.groupby('final_status').count().show()
# MAGIC # which log has max and min sent bytes
# MAGIC df_log.orderBy('bytes_sent').show()

# COMMAND ----------

# MAGIC %md # Section 4 => CSV Parsing
# MAGIC The following examples involove working with simple CSV data

# COMMAND ----------

# MAGIC %scala
# MAGIC val full_csv = sc.parallelize(Array(
# MAGIC   "col_1, col_2, col_3",
# MAGIC   "1, ABC, Foo1",
# MAGIC   "2, ABCD, Foo2",
# MAGIC   "3, ABCDE, Foo3",
# MAGIC   "4, ABCDEF, Foo4",
# MAGIC   "5, DEF, Foo5",
# MAGIC   
# MAGIC   "6, DEFGHI, Foo6",
# MAGIC   "7, GHI, Foo7",
# MAGIC   "8, GHIJKL, Foo8",
# MAGIC   "9, JKLMNO, Foo9",
# MAGIC   "10, MNO, Foo10"))

# COMMAND ----------

# MAGIC %md #### **Question #1**: CSV Header Rows
# MAGIC Given the simple RDD `full_csv` below, write the most efficient Spark job you can to remove the header row

# COMMAND ----------

# MAGIC %scala
# MAGIC //Answer here for Section4/Question#1 or create required cells here 
# MAGIC val header = full_csv.first() // get the header row
# MAGIC val data = full_csv.filter(row => row != header)// filter out the header row
# MAGIC 
# MAGIC data.collect().foreach(println)

# COMMAND ----------

# MAGIC %md #### **Question #2**: SparkSQL Dataframes
# MAGIC Using the `full_csv` RDD above, write code that results in a DataFrame where the schema was created programmatically based on the heard row. Create a second RDD similair to `full_csv` and uses the same function(s) you created in this step to make a Dataframe for it.

# COMMAND ----------

# MAGIC %scala
# MAGIC //Answer here for Section4/Question#2 or create required cells here 
# MAGIC import org.apache.spark.sql.types.{StructType, StructField, StringType}
# MAGIC 
# MAGIC // create the schema based on the header row
# MAGIC val header = full_csv.first()
# MAGIC val schema = StructType(header.split(",").map(fieldName => StructField(fieldName.trim, StringType, true)))
# MAGIC 
# MAGIC // create the DataFrame using the schema
# MAGIC val data = full_csv.filter(row => row != header)
# MAGIC val dataRows = data.map(row => row.split(",").map(_.trim))
# MAGIC val dataRDD = dataRows.map(row => Row.fromSeq(row))
# MAGIC val df = spark.createDataFrame(dataRDD, schema)
# MAGIC df.show()

# COMMAND ----------

# MAGIC %md #### **Question #3**: Parsing Pairs
# MAGIC 
# MAGIC Write a Spark job that processes comma-seperated lines that look like the below example to pull out Key Value pairs. 
# MAGIC 
# MAGIC Given the following data:
# MAGIC 
# MAGIC ~~~
# MAGIC Row-Key-001, K1, 10, A2, 20, K3, 30, B4, 42, K5, 19, C20, 20
# MAGIC Row-Key-002, X1, 20, Y6, 10, Z15, 35, X16, 42
# MAGIC Row-Key-003, L4, 30, M10, 5, N12, 38, O14, 41, P13, 8
# MAGIC ~~~
# MAGIC 
# MAGIC You'll want to create an RDD that contains the following data:
# MAGIC 
# MAGIC ~~~
# MAGIC Row-Key-001, K1
# MAGIC Row-Key-001, A2
# MAGIC Row-Key-001, K3
# MAGIC Row-Key-001, B4
# MAGIC Row-Key-001, K5
# MAGIC Row-Key-001, C20
# MAGIC Row-Key-002, X1
# MAGIC Row-Key-002, Y6
# MAGIC Row-Key-002, Z15
# MAGIC Row-Key-002, X16
# MAGIC Row-Key-003, L4
# MAGIC Row-Key-003, M10
# MAGIC Row-Key-003, N12
# MAGIC Row-Key-003, O14
# MAGIC Row-Key-003, P13
# MAGIC ~~~

# COMMAND ----------

# MAGIC %scala
# MAGIC //Answer here for Section4/Question#3 or create required cells here 
# MAGIC val data = sc.parallelize(List(
# MAGIC   "Row-Key-001, K1, 10, A2, 20, K3, 30, B4, 42, K5, 19, C20, 20",
# MAGIC   "Row-Key-002, X1, 20, Y6, 10, Z15, 35, X16, 42",
# MAGIC   "Row-Key-003, L4, 30, M10, 5, N12, 38, O14, 41, P13, 8"
# MAGIC ))
# MAGIC 
# MAGIC val keyValuePairs = data.flatMap(line => {
# MAGIC   val parts = line.split(",")
# MAGIC   val key = parts(0)
# MAGIC   val values = parts.drop(1)
# MAGIC   val pairs = values.grouped(2).map(pair => (key, pair(0))).toList
# MAGIC   pairs
# MAGIC })
# MAGIC 
# MAGIC keyValuePairs.collect().foreach(println)

# COMMAND ----------

# MAGIC %md # Section 5 => Connecting to JDBC Database
# MAGIC 
# MAGIC Write a Spark job that queries MySQL using its JDBC Driver.
# MAGIC 
# MAGIC #### Load your JDBC Driver onto Databricks
# MAGIC  * Databricks comes preloaded with JDBC libraries for **mysql**, but you can attach other JDBC libraries and reference them in your code
# MAGIC  * See our [Libraries Notebook](/#workspace/databricks_guide/02 Product Overview/04 Libraries) for instructions on how to install a Java JAR.

# COMMAND ----------

# MAGIC %md
# MAGIC I cannot finish this section due to Azure personal trail limitation. Running the smallest cluster for this workspace used all my trail source and I cannot create sql dataware house to connect.
# MAGIC I did the steps below
# MAGIC 1. I downloaded JDBC file in [databrick website](https://www.databricks.com/spark/jdbc-drivers-download)
# MAGIC 2. I upload this drive into the cluster and installed it.
# MAGIC 3. I find the connection details in Starter Warehouse page but cannot start the warehouse cluster.
# MAGIC ![](/files/error.png)

# COMMAND ----------

# MAGIC %sql
# MAGIC create table my_table2 as
# MAGIC select name from my_table;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from my_table2

# COMMAND ----------

# MAGIC %python
# MAGIC #//Answer here for Section 5 or create required cells here 
# MAGIC from pyspark.sql.types import *
# MAGIC 
# MAGIC url = 'jdbc:databricks://adb-1332124237736688.8.azuredatabricks.net:443/default;transportMode=http;ssl=1;httpPath=sql/protocolv1/o/1332124237736688/0216-182825-j7yyd3qp;AuthMech=3;UID=token;PWD=dapic35fddcf261d962fe82546ce43ca0650-3'
# MAGIC 
# MAGIC driver_class = 'com.databricks.client.jdbc.Driver'
# MAGIC # create a DataFrame by querying the MySQL database
# MAGIC df_1 = spark.read.format("jdbc")\
# MAGIC .option("url", url)\
# MAGIC .option("driver", driver_class)\
# MAGIC .option("dbtable", 'partsupp')\
# MAGIC .load()
# MAGIC 
# MAGIC df_1.show()

# COMMAND ----------

# MAGIC %md # Section 6 => Create Tables Programmatically And Cache The Table
# MAGIC 
# MAGIC Create a table using Scala or Python
# MAGIC 
# MAGIC * Use `CREATE EXTERNAL TABLE` in SQL, or `DataFrame.saveAsTable()` in Scala or Python, to register tables.
# MAGIC * Please refer to the [Accessing Data](/#workspace/databricks_guide/03 Accessing Data/0 Accessing Data) guide for how to import specific data types.
# MAGIC 
# MAGIC #### Temporary Tables
# MAGIC * Within each Spark cluster, temporary tables registered in the `sqlContext` with `DataFrame.registerTempTable` will also be shared across the notebooks attached to that Databricks cluster.
# MAGIC   * Run `someDataFrame.registerTempTable(TEMP_TABLE_NAME)` to give register a table.
# MAGIC * These tables will not be visible in the left-hand menu, but can be accessed by name in SQL and DataFrames.

# COMMAND ----------

# MAGIC %python
# MAGIC #//Answer here for Section 6 or create required cells here 
# MAGIC from pyspark.sql.types import *
# MAGIC 
# MAGIC mySchema = StructType([
# MAGIC     StructField("id", IntegerType(), True),
# MAGIC     StructField("name", StringType(), True)
# MAGIC ])
# MAGIC 
# MAGIC myData = [(1, "John"), (2, "Jane"), (3, "Bob")]
# MAGIC myDataFrame = spark.createDataFrame(myData, schema=mySchema)
# MAGIC 
# MAGIC myDataFrame.write.format("parquet").saveAsTable("my_table")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from my_table

# COMMAND ----------

# MAGIC %md # Section 7 => DataFrame UDFs and DataFrame SparkSQL Functions
# MAGIC 
# MAGIC Below we've created a small DataFrame. You should use DataFrame API functions and UDFs to accomplish two tasks.
# MAGIC 
# MAGIC 1. You need to parse the State and city into two different columns.
# MAGIC 2. You need to get the number of days in between the start and end dates. You need to do this two ways.
# MAGIC   - Firstly, you should use SparkSQL functions to get this date difference.
# MAGIC   - Secondly, you should write a udf that gets the number of days between the end date and the start date.

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC from pyspark.sql import functions as F
# MAGIC from pyspark.sql.types import *
# MAGIC 
# MAGIC # Build an example DataFrame dataset to work with. 
# MAGIC dbutils.fs.rm("/tmp/dataframe_sample.csv", True)
# MAGIC dbutils.fs.put("/tmp/dataframe_sample.csv", """id|end_date|start_date|location
# MAGIC 1|2015-10-14 00:00:00|2015-09-14 00:00:00|CA-SF
# MAGIC 2|2015-10-15 01:00:20|2015-08-14 00:00:00|CA-SD
# MAGIC 3|2015-10-16 02:30:00|2015-01-14 00:00:00|NY-NY
# MAGIC 4|2015-10-17 03:00:20|2015-02-14 00:00:00|NY-NY
# MAGIC 5|2015-10-18 04:30:00|2014-04-14 00:00:00|CA-SD
# MAGIC """, True)
# MAGIC 
# MAGIC formatPackage = "csv" if sc.version > '1.6' else "com.databricks.spark.csv"
# MAGIC df = sqlContext.read.format(formatPackage).options(header='true', delimiter = '|').load("/tmp/dataframe_sample.csv")
# MAGIC df.show()
# MAGIC df.printSchema()

# COMMAND ----------

# MAGIC %python
# MAGIC #Answer here for Section 7 or create required cells here 
# MAGIC #parse location
# MAGIC new_col = ['state','city']
# MAGIC for i, c in enumerate(new_col):
# MAGIC   df =  df.withColumn(c, F.split('location', '-')[i])
# MAGIC df.show()
# MAGIC # create temp view for next question find date diff
# MAGIC df.createOrReplaceTempView("my_table2")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT id, datediff(end_date, start_date) AS days_diff, location
# MAGIC from my_table2

# COMMAND ----------

from datetime import datetime

def get_days_diff(start_date, end_date):
    start_date = datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S')
    end_date = datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S')
    return (end_date - start_date).days

days_udf = udf(get_days_diff, IntegerType())

df_diff = df.withColumn('days_diff', days_udf('start_date', 'end_date'))
df_diff.show()

# COMMAND ----------

# MAGIC %md # Section 8 => Machine Learning

# COMMAND ----------

# MAGIC %md #### **Question 1:** Demonstrate The Use of a MLlib Algorithm Using the DataFrame Interface(`org.apache.spark.ml`).
# MAGIC 
# MAGIC Demonstrate use of an MLlib algorithm and show an example of tuning the algorithm to improve prediction accuracy.
# MAGIC 
# MAGIC Use Decision Tree Example using Databricks MLib. 

# COMMAND ----------

# MAGIC %python
# MAGIC #//Answer here for Section 8 or create required cells here 
# MAGIC from pyspark.ml.feature import VectorAssembler, StringIndexer
# MAGIC from pyspark.ml import Pipeline
# MAGIC from pyspark.ml.classification import DecisionTreeClassifier
# MAGIC from pyspark.ml.evaluation import MulticlassClassificationEvaluator
# MAGIC from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
# MAGIC 
# MAGIC # Load Iris dataset
# MAGIC # famous sample iris dataset download url
# MAGIC # https://www.kaggle.com/datasets/saurabh00007/iriscsv?resource=download
# MAGIC path = "dbfs:/FileStore/Iris.csv"
# MAGIC data = spark.read.format("csv").option("header", "true").option("inferSchema" ,"True").load(path)
# MAGIC data.printSchema()
# MAGIC # Create feature vector
# MAGIC assembler = VectorAssembler(inputCols=["SepalLengthCm", "SepalWidthCm", "PetalLengthCm", "PetalWidthCm"], outputCol="features")
# MAGIC data = assembler.transform(data)
# MAGIC 
# MAGIC # Encode label column as numeric index
# MAGIC indexer = StringIndexer(inputCol="Species", outputCol="label")
# MAGIC data = indexer.fit(data).transform(data)
# MAGIC # Split data into training and testing sets
# MAGIC trainingData, testData = data.randomSplit([0.7, 0.3])
# MAGIC # Create Decision Tree classifier
# MAGIC dt = DecisionTreeClassifier(labelCol="label", featuresCol="features")
# MAGIC 
# MAGIC # Fit Decision Tree to training data
# MAGIC model = dt.fit(trainingData)
# MAGIC # Make predictions on test data
# MAGIC predictions = model.transform(testData)
# MAGIC 
# MAGIC # Evaluate performance
# MAGIC evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
# MAGIC accuracy = evaluator.evaluate(predictions)
# MAGIC print("Accuracy = %g" % accuracy)
# MAGIC 
# MAGIC # Define parameter grid
# MAGIC paramGrid = ParamGridBuilder() \
# MAGIC     .addGrid(dt.maxDepth, [2, 3, 4, 5, 6]) \
# MAGIC     .addGrid(dt.impurity, ['gini', 'entropy']) \
# MAGIC     .build()
# MAGIC 
# MAGIC # Perform cross validation
# MAGIC cv = CrossValidator(estimator=dt, estimatorParamMaps=paramGrid, evaluator=evaluator, numFolds=6)
# MAGIC cvModel = cv.fit(trainingData)
# MAGIC 
# MAGIC # Make predictions on test data using best model
# MAGIC bestModel = cvModel.bestModel
# MAGIC predictions = bestModel.transform(testData)
# MAGIC 
# MAGIC # Evaluate performance
# MAGIC accuracy = evaluator.evaluate(predictions)
# MAGIC print("Accuracy = %g" % accuracy)

# COMMAND ----------

# MAGIC %md # Section 9 => XML, Sql understanding, Streaming, Debugging Spark

# COMMAND ----------

# MAGIC %md #### 
# MAGIC **Question 1** : Create sample large xml data in your own with namespace and generate dataframe and do some DataSet transformations and actions. Also provide some meaningful insights from it.

# COMMAND ----------

# MAGIC %python
# MAGIC ! pip install lxml
# MAGIC from lxml import etree
# MAGIC 
# MAGIC # define an XML string with namespaces
# MAGIC xmlString = """
# MAGIC <employees xmlns="https://example.com/employees"
# MAGIC            xmlns:contact="https://example.com/contact"
# MAGIC            xmlns:address="https://example.com/address">
# MAGIC   <employee id="1">
# MAGIC     <name>John</name>
# MAGIC     <contact:email>john@example.com</contact:email>
# MAGIC     <address:city>New York</address:city>
# MAGIC   </employee>
# MAGIC   <employee id="2">
# MAGIC     <name>Bob</name>
# MAGIC     <contact:email>bob@example.com</contact:email>
# MAGIC     <address:city>San Francisco</address:city>
# MAGIC   </employee>
# MAGIC   <employee id="3">
# MAGIC     <name>Alice</name>
# MAGIC     <contact:email>alice@example.com</contact:email>
# MAGIC     <address:city>Los Angeles</address:city>
# MAGIC   </employee>
# MAGIC </employees>
# MAGIC """
# MAGIC 
# MAGIC # parse the XML string with lxml and extract data into a list of dictionaries
# MAGIC root = etree.fromstring(xmlString)
# MAGIC rows = []
# MAGIC for employee in root.findall("{https://example.com/employees}employee"):
# MAGIC     row = {
# MAGIC         "id": employee.get("id"),
# MAGIC         "name": employee.find("{https://example.com/employees}name").text,
# MAGIC         "email": employee.find("{https://example.com/contact}email").text,
# MAGIC         "city": employee.find("{https://example.com/address}city").text,
# MAGIC     }
# MAGIC     rows.append(row)
# MAGIC 
# MAGIC # create a DataFrame from the list of dictionaries
# MAGIC df = spark.createDataFrame(rows)
# MAGIC 
# MAGIC # perform some transformations and actions on the DataFrame
# MAGIC df = df.filter(df["city"] == "San Francisco")
# MAGIC 
# MAGIC df.show()

# COMMAND ----------

# MAGIC %md #### 
# MAGIC **Question 2** : You have the SQL query below to find employees who earn the top three salaries in each of the
# MAGIC department. Please write a pure Java/Scala code equivalent to this SQL code. Do not use direct SQL inside the java/scala code. But we need you to use your programming skills to answer the question

# COMMAND ----------

# MAGIC %sql 
# MAGIC WITH order_salary AS
# MAGIC ( 
# MAGIC SELECT dept.Name AS DeptName, emp.Name AS EmpName,emp.Salary AS Salary, RANK() OVER (PARTITION BY dept.Id ORDER BY emp.Salary DESC) OrderedRank
# MAGIC FROM Employee AS emp
# MAGIC INNER JOIN Department AS dept on dept.Id = emp.DepartmentId 
# MAGIC )
# MAGIC SELECT DeptName, EmpName, Salary FROM order_salary WHERE OrderedRank <= 3;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Note
# MAGIC * I won't  spend time creating actual database. I will just try to rewrite sparkSQL code above, the below answer has not been tested.

# COMMAND ----------

# MAGIC %scala
# MAGIC //Answer here for Section 9/Question 2 or create required cells here 
# MAGIC // Assuming we have a DataFrame for the "Employee" table called employeeDF and a DataFrame for the "Department" table called departmentDF
# MAGIC import org.apache.spark.sql.functions.rank
# MAGIC import org.apache.spark.sql.expressions.Window
# MAGIC 
# MAGIC val order_salary = employeeDF.join(departmentDF, employeeDF("DepartmentId") === departmentDF("Id"))
# MAGIC   .select(departmentDF("Name").alias("DeptName"), employeeDF("Name").alias("EmpName"), employeeDF("Salary"))
# MAGIC   .withColumn("OrderedRank", rank().over(Window.partitionBy("Id").orderBy($"Salary".desc)))
# MAGIC 
# MAGIC val result = order_salary.where($"OrderedRank" <= 3).select("DeptName", "EmpName", "Salary")

# COMMAND ----------

# MAGIC %md #### 
# MAGIC **Question 3** : There is a customer who reports that one of his spark job stalls at the last executor (399/400) for more than 1hr and executor logs are showing ‘ExecutorLost’ stacktrace and couple of retry attempts. What do you recommend as a next step(s)?

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Check the logs of the failed executor (executor 399) to see if there are any error messages or exceptions that could explain the issue. Look for any error messages or stack traces that may help you diagnose the problem.
# MAGIC 
# MAGIC 2. Check the resource utilization of the executor 399 during the period of the stall. This includes CPU usage, memory usage, and network bandwidth utilization. If the resource utilization is high, it could be an indication of a resource contention issue.
# MAGIC 
# MAGIC 3. Check the network connection between the driver and executor. If the network connection is slow or unreliable, it can cause data transfer failures, leading to job stalls.
# MAGIC 
# MAGIC 4. Check the cluster configuration, specifically the Spark settings and the resources allocated to the job. If the resources are insufficient or the Spark settings are not optimal, it can lead to job stalls and executor failures.
# MAGIC 
# MAGIC 5. If the problem persists, consider scaling up the cluster resources, increasing the number of executors, or increasing the resources allocated to the job.
# MAGIC 
# MAGIC 6. If none of the above steps help to resolve the issue, try to reproduce the problem in a test environment to isolate the issue further.

# COMMAND ----------

# MAGIC %md #### 
# MAGIC **Question 4** : Consider the following streaming job  What does the below code do?  How long will it retain the state? And why?
# MAGIC 
# MAGIC ```val streamingDf  = readStream()
# MAGIC streamingDf.withWatermark("eventTime","10 seconds").dropDuplicates("guid")
# MAGIC streamingDf.writeStream()```

# COMMAND ----------

# MAGIC %md
# MAGIC The code appears to define a streaming DataFrame using the readStream() method, which likely reads data from a streaming source. The data is then processed by dropping duplicates based on the "guid" column using the dropDuplicates() method. The DataFrame is also set with a watermark of "10 seconds" on the "eventTime" column using the withWatermark() method, which means that any data that arrives more than 10 seconds after its event time will be dropped. Finally, the streaming DataFrame is being written using the writeStream() method.
