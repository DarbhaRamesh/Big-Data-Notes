##### Spark SQL & Data frames

In spark core API's, we saw we chain transformations and call actions using RDD. RDD doesn't have any schema or metadata associated with it.

Creators of Spark recommend to use Higher level API's as lot of **optimizations** are present in spark engine.

**Spark SQL** 
- **data** will be present on **disk** - data lake & **metadata on meta store**
- **Spark Table is persistent**
- available across sessions

Data frames 
- RDD with some structure(schema/ metadata)
- **data & metadata - in memory**
- no meta store, your meta data is kept **temporarily** in metadata catalog.
- available only with in session

---
##### Data frame Reader

How to create a data frame?

``` python
orders_df = spark.read \
.format("csv") \
.option("header","true") \
.option("inferSchema","true") \
.load("/public/trendytech/orders_wh/*")
```
\
``` python
orders_df.show()
orders_df.show(5)
orders_df.show(20)
orders_df.printSchema()
```

It is **not recommended to use infer schema** as 
- it might not infer the schema correctly
- to infer schema, spark engine has to scan the data. Might lead to performance issues.

```python
renamed_orders_df = orders_df.withColumnRenamed("order_status","status")
renamed_orders_df.show(5)
```

```python
from pyspark.sql.functions import to_timestamp
changed_datatype_df=renamed_orders_df.withColumn("orders_date", to_timestamp("order_date"))
changed_datatype_df.printSchema()
```

There are various shortcut methods to read data from different sources/file formats to data frame but standard method is to use **read** method.

``` python
orders_csv_df = spark.read \
.csv("/public/trendytech/orders_wh/*", \
     header = "true", \
     inferSchema="true")
orders_csv_df.show(5)
```

``` python
orders_json_df = spark.read.json("/public/trendytech/datasets/orders.json")
orders_json_df.show(5)
orders_json_df.printSchema()
```

``` python
orders_parquet_df = spark.read.parquet("/public/trendytech/datasets/ordersparquet")
orders_parquet_df.show(5)
orders_parquet_df.printSchema()
```

Parquet is the best file format for spark. Its a columnar storage.

``` python
orders_orc_df = spark.read.orc("/public/trendytech/datasets/ordersorc")
orders_orc_df.show(5)
orders_orc_df.printSchema()
```

**Filtering data**

``` python
filtered_data = orders_df.where("customer_id = 11599")
filtered_data.show(truncate = False)
```

``` python
filtered_data_using_filter = orders_df.filter("customer_id = 11599")
filtered_data_using_filter.show()
```

**Spark Table/View**

``` python
orders_df.createOrReplaceTempView("orders")
filtered_using_spark_sql_df = spark.sql("select * from orders where order_status = 'CLOSED'")
filtered_using_spark_sql_df.show(5, truncate=False)
```

createOrReplaceTempView will create a spark table which is accessible in your session.
createOrReplaceGlobalTempView will create a spark table which can be accessible across multiple spark applications/notebooks/sessions

**Spark Table to Data Frame**

```python
orders_df_using_spark_sql = spark.read.table("orders")
orders_df_using_spark_sql.show(5)
```

---

##### Spark SQL

First we have to create database and then we can work with the database by creating tables and much more.

```python
spark.sql("create database if not exists itv014945_db")
spark.sql("show databases").show(5)
spark.sql("show databases").filter("namespace = 'itv014945_db'").show()
spark.sql("show databases").filter("namespace like 'itv014945_%'").show()
spark.sql("show tables").show(5) # shows results in default database
```

``` python
spark.sql("use itv014945_db") 
spark.sql("show tables").show(5)
spark.sql("create table itv014945_db.sampleTable(id integer, name string)")
spark.sql("show tables").show(5)
spark.sql("insert into itv014945_db.sampleTable values(1,'Big Data'),(2,'Spark')")
spark.sql("select * from itv014945_db.sampleTable")
spark.sql("describe table itv014945_db.sampleTable").show()
spark.sql("describe extended itv014945_db.sampleTable").show(truncate=False)
spark.sql("describe formatted itv014945_db.sampleTable").show(truncate=False)
```

The database will be created in HDFS and you can navigate to see the data which is stored in files

``` console
hadoop fs -ls -h warehouse/itv014945_db.db/sampletable/
```

![Spark SQL database in HDFS](../pictures/Spark_sql_database_in_hdfs.png "Spark SQL database in HDFSs")

---
##### Managed vs External Table

In Managed table, if you drop the table, both **data and metadata** will be whipped off.
In External table, if you drop the table, only **metadata** will be whipped off.

``` python
spark.sql("create table itv014945_db.orders_ext (order_id integer,order_date string,customer_id integer,order_status string) using csv location '/public/trendytech/orders/orders.csv'")
```

Insert will work on external table. Spark engine will create a new file if a directory is given in location but is not advisable to perform inserts on external tables

Update and delete wont work but will work in delta lake.

---


