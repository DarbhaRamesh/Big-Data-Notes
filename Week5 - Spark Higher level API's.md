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
