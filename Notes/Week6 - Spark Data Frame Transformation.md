
### Schema Enforcement

We already encountered an option to infer schema by spark. But how much data will it scan to infer schema. For this reason, there is an option called sampling ratio. Based on the percentage you specify, it scans the same percent of data to infer schema.

```python
yelp_user = spark.read \
.format("csv") \
.option("header","true") \
.option("inferSchema","true") \
.option("samplingRatio",0.1) \
.load("/public/yelp-dataset/yelp_user.csv")
```

Based on the percentage you specify, spark takes that much time to infer schema. In above code block. spark scans 10% of data to infer schema. More the sample size, more accurately the spark infers schema and takes more time to infer schema.

Even with this, if spark couldn't interpret the data, then it classifies the column as string. To avoid this, we can enforce schema

We can enforce schema in two ways.
1. **Schema DDL** 
	```python
orders_schema = 'order_id long, order_date date, cust_id long, order_status string'
orders1_df = spark.read \
.format("csv") \
.schema(orders_schema) \
.load("/public/trendytech/datasets/orders_sample1.csv")
orders1_df.printSchema()
```

2. **Struct type** 
``` python
from pyspark.sql.types import StructType,StructField,LongType,StringType,DateType,IntegerType
orders_schema_struct = StructType([
    StructField("orderId", LongType()),
    StructField("orderDate", DateType()),
    StructField("customerId",IntegerType()),
    StructField("orderStatus", StringType())
])
orders1_struct_df = spark.read \
.format("csv") \
.schema(orders_schema_struct) \
.load("/public/trendytech/datasets/orders_sample1.csv")
orders1_struct_df.printSchema()
```

When ever there is a datatype mismatch, spark will replace it will null.

---
### Read Modes

1. **Fail Fast**
	1. If spark is not able to parse it due to datatype mismatch then spark will throw an error.
	2. Can opt for this option, if you don't want to proceed if there are any parsing issues.
2. **Permissive**
	1. Default
	2. If spark is not able to parse it due to datatype mismatch then make it as null without impacting other results
3. **Drop Malformed**
	1. If spark is not able to parse it due to datatype mismatch then spark will drop the row.
