##### Python Essentials for Spark

Normal Function and Lambda function

``` python
# define the function
def my_sum(x,y):
	return x+y
# call the function
print(my_sum(2,3)) #output - 5
```

```python
my_list = [1,2,3,4]
list(map(lambda x: 2*x, my_list))
```

lambda function is a small anonymous function (which doesn't have name)

``` python
from functools import reduce
my_list = [1,2,4,5,6]
reduce(lambda x,y: x+y, my_list) # takes two elements in a list and sum it.
```

map and reduce are **higher order functions** as it takes another function in parameter.

Higher order functions are which takes another function as input or gives function as output.

---

##### Another example

``` python 
from pyspark.sql import SparkSession
import getpass

username = getpass.getuser()

spark = SparkSession. \
	builder. \
	config('spark.ui.port', '0'). \
	config("spark.sql.warehouse.dir", f"/user/{username}/warehouse"). \
	enableHiveSupport(). \
	master('yarn'). \
	getOrCreate()
```

Above is a boiler plate code, lets discuss about this later

1. count the orders under each status
2. find the premium customers(Top10 who placed the most number of orders)
3. distinct count of customers who placed at least one order
4. 4.which customers has the maximum number of CLOSED orders

```python
orders_rdd = spark.sparkContext.textFile("/public/trendytech/retail_db/orders/*")
mapped_rdd = orders_rdd.map(lambda x:(x.split(",")[3],1))
mapped_rdd.take(10)
```

``` python
## output
[('CLOSED', 1), ('PENDING_PAYMENT', 1), ('COMPLETE', 1), ('CLOSED', 1), ('COMPLETE', 1), ('COMPLETE', 1), ('COMPLETE', 1), ('PROCESSING', 1), ('PENDING_PAYMENT', 1), ('PENDING_PAYMENT', 1)]
```

``` python
 reduced_rdd = mapped_rdd.reduceByKey(lambda x,y:x+y)
 reduced_rdd.collect()
 ```
 
``` python
 ## output
 [('CLOSED', 7556), ('CANCELED', 1428), ('COMPLETE', 22899), ('PENDING_PAYMENT', 15030), ('SUSPECTED_FRAUD', 1558), ('PENDING', 7610), ('ON_HOLD', 3798), ('PROCESSING', 8275), ('PAYMENT_REVIEW', 729)]
 ```
 
```python
reduced_sorted = reduced_rdd.sortBy(lambda x:x[1],False)
reduced_sorted.collect()
```

``` python
## output
[('COMPLETE', 22899), ('PENDING_PAYMENT', 15030), ('PROCESSING', 8275), ('PENDING', 7610), ('CLOSED', 7556), ('ON_HOLD', 3798), ('SUSPECTED_FRAUD', 1558), ('CANCELED', 1428), ('PAYMENT_REVIEW', 729)]
```

``` python
customers_mapped = orders_rdd.map(lambda x:(x.split(",")[2],1))
customers_mapped.take(5)
## output - [('11599', 1), ('256', 1), ('12111', 1), ('8827', 1), ('11318', 1)]
customers_aggregated = customers_mapped.reduceByKey(lambda x,y:x+y)
customers_aggregated.take(20)
```

```python
## output
[('256', 10),
 ('12111', 6),
 ('11318', 6),
 ('7130', 7),
 ('2911', 6),
 ('5657', 12),
 ('9149', 4),
 ('9842', 7),
 ('7276', 5),
 ('9488', 7),
 ('2711', 3),
 ('333', 6),
 ('656', 5),
 ('6983', 6),
 ('4189', 3),
 ('4840', 2),
 ('5863', 6),
 ('8214', 5),
 ('7776', 8),
 ('1549', 4)]
```

```python
customers_sorted = customers_aggregated.sortBy(lambda x:x[1],False)
customers_sorted.take(10)
```

``` python
## output
[('5897', 16),
 ('6316', 16),
 ('12431', 16),
 ('569', 16),
 ('4320', 15),
 ('221', 15),
 ('5624', 15),
 ('5283', 15),
 ('12284', 15),
 ('5654', 15)]
```

``` python
distinct_customers = orders_rdd.map(lambda x:(x.split(",")[2])).distinct()
distinct_customers.count() # output - 12405
orders_rdd.count() #output - 68883
filtered_orders = orders_rdd.filter(lambda x:x.split(",")[3] == 'CLOSED')
filtered_orders.take(20)
```

``` python
# output 
['1,2013-07-25 00:00:00.0,11599,CLOSED',
 '4,2013-07-25 00:00:00.0,8827,CLOSED',
 '12,2013-07-25 00:00:00.0,1837,CLOSED',
 '18,2013-07-25 00:00:00.0,1205,CLOSED',
 '24,2013-07-25 00:00:00.0,11441,CLOSED',
 '25,2013-07-25 00:00:00.0,9503,CLOSED',
 '37,2013-07-25 00:00:00.0,5863,CLOSED',
 '51,2013-07-25 00:00:00.0,12271,CLOSED',
 '57,2013-07-25 00:00:00.0,7073,CLOSED',
 '61,2013-07-25 00:00:00.0,4791,CLOSED',
 '62,2013-07-25 00:00:00.0,9111,CLOSED',
 '87,2013-07-25 00:00:00.0,3065,CLOSED',
 '90,2013-07-25 00:00:00.0,9131,CLOSED',
 '101,2013-07-25 00:00:00.0,5116,CLOSED',
 '116,2013-07-26 00:00:00.0,8763,CLOSED',
 '129,2013-07-26 00:00:00.0,9937,CLOSED',
 '133,2013-07-26 00:00:00.0,10604,CLOSED',
 '191,2013-07-26 00:00:00.0,16,CLOSED',
 '201,2013-07-26 00:00:00.0,9055,CLOSED',
 '211,2013-07-26 00:00:00.0,10372,CLOSED']
```

``` python 
filtered_mapped = filtered_orders.map(lambda x:(x.split(",")[2],1))
filtered_mapped.take(10)
```

``` python
# output
[('11599', 1),
 ('8827', 1),
 ('1837', 1),
 ('1205', 1),
 ('11441', 1),
 ('9503', 1),
 ('5863', 1),
 ('12271', 1),
 ('7073', 1),
 ('4791', 1)]
```

``` python
filtered_aggregated = filtered_mapped.reduceByKey(lambda x,y:x+y)
filtered_aggregated.take(20)
```

``` python
# output
[('5863', 1),
 ('12271', 2),
 ('7073', 1),
 ('3065', 2),
 ('5116', 2),
 ('8763', 1),
 ('10604', 2),
 ('16', 1),
 ('9055', 3),
 ('10372', 3),
 ('11715', 1),
 ('5925', 1),
 ('8309', 3),
 ('948', 1),
 ('5191', 1),
 ('7650', 2),
 ('4199', 2),
 ('6989', 1),
 ('5011', 4),
 ('11394', 1)]
```

``` python
filtered_sorted = filtered_aggregated.sortBy(lambda x:x[1],False)
filtered_sorted.take(10)
```

```python
# output
[('1833', 6),
 ('1363', 5),
 ('1687', 5),
 ('5493', 5),
 ('5011', 4),
 ('8974', 4),
 ('2321', 4),
 ('3736', 4),
 ('8368', 4),
 ('2236', 4)]
```

---

To develop a spark code, we don't work on complete data instead on subset of data. To simulate the distributed style computing, there is concept called parallelize.

``` python
spark.SparkContext.parallelize(<subset of data>)
```

based on default min number of partitions, our sample data will split. And  based on default parallelism setting we achieve parallelism.

``` python
subset_rdd.getNumpartitions() #output - 2

spark.sparkContext.defaultParallelism  # output - 2, thats why our subset rdd has 2 partitions

spark.sparkContext.defaultMinPartitions # output - 2
```

default block size = 128mb

If we have 
- 100mb - 2 partitions
- 150mb - 2 partitions
- 300mb - 3 partitions

---

Another Way to count the orders under each status

``` python
orders_rdd = spark.sparkContext.textFile("/public/trendytech/retail_db/orders/*")
mapped_rdd = orders_rdd.map(lambda x:(x.split(",")[3]))
mapped_rdd.take(10)
```

``` python 
## output
['CLOSED','PENDING_PAYMENT','COMPLETE','CLOSED','COMPLETE', 'COMPLETE','COMPLETE', 'PROCESSING','PENDING_PAYMENT','PENDING_PAYMENT']
```

``` python
mapped_rdd.countByValue() # action
```

``` python
 ## output
 [('CLOSED', 7556), ('CANCELED', 1428), ('COMPLETE', 22899), ('PENDING_PAYMENT', 15030), ('SUSPECTED_FRAUD', 1558), ('PENDING', 7610), ('ON_HOLD', 3798), ('PROCESSING', 8275), ('PAYMENT_REVIEW', 729)]
```

Now when to use reduce by key and count by value? if you know you don't do any transformations otherwise we go with reduce by key so that we can chain another transformation on it. And also count by value will bring the data to one node and perform the action.

---
##### Types of Transformations

Some transformations we encountered
- map
- filter
- flatMap
- reduceByKey

All transformations fall under 2 categories
1. **Narrow Transformation** - data is processed on the same node and no shuffling is done.
	1. Example - Map, Filter
2. **Wide Transformation** - data is shuffled from multiple worker nodes and processed
	1. Example - reduceByKey, groupByKey

Always try to minimize wide transformation and try to have wide transformations as later as possible. 

---
##### Spar k Job - High level info

Lets say, we loaded data, performed map and reduceByKey transformation and collected the data.

load -> map -> reduceByKey -> collect

You can find the spark jobs in history server once you close the kernel or stop the spark session (command -> spark.stop()).

For above spark code, how many jobs are created?

**No. of jobs = No. of actions** (in the above case, No. of jobs = 1)

In each job, we will have multiple stages. At end of each stage, data is written back to disk and next stage picks the data from disk.

**No. of staged = No. of wide transformations + 1** ( in above case, No. of  stages = 2)

In each stage, we will have multiple tasks

**No. of tasks = No. of partitions** ( by default min partitions is 2. so, No. of min tasks = 2 )

---
##### reduce vs reduceBykey

reduceByKey is a transformation works on pair(key, value) RDD. 

reduce is an action and works on non-pair RDD.

why one is transformation and other is action?

Because reduce always proves single output as it aggregates the data entirely so this can come to local node whereas reduceByKey depends on numbers of keys, data may or may not be small and we can process further this data.

---
##### reduceByKey vs groupByKey

reduceByKey is a wide transformation with local aggregation (data shuffling will be minimal).

groupByKey is a wide transformation without local aggregation.

*local aggregation is on each node, data is aggregated.*

one important common thing is all the same keys will go to same machine (In MR, similar to Hash function concept).

run the spark code and analyze the job in history server (concentrate on shuffle read/write)

groupByKey can lead to out of memory errors and is not recommended.

---
##### Spark Join

Consider you have two files, orders and customers with 1100mb and 1mb.

orders - 1100mb - 9 blocks | 9 partitions 
customers - 1mb - 1 block | 2 partitions

To join, same keys from both files should be on same node.

Lot of data will be shuffled from both files and join is performed. (Wide transformation)

```python
orders_base = spark.sparkContext.textFile("/public/trendytech/orders/orders_1gb.csv")
orders_mapped = orders_base.map(lambda x:(x.split(",")[2], x.split(",")[3]))
customers_base = spark.sparkContext.textFile("/public/trendytech/retail_db/customers/part-00000")
customers_mapped = customers_base.map(lambda x:(x.split(",")[0], x.split(",")[8]))
joined_rdd = customers_mapped.join(orders_mapped)
joined_rdd.saveAsTextFile("data/orders_joined")
```

This takes lot of time and plan will be complex

---
##### Broadcast Join

From above join, job takes lot of time to shuffle the data so that same keys are in same node.

If smaller non partitioned file (in above case,1mb file) is kept on all the nodes then on each node the data can be joined. so that data shuffling can be avoided. Trade off is there is lot of redundant data but performance is improved.

``` python
customers_broadcast = spark.sparkContext.broadcast(customers_mapped.collect())
```

After this you can use a map transformation to join

``` python
def getPinCode(customerID):
    try:
        for customer in customers_broadcast .value:
            if customer[0] == str(customerID):
                return (customer[0],customer[1])
        return -1    
    except:
        return -1
joined_rdd = orders_mapped.map(lambda x : (getPinCode(int(x[0])), x[1]))
joined_rdd.saveAsTextFile("data/broadcastresults")
```

---
##### Repartition vs Coalesce

If you have **lot of resources** then you wish to **increase the partitions** or vise versa. Based on other requirements, you wish to **increase or decrease partitions**.

``` python
repartitioned_orders = orders_base.repartition(15)
```

Other case is after filtering the data, each partition is holding less amount of data(approx., 1 mb). then we can decrease the partitions so that each partitions will hold reasonable amount of data to process (~ 128mb). So that it can help other transformations to optimize.

**Coalesce can only decrease the number of partitions**. It will not increase the partitions but will skip it if you try to increase.

**Repartition will do complete re-shuffling of data** with an intent to have almost same amount of data on each partition.

**Coalesce will try to merge the data on same node to avoid lot of data shuffling**, not with an intent to have same amount of data on each partition.

---
##### Cache

