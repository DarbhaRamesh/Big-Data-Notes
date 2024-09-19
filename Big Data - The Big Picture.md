##### What is Big Data?

Due to resource crunch, we cant store or process the data.

According to IBM, Any data which is characterized by 3 V's is Big data

1. **Volume**- Huge amount of data
2. **Variety** - data can be in various forms like structured, semi- structured or Unstructured.
	1. Structured - RDMS tables.
	2. Semi- Structured - JSON, XML. (partial schema associated with it)
	3. Unstructured - log files, image, video (do not have any schema)
3. **Velocity** - speed at which new data is coming in. At whatever pace data is coming in, we have to process and store it.
4. **Veracity** - quality or correctness of data.
5. **Value** - data should be able to generate value.
----------------
##### New Technology stack?

Our traditional technology stack, will not be able to handle big data so we have to use/learn new technology stack.

**Monolithic System** - One big system holding a lot of power(resources)
 - Resources
	 - Compute - CPU cores
	 - Memory - RAM
	 - Storage - Hard disk
Example - personal computer/laptop

Lets say, you have X resources which gives Y performance. Now you want to increase the performance/ double it, you tend to increase the resources. Till certain point, the rate at which you increase the resource, performance also increases but after that it wouldn't. This is Vertical Scaling

Meaning 2X  resources < 2Y performance.

Traditional tech stack is Monolithic.

**Distributed System** - a group of nodes (machine) called as cluster. Here by increase the nodes, performances increases in the same ratio. This is Horizontal Scaling.

True scaling is Horizontal Scaling as performance increases in same ratio as you increase the nodes in cluster.

In Big Data, we use Distributed architecture.

Here comes few questions 
 1. Distributed Storage - how will you store the data? 
 2. Distributed Processing/Computation - how will you process the data?
 3. Scalability - Is the system scalable ? and how will you scale the system ?.
---
##### Hadoop

Its a framework to solve Big data problems. Its not a single tool instead a ecosystem of tools to solve Big data problems.

There are 3 core components in Hadoop
- **HDFS** - Hadoop Distributed File System (Distributed Storage)
- **MapReduce** - Distributed Processing 
	- Writing a MapReduce code is very hard, so it is obsolete.
	- Apache Spark has taken its place.
- **YARN** - Yet Another Resource Negotiator (resource manager)
	- Let say you have 20 node Hadoop cluster - some one has to manage these nodes. Here comes the role of YARN, where you will be negotiating for resources.

Using these three core components, there are many tools/technologies in Hadoop Ecosystem to get started quickly like 
1. **Sqoop** - Data Ingestion to/from Hadoop from/to external sources(Sqoop import/Sqoop export).
	- Underlying code is MapReduce.
	- With few Sqoop commands we can achieve the functionality
	- Cloud based tool - Azure Data factory
2. **Pig** - Scripting language, used mainly to clean the data
3. **Hive** - SQL kind interface to query the data (still in demand).
4. **HBase** - NoSQL database (Cosmos DB)
5. **Oozie** - workflow scheduler (Azure Data factory)

- Challenges
	- MapReduce is very slow and really hard to write code.
	- Learn different components and each has its own big learning curve
	- Mainly On-premise
Because of these challenges, Hadoop is slowing loosing its market share.
---
##### Apache Spark

Its a **general purpose**, **in-memory** **compute engine**.

Spark is replacement of MapReduce, not for Hadoop.
Because spark is in-memory compute engine, its much faster than MapReduce.

For On-premise Hadoop setup, HDFS/ Apache Spark / YARN.

Spark needs two other core components like Storage and resource manager to work.
- **Storage** - ADLS Gen2/ GCS/ S3/ HDFS/ Local Storage
- **Resource Manager** - YARN/ Mesos/ Kubernetes

Its a plug and play compute engine. 

In what languages we can write spark code? - Python / Scala/ Java / R

Spark is developed using Scala, so many likes to use Spark with Scala but majorly used with Python(PySpark)

---

##### Database vs Data warehouse vs Data lake

Before processing, lets understand first where to store the data.

**Database** mainly deals with **most recent** **transactional** and **structured** data, best meant for **OLTP**(Online transactional processing) purpose. It follows **schema on write** (while writing into database, it validates the data and throws error if there is any mismatch) and **cost to store the data is high**
Tools used - Oracle, MySQL, Postgres.

**Datawarehouse** is mainly used for **Analytical** processing and deals with lot of **historical structured data**, best meant for **OLAP**(Online Analytical processing) purpose.  It follows **schema on write**, cost is comparatively low than database. To ingest data we use **ETL** or **ELT** process.
Tools - Snowflake, Google Big query

**Data Lake** is used to store structured, semi structured or unstructured data (raw form). It follows **Schema on read**(while reading the data schema is identified). It is **cost effective**. 
Tools - S3, ADLS Gen2, cloud storage

----
##### Big Data Big Picture

Multiple data sources -> Ingestion -> Data Lake -> Processing -> Serving layer

For reporting, serving layer will be Data warehouse.
To build some custom UI, No SQL DB will be used. 
To build an application, database will be used

Hadoop 

Multiple data sources -> Sqoop -> HDFS -> MapReduce(outdated)/ Spark -> Hive/ HBase

Azure

Multiple data sources -> Azure Data Factory -> ADLS Gen2 -> Azure Data Factory/ Synapse -> Azure SQL/ Cosmos DB

AWS

Multiple data sources -> AWS Glue -> Amazon S3 -> AWS Databricks/ Athena/ Redshift -> AWS RDS / Dynamo DB

For computing, either we process data in a dedicated server or using a serverless (uses resources from shared pool) architecture. **In serverless, performance is not guaranteed but cost effective.**

Athena/ Synapse Serverless  - Serverless 
Redshift/ Synapse Dedicated Pool - Dedicated Server

---

#### HDFS - Hadoop Distributed File System

Let say a file f1 has to be loaded into HDFS. We are having a 4 node cluster. So the file f1 is broken into 4 parts b1, b2, b3 and b4. On each node, one part of file is stored. These nodes are called **Data nodes**. N1 -> b1, N2 -> b3, N3 -> b2, N4 -> b4

Now if you want to read the file f1 from HDFS, how does HDFS know which part is stored in which node? For this purpose, a master node or **Name node** is present which contains this info in tabular like format.

| file name | block | data node |
| --------- | ----- | --------- |
| file1     | b1    | N1        |
| file1     | b3    | N2        |
| file1     | b2    | N3        |
| file1     | b4    | N4        |

Therefore, Data node holds actual data and Name node contains metadata. Whenever a client wants to read the data, the request goes to name node. Name node tells the client from where it has to read the data from data nodes.

Lets say due to any reason a data node goes down. Then we might loose the data in that node. For this reason there is concept of **replication factor** which will replicate the part of the file is other node. By default, it is 3. If replication factor = 2 then a part of file is stored in two nodes. 

How does a name node knows a data node is dead? Every data node sends **heartbeat** to name node every 3 seconds indicating it is alive. If name node does not receive heart beat for 10 consecutive times, name node considers the data node is dead.

Other question is how many parts a file can be broken. For this, there is concept of **default blob size** which is set by HDFS admin (128 mb) and based on the default size the file is broken into multiple parts / blobs.

Now, what if name node dies? There is again a **secondary name node** which will takes its place but chances of going down is very minimal.

Processing the data on the node where it is kept, this is called **Data locality**. Hadoop works on this principle.

---



