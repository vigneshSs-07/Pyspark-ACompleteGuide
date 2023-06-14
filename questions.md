Q: If Spark can spill the data to disk, why would it fail with the OOM - out-of-memory exception?

I heard many answers. However, most of them are beating around the bush.Here are the two points I would search for in any answer to this question.

1. Spill to disk and GC are the features of JVM Heap memory. The off-heap memory is not subject to spill and garbage collection. Spark overhead memory is always off-heap, and many OOM exceptions are caused by overhead memory exceeding its limit.

2. Spark can spill the data blocks to vacate the memory for processing. However, most of the in-memory data structures are not splittable. Spark cannot split the in-memory data structures used for processing work such as sorting/hashing, aggregating, and joining. Hence, they must fit in the available on-heap memory, and we cannot spill half of the data structures onto the disk. That's another reason causing Spark to raise an OOM exception even if it can spill data to disk.


what is optimization technique in spark?
A.) Spark optimization techniques are used to modify the settings and properties of Spark to ensure that the resources are utilized properly and the jobs are executed quickly. All this ultimately helps in processing data efficiently. The most popular Spark optimization techniques are listed below:

popular Spark optimization techniques are listed:
1. Data Serialization: Here, an in-memory object is converted into another format that can be stored in a file or sent over a network.

a. Java serialization: The ObjectOutputStream framework is used for serializing objects.

b. kyro serialization : To improve the performance, the classes have to be registered using the registerKryoClasses method.

2. caching: This is an efficient technique that is used when the data is required more often. Cache() and persist() are the methods used in this technique.

3. Data structure tuning: We can reduce the memory consumption while using Spark, by tweaking certain Java features that might add overhead.

4. Garbage collection optimization: G1 and GC must be used for running Spark applications. The G1 collector manages growing heaps. GC tuning is essential according to the generated logs, to control the unexpected behavior of applications.

Apache Spark:
➡️ It is general purpose
➡️ in memory
➡️ compute engine

❗Compute Engine:
what does hadoop provides?
hadoop provides 3 things:
hdfs = storage
MapReduce = computation
YARN = Resource manager.

-- Spark is an replacement/alternative of Mapreduce.
-- it is not good to compare spark with Hadoop, but we can compare spark with mapreduce.
-- spark is a plug an play compute engine which requires 2 things.
1. storage - local storage, Hdfs, Amazon s3
2. Resource manager - Yarn, Mesos, Kubernetes
-- spark is not bounded for particular storage or resource manager.

❗In Memory:
-- for each MapReduce job HDFS required 2-disc access i.e. onetime for reading and one time for writing.
-- but in Spark only one Io's disc is required which is initial read and final write.
-- spark is said to be 10 to 100 times faster than MapReduce.

General Purpose:
❗in hadoop we use Pig for cleaning.
❗hive for querying.
❗ for machine learning mahout
❗sqoop for database streaming data.
❗in mapreduce we only bound to use map and reduce.
❗but in spark everything is possible. like ❗whatever discussed above.
❗to achieve we just need to learn one style of code.
❗these are the reasons Spark is most preferred choice.
❗spark also provides filter too.
❗And this is called as General purpose compute.
❗The basic unit which holds the data in spark is called as RDD (Resilient Distributed Dataset).

❗In spark there are 2 kinds of operations.
1. Transformations.
2. Actions.


1. What is spark? Explain Architecture
2. Explain where did you use spark in your project?
3. What all optimization techniques have you used in spark?
4. Explain transformations and actions have you used?
5. What happens when you use shuffle in spark?
6. Difference between ReduceByKey Vs GroupByKey?
7. Explain the issues you resolved when you working with spark?
8. Compare Spark vs Hadoop MapReduce?
9. Difference between Narrow & wide transformations?
10. What is partition and how spark Partitions the data?
11. What is RDD?
11. what is broadcast variable?
12. Difference between Sparkcontext Vs Sparksession?
13. Explain about transformations and actions in the spark?
14. what is Executor memory in spark?
15. What is lineage graph?
16. What is DAG?
17. Explain libraries that Spark Ecosystem supports?
18. What is a DStream?
19. What is Catalyst optimizer and explain it?
20. Why parquet file format is best for spark?
21. Difference between dataframe Vs Dataset Vs RDD?
22. Explain features of Apache Spark?
23. Explain Lazy evaluation and why is it need?
24. Explain Pair RDD?
25. What is Spark Core?
26. What is the difference between persist() and cache()?
27. What are the various levels of persistence in Apache Spark?
28. Does Apache Spark provide check pointing?
29. How can you achieve high availability in Apache Spark?
30. Explain Executor Memory in a Spark?
31. What are the disadvantages of using Apache Spark?
32. What is the default level of parallelism in apache spark?
33. Compare map() and flatMap() in Spark?
34. Difference between repartition Vs coalesce?
35. Explain Spark Streaming?
36. Explain accumulators?
37. What is the use of broadcast join?
38. How to achieve parallelism in airflow tasks?
39. What is the difference between dataframe API and Spark API
40. What are the different formats supported in CSV format
41. When Spark is in memory compute engine, then why do we ever need to cache a Spark Dataframe in memory?


1. What is Serialization in Spark ?
2. Why should one not use a UDF?
3. What according to you is a common mistake apache-spark developers make when using spark?
4. RDD lineage
5. DAG
6. Is it a good idea to change spark.default.parallelism to a big number so that lots of threads can work on a single partition concurrently?
7. Metastore in Apache Spark
8. Difference between spark.sql.shuffle.partitions vs spark.default.parallelism?
9. Optimal file size for HDFS & S3
10. Speculative Execution in Spark
11. Spark Submit and its Options
12. Partitioning vs Bucketing in Hive
13. What is static allocation and dynamic allocation in Spark?
14. How to monitor Spark applications
15. What is Shuffle partitions in Spark
16. Internal working of Spark

Spark Databricks Tough Interview Question

1. Spark Architecture
2. RDD vs Dataframe
3. Broadcast variable
4, Accumulators
5. Memory management internals
6. Caching in Spark
7. Cache vs Persist
8. Repartitioning
9. Data Skew
10. Adatptive Query Engine
11. Map Join
12. Join Optimization
13. Cluster selection
14. ADLS mounting on Databricks
15. ADF to ADB integration
16. Magic commands
17. Passing Param in-out ADF to ADB
18. DAG
19. Delta Lake
20. Datalake house


❗ What is PySpark Architecture?
❗ What's the difference between an RDD, a DataFrame & DataSet?
❗ How can you create a DataFrame a) using existing RDD, and b) from a CSV file?
❗ Explain the use of StructType and StructField classes in PySpark with examples?
❗ What are the different ways to handle row duplication in a PySpark DataFrame?
❗ Explain PySpark UDF with the help of an example?
❗ Discuss the map() transformation in PySpark DataFrame
❗ what do you mean by ‘joins’ in PySpark DataFrame? What are the different types of joins?
❗ What is PySpark ArrayType?
❗ What is PySpark Partition?
❗ What is meant by PySpark MapType? How can you create a MapType using StructType?
❗ How can PySpark DataFrame be converted to Pandas DataFrame?
❗ What is the function of PySpark's pivot() method?
❗ In PySpark, how do you generate broadcast variables?
❗ When to use Client and Cluster modes used for deployment?
❗ How can data transfers be kept to a minimum while using PySpark?
❗ What are Sparse Vectors? What distinguishes them from dense vectors?
❗ What API does PySpark utilize to implement graphs?
❗ What is meant by Piping in PySpark?
❗ What are the various levels of persistence that exist in PySpark?
❗ List some of the benefits of using PySpark?
❗ Why do we use PySpark SparkFiles?
❗ Does PySpark provide a machine learning API?
❗ What are the types of PySpark’s shared variables and why are they useful?
❗ What PySpark DAGScheduler?




* What is Spark, and what are its primary use cases?

* Can you explain the difference between batch processing and stream processing in Spark?

* What is the difference between RDDs, DataFrames, and Datasets in Spark?

* Can you explain how Spark handles data partitioning and data shuffling?

* What are the different cluster managers supported by Spark?

* Can you explain the concept of lazy evaluation in Spark?

* How does Spark handle fault tolerance, and what is the role of RDD lineage in this process?

* What is the difference between Spark's standalone mode and cluster mode?

* What is the purpose of Spark SQL, and what are its advantages over traditional SQL?

* Can you explain the concept of window functions in Spark SQL?

* What is the difference between a transformation and an action in Spark?

* What is the role of Spark's shuffle operations, and when are they necessary?

* What is Spark Streaming, and how does it differ from other stream processing frameworks?

* Can you explain the concept of micro-batching in Spark Streaming?

* What are some best practices for tuning Spark applications for optimal performance?

* What is the difference between local and distributed mode in Spark, and when would you use each one?

* What is the difference between Spark and Hadoop, and when would you choose one over the other?

* How do you handle errors and exceptions in Spark applications, and what are some common issues you have faced?


1. What distinguishes SparkContext from SparkSession? How do they differ in their functions and capabilities?
2. What are the various ways to deploy an Apache Spark application? Can you explain the distinctions between client mode and cluster mode?
3. What exactly is an RDD, and what do the words "Resilient," "Distributed," and "Datasets" signify? Can you provide a detailed explanation of each term?
4. How do a driver and an executor work together? What is the difference between an executor and an executor core?
5. Could you provide a detailed overview of Spark's architecture, including its various components and how they interact with one another?
6. What distinguishes serialization from deserialization in the context of Spark? Can you explain the differences between these two concepts? Have you used Kyro serialization?
7. How did Spark manage to become so efficient in data processing compared to MapReduce?
8. What is Spark's execution plan, and how does it work? What is the logical & physical plan?
9. If an executor in Spark fails, what happens to the data and the computation that was being executed? What makes Spark fault tolerant?
10. What happens if the driver machine in Spark fails? Can you explain the implications of this scenario & what if one executor machine fails?
11. If you stop a SparkContext, what exactly happens?
12. Can you explain the differences between caching and checkpointing in Spark?
13. What is lazy evaluation in Spark, and transformations and actions differ from each other?
14. Can you provide an overview of various columnar file systems used in Spark, such as Parquet, ORC, and Avro? What do you mean by predicate pushdown?
15. How would you perform a join operation on two tables in Spark, particularly if one is much larger than the other? Basically, an interview is asking What is broadcast join in Spark?
16. Once you will explain broadcast join they will just ask How would you join two large tables in Spark 😁 ?

1.What is Spark?
2.Difference b/w Spark & MR
3.What is RDD?
4.Explain Architecture of Spark
5.What is Transformation and Action in Spark?
6.What is difference b/w DAG & Lineage?
7.What is Partition & how Spark partitions the data?
8.What is Spark core?
9.What is Pair RDD in Spark?
10.Diff b/w Map & flatMap()?
11. Diff b/w Map & mapValues()?
12.what is serialization & Deserialization?
13.What is Spark Context?
14.What is base RDD in Spark?
15. Explain Spark Stages?
16.Diff b/w RDD vs Dataframe & Dataset in Spark?
17.What is Catalyst Optimizer?
18.What is Spark Streaming?
19.What Spark is Lazy?
20. Difference b/w countByValue & combination Map & reduceByKey?
21. What is reduceByKey and its usage?
22. Diff b/w Persist () & Cache () in spark?
23.What is Spark Executors?
24.What is broadcast variable in Spark?
25.What is Spark Context & Spark Session?


1. Spark Architecture
2. spark operations.
3. Lazy Evaluation/DAG
4. RDD Vs DataFrame Vs Dataset
5. Types of transformations /Actions used.
6. Broadcast Join /SMB join.
7. Accumulators.
8. cluster Mode/client Mode
9. Optimizations of spark.
10. Different Memory types in spark.
11. Structured streaming types.


Python Questions:
==================
4. What do you know about object-oriented programing in Python, can you please describe different aspects of object-oriented programming.
5. I have a list that has some elements of different data types, tell me how will you iterate the list and short the elements, and display it.
6. I have a tuple with some elements and wanted to remove the duplicate elements from it. What's your approach
7. What is Dictionary in Python, how you create a dictionary in Python?
8. Have you ever performed slicing in Python, how will you perform slicing.
9. Where do you write instructions for the python packages which you have built.
10.How do you package a python project. When you execute the python code what the first file Python looks for
11. What are encapsulations, how do you restrict private members inside a class.
12. What are lambda functions in Python programming, what's the syntax.
13. How do you import a module into another module.
14. How do you extend multiple classes in Python.
15. virtual environment
16. data structures
17. python interpreter
18. pickel file
19. what is a package

1. What do you know about Snowflake and how the data files are stored in snowflake.
2. How do you connect Snowflake using Python. What are the parameters we have to define in the connection object?
3. How do you access files and execute the query in snowflake through python.
4. How many types of Warehouses are there in Snowflake.
5. Does snowflake have caching mechanism? How Caching in Works
6. How do you load data to snowflake?

Started Asking About Databricks

1. What have you done using Databricks. Why did your project choose to use Databricks?
2. How Spark features are integrated into Databricks.
3. How do you relate Azure ML Flow and Dataflow. Is Azure ML Flow belongs to Databricks or Azure?

Started Asking Questions in AWS.

1. How do you enforce security features on your S3 buckets.
2. Have you ever implemented elastic search in AWS.
3. what package do you use to make a connection to AWS from Python.

Started Asking on Pyspark

1. How you load data to a spark dataframe and tell me the syntax and what goes behind the scene when you load data to dataframe
2. What are the transformations you have done using spark, have you ever created rule based methods.
3. What are narrow and wide transformations in spark, which one you should consider
4. How you deploy your spark application in Production.
5. How you set your configuration in Spark and how spark application inherit the configuration.
6. Have you heard about Piping in Spark?
6. Tell me about your past experience where you have encountered performance issues in your application and what steps you have taken to fix the issue.
7. How do you execute SQL queries in Spark using the API Program.
8. You wanted to perform a full outer join using the API program. How do you write ?
9. How do you debug your spark application, what do you have to see when your spark application is stuck.
10. Have you ever used Apache Airflow to Schedule your job?
11. How data is stored when you load data to dataframe
12. How Spark decides the no of partition.
13. How can you minimize the data transfer for spark applications.
14. you have a textfile how do you find word count write code from imports
15. what does getorcreate() do 
16. for long running jobs, How to do debugging?
17. How do you run the python code on cluster?



1st Question is regarding Azure Data Bricks

1) How do you mount Data Bricks notebook with ADLS container ( inclusion of Scope is added advantage)

2) How do you read CSV File from ADLS location???

3) How do you read JSON file which is having Nested Columns???

4) Reparation Vs Coalesce

5) Group by Key Vs Reduce by Key

6) Difference between Partition By and Bucket By and some Use cases

7) Cache and Persist (They have also asked different layers of Cache)

8) Different Read modes in Spark and Write Modes as well

9) Salting Technique explanation

10) Join Optimization as well ( What will you do when you come across a Small dataset and Large Dataset , 2 Small Data Sets ), We need to Explain Our Strategy

11) Some Questions were around Executors and how we can increase Parallelism using them

12) Narrow Transformation and Wide Transformation examples and Explanation

13) How we can reduce the Shuffling was frequently asked in Interviews

14) Window Functions with Examples were asked more

15) What is Job, What is Stage, What are Partitions???

16) How can you tackle Skew Partitions???


17) How can you do Masking on certain Columns using UDF

18) Difference between Fat and Thin Executor

19) Difference between Dataframe and RDD

20) What is the use of Catalyst Optimizer???



20 Spark Interview Questions to Gear UP!

1.   What are the roles and responsibility of driver in spark Architecture?
2.   How spark divides the program in different Jobs, stages and tasks?
3.   What is Difference between Coalesce and Repartition?
4.   What is a Broadcast Join ?
5.   What is a OOM Issue, how to deal it?
6.   How to use SparkUI to debug performance issue?
7.   What is tungsten engine in Spark?
8.   What is meant by Data Skewness? How is it dealt?
9.   What are accumulator and BroadCast Variables?
10. Explain Difference Between DataFrame and DataSet?
11. What is Difference Between Map and FlatMap?
12. Explain internal working of Spark Submit?
13. What are the optimisation techniques used in Spark?
14. What is the role of Catalyst Optimizer in Spark SQL?
15. What is serialization? How can it impact the performance?
16. What are partitions? how to control partitions?
17. Difference between persist() and cache() in spark?
18. How is memory tuning done in spark?
19. What are the operations that cause shuffle in spark?
20. Common symptoms of excessive GC in Spark and how to address?

Explain Hadoop Architecture?
What is 5 v's of big data?
What is default replica in Hadoop? Can you increase or decrease it?
Difference between Hadoop (Gen1) and Hadoop (Gen2)?
What is heartbeats in hadoop? why is that important?
Write down few Linux commands?
What is partition, shuffling, sort in Mapreduce?
What is Record Reader?
Explain Sqoop Eval Command?
Explain different optimizations used in Sqoop?
Explain combiner in MapReduce?
What is Yarn? Why is it used?
Features of sqoop? Explain significance of them?
Explain Boundary Val's Query? Explain the formula?
Explain Modes available in Sqoop that used in job execution?
Difference between Target Vs Warehouse directory?
What is split by command? when it is used?
Hive Architecture?
Explain Transactional Processing Vs Analytical Processing?
Difference between Hive and RDBMS?
What is seek time in Hive?
Difference between SQL Vs HQL?
Explain UDF? How many types?
What is views in hive?
Explain Managed Table and External Table?
Spark Architecture?
What is transformations and actions? Name few?

Intermediate to Advanced questions. Those Include --
Explain different no.of optimizations in hive?
Explain types of Joins?
What is Map side Join? What is Bucket Map Join and Sort Merge Bucket join(SMB)?
Explain SCD Types in Hive?
Explain File-formats in hive?
Explain CAP Theorem?
Explain RDD? Difference between RDD Vs Dataframe vs Dataset?
Broadcast in Spark?
Explain Catalyst optimizer?
Difference between client Mode vs Cluster Mode?
Explain Cache & persist?
Explain Spark Performance Optimizations?
Explain Accumulators?


Interview Question :- Spark Scala
To Find the middle as index to get particular records
Input (1,2,3,4,5,6,7,8,9,10)

import org.apache.spark.sql.functions.monotonically_increasing_id
val values = Seq(1,2,3,4,5,6,7,8,9,10)
val df = values.toDF("values")
val indexdf = df.withColumn("index",functions.monotonically_increasing_id())
val totalCount = indexdf.count()
val middlleIndex = (totalCount -1)/2
val middleRecord = indexdf.filter($"index" === middlleIndex).select ("values").first().getInt(0)
println(s"Middle Index: $middlleIndex")
print("Middle Record: $middleRecord")

OUTPUT
Middle Index: 4
values: Seq[Int] = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
totalCount: Long = 10
middlleIndex: Long = 4
middleRecord: Int = 5

Here I used functions.monotonically_increasing_id()
This function generates a unique ID for each row, which acts as an "index".


Spark Interview Question:
=================
country.csv
city, country, city_population

find top 5 countries with highest population?

df.groupby('country').agg(sum('city_population')).alias('country_population').orderBy(col('country_population').desc()).show(5)

feel free to add your answers in comments. Stay tuned for answer


Spark Interview Questions
ORDER BY in Spark
----------------------------------------------

ORDER BY can be one of the worst things to use in Spark for job performance. ORDER BY sorts the data truly/ globally and for this it brings all the data to one machine and parallelism is lost!

Solution to this?

- Repartitioning your data on your expected join key and then using sortWithinPartitions

- Bucketing your data. Use DISTRIBUTE BY keyword in your table's DDL to get Spark to recognize the buckets



what are hadoop components and their services?
A.) HDFS: Hadoop Distributed File System is the backbone of Hadoop which runs on java language and stores data in Hadoop applications. They act as a command interface to interact with Hadoop.
The two components of HDFS – Data node, Name Node. Name node manages file systems and operates all data nodes and maintains records of metadata updating. In case of deletion of data,
they automatically record it in Edit Log. Data Node (Slave Node) requires vast storage space due to reading and writing operations.

Yarn: It’s an important component in the ecosystem and called an operating system in Hadoop which provides resource management and job scheduling task.

Hbase: It is an open-source framework storing all types of data and doesn’t support the SQL database. They run on top of HDFS and written in java language.
HBase master, Regional Server. The HBase master is responsible for load balancing in a Hadoop cluster and controls the failover. They are responsible for performing administration role. The regional server’s role would be a worker node and responsible for reading, writing data in the cache.

Sqoop: It is a tool that helps in data transfer between HDFS and MySQL and gives hand-on to import and export data.

Apache spark: It is an open-source cluster computing framework for data analytics and an essential data processing engine. It is written in Scala and comes with packaged standard libraries.

Apache Flume: It is a distributed service collecting a large amount of data from the source (webserver) and moves back to its origin and transferred to HDFS. The three components are Source, sink and channel.

MapReduce: It is responsible for data processing and acts as a core component of Hadoop. MapReduce is a processing engine that does parallel processing in multiple systems of the same cluster.

Apache Pig: Data Manipulation of Hadoop is performed by Apache Pig and uses Pig Latin Language. It helps in the reuse of code and easy to read and write code.

Hive: It is an open-source platform for performing data warehousing concepts; it manages to query large data sets stored in HDFS. It is built on top of the Hadoop Ecosystem. the language used by Hive
is Hive Query language.

Apache Drill: Apache Drill is an open-source SQL engine which process non-relational databases and File system. They are designed to support Semi-structured databases found in Cloud storage.

Zookeeper: It is an API that helps in distributed Coordination. Here a node called Znode is created by
an application in the Hadoop cluster.


Que 16.) Explain describe() and summary() function in Pyspark ❓

➡ In PySpark, the describe() and summary() functions are used to generate statistical summaries of a DataFrame's columns. They can be used to gain insights into the distribution of the data, detect potential outliers, and identify missing values.

➡ describe()
The describe() function returns a DataFrame containing the following statistics for each numerical column:
👉 count: number of non-null values
👉 mean: arithmetic mean
👉 stddev: standard deviation
👉 min: minimum value
👉 max: maximum value
👉 Here's an example usage:
df.describe().show()

➡ summary()
The summary() function is similar to describe(), but it allows you to specify which statistics you want to generate. You can use it to generate statistical summaries for multiple columns at once.
👉 Here's an example usage:
df.summary("count", "min", "25%", "50%", "75%", "max").show()
