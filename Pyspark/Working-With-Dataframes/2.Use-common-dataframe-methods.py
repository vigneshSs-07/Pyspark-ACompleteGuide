# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Use common DataFrame methods
# MAGIC 
# MAGIC In the previous notebook, you ended off by executing a count of records in a DataFrame. We will now build upon that concept by introducing common DataFrame methods.

# COMMAND ----------

# MAGIC %md
# MAGIC **Technical Accomplishments:**
# MAGIC * Develop familiarity with the `DataFrame` APIs
# MAGIC * Use common DataFrame methods for performance
# MAGIC * Explore the Spark API documentation

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Getting Started
# MAGIC 
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC Prepare the data source.

# COMMAND ----------

(source, sasEntity, sasToken) = getAzureDataSource()

spark.conf.set(sasEntity, sasToken)

# COMMAND ----------

# MAGIC %md
# MAGIC Create the DataFrame. This is the same one we created in the previous notebook.

# COMMAND ----------

parquetDir = source + "/wikipedia/pagecounts/staging_parquet_en_only_clean/"

# COMMAND ----------

pagecountsEnAllDF = (spark  # Our SparkSession & Entry Point
  .read                     # Our DataFrameReader
  .parquet(parquetDir)      # Returns an instance of DataFrame
)
print(pagecountsEnAllDF)    # Python hack to see the data type

# COMMAND ----------

# MAGIC %md
# MAGIC Execute a count on the DataFrame as we did at the end of the previous notebook.

# COMMAND ----------

total = pagecountsEnAllDF.count()

print("Record Count: {0:,}".format( total ))

# COMMAND ----------

# MAGIC %md
# MAGIC That tells us that there are around 2 million rows in the `DataFrame`. 
# MAGIC 
# MAGIC Before we take a closer look at the contents of the `DataFrame`, let us introduce a technique that speeds up processing.  

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) cache() & persist()
# MAGIC 
# MAGIC The ability to cache data is one technique for achieving better performance with Apache Spark. 
# MAGIC 
# MAGIC This is because every action requires Spark to read the data from its source (Azure Blob, Amazon S3, HDFS, etc.) but caching moves that data into the memory of the local executor for "instant" access.
# MAGIC 
# MAGIC `cache()` is just an alias for `persist()`. 

# COMMAND ----------

(pagecountsEnAllDF
  .cache()         # Mark the DataFrame as cached
  .count()         # Materialize the cache
) 

# COMMAND ----------

# MAGIC %md
# MAGIC If you re-run that command, it should take significantly less time.

# COMMAND ----------

pagecountsEnAllDF.count()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Performance considerations of Caching Data
# MAGIC 
# MAGIC When Caching Data you are placing it on the workers of the cluster. 
# MAGIC 
# MAGIC Caching takes resources, before moving a notebook into production please check and verify that you are appropriately using cache. 

# COMMAND ----------

# MAGIC %md
# MAGIC And as a quick side note, you can remove a cache by calling the `DataFrame`'s `unpersist()` method but, it is not necessary.

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Our Data
# MAGIC 
# MAGIC Let's continue by taking a look at the type of data we have. 
# MAGIC 
# MAGIC We can do this with the `printSchema()` command:

# COMMAND ----------

pagecountsEnAllDF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC We should now be able to see that we have four columns of data:
# MAGIC * **project** (*string*): The name of the Wikipedia project. This will include values such as:
# MAGIC   * **en**: The English version of Wikipedia.
# MAGIC   * **fr**: The French version of Wikipedia.
# MAGIC   * **en.d**: The English version of Wiktionary.
# MAGIC   * **fr.b**: The French version of Wikibooks.
# MAGIC   * **de.n**: The German version of Wikinews.
# MAGIC * **article** (*string*): The name of the article in the corresponding project. This will include values such as:
# MAGIC   * <a href="https://en.wikipedia.org/wiki/Apache_Spark" target="_blank">Apache_Spark</a>
# MAGIC   * <a href="https://en.wikipedia.org/wiki/Matei_Zaharia" target="_blank">Matei_Zaharia</a>
# MAGIC   * <a href="https://en.wikipedia.org/wiki/Kevin_Bacon" target="_blank">Kevin_Bacon</a>
# MAGIC * **requests** (*integer*): The number of requests (clicks) the article has received in the hour this data represents.
# MAGIC * **bytes_served** (*long*): The total number of bytes delivered for the requested article.
# MAGIC   * **Note:** In our copy of the data, this value is zero for all records and consequently is of no value to us.

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Spark API
# MAGIC 
# MAGIC You have already seen one command available to the `DataFrame` class, namely `DataFrame.printSchema()`
# MAGIC   
# MAGIC Let's take a look at the API to see what other operations we have available.

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Spark API Home Page**
# MAGIC 0. Open a new browser tab
# MAGIC 0. Google for **Spark API Latest** or **Spark API _x.x.x_** for a specific version.
# MAGIC 0. Select **Spark API Documentation - Spark _x.x.x_ Documentation - Apache Spark** 
# MAGIC 
# MAGIC Other Documentation:
# MAGIC * Programming Guides for DataFrames, SQL, Graphs, Machine Learning, Streaming...
# MAGIC * Deployment Guides for Spark Standalone, Mesos, Yarn...
# MAGIC * Configuration, Monitoring, Tuning, Security...
# MAGIC 
# MAGIC Here are some shortcuts
# MAGIC   * <a href="https://spark.apache.org/docs/latest/" target="_blank">Spark API Documentation - Latest</a>
# MAGIC   * <a href="https://spark.apache.org/docs/2.1.1/api.html" target="_blank">Spark API Documentation - 2.1.1</a>
# MAGIC   * <a href="https://spark.apache.org/docs/2.1.0/api.html" target="_blank">Spark API Documentation - 2.1.0</a>
# MAGIC   * <a href="https://spark.apache.org/docs/2.0.2/api.html" target="_blank">Spark API Documentation - 2.0.2</a>
# MAGIC   * <a href="https://spark.apache.org/docs/1.6.3/api.html" target="_blank">Spark API Documentation - 1.6.3</a>

# COMMAND ----------

# MAGIC %md
# MAGIC Naturally, which set of documentation you will use depends on which language you will use.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Spark API (Python)
# MAGIC 
# MAGIC 0. Select **Spark Python API (Sphinx)**.
# MAGIC 0. Look up the documentation for `pyspark.sql.DataFrame`.
# MAGIC   0. In the lower-left-hand-corner type **DataFrame** into the search field.
# MAGIC   0. Hit **[Enter]**.
# MAGIC   0. The search results should appear in the right-hand pane.
# MAGIC   0. Click on **pyspark.sql.DataFrame (Python class, in pyspark.sql module)**
# MAGIC   0. The documentation should open in the right-hand pane.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Spark API (Scala)
# MAGIC 
# MAGIC 0. Select **Spark Scala API (Scaladoc)**.
# MAGIC 0. Look up the documentation for `org.apache.spark.sql.DataFrame`.
# MAGIC   0. In the upper-left-hand-corner type **DataFrame** into the search field.
# MAGIC   0. The search will execute automatically.
# MAGIC   0. In the class/package list, click on **DataFrame**.
# MAGIC   0. The documentation should open in the right-hand pane.
# MAGIC   
# MAGIC This isn't going to work, but why?

# COMMAND ----------

# MAGIC %md
# MAGIC ### Spark API (Scala), Try #2
# MAGIC 
# MAGIC Look up the documentation for `org.apache.spark.sql.Dataset`.
# MAGIC   0. In the upper-left-hand-corner type **Dataset** into the search field.
# MAGIC   0. The search will execute automatically.
# MAGIC   0. In the class/package list, click on **Dataset**.
# MAGIC   0. The documentation should open in the right-hand pane.

# COMMAND ----------

# MAGIC %md
# MAGIC Now that we have found the proper documentation, we can take a quick peek at the function `printSchema()`.
# MAGIC 
# MAGIC Nothing special here.
# MAGIC 
# MAGIC If you look at the API docs, `printSchema(..)` is described like this:
# MAGIC > Prints the schema to the console in a nice tree format.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next steps
# MAGIC 
# MAGIC Start the next lesson, [Use the Display function]($./3.Display-function)