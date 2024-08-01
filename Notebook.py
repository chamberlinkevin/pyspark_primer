# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Primer
# MAGIC **With a Focus on PySpark** \
# MAGIC *By Kevin Chamberlin*\
# MAGIC Updated July 2024

# COMMAND ----------

# MAGIC %md
# MAGIC ##File Management

# COMMAND ----------

# MAGIC %md
# MAGIC **Workspace** is the term used to describe the file structure around notebooks in Databricks. They're generally organized by the original *author of the notebooks*, but companies can handle this differently if they wish.\
# MAGIC The "bb" shared workspace was created to make sharing and collaborating on notebooks in real time a lot easier. It has since been replaced with the use of an Azure DevOps Repo

# COMMAND ----------

# MAGIC %md
# MAGIC **Repos** or _Repositories_ reference mostly Github or Azure DevOps repositories of code. This is a way to maintain version controlling and organized development/implementation of code. In Databricks, they are stored as a type of "Workspace".

# COMMAND ----------

# MAGIC %md
# MAGIC **Catalog** is the term used in Databricks for the database storage accessible natively within Databricks. In this notebook, I use a generic example Spark Dataframe created manually. I've also included two examples of the syntax we use to data that is stored in our Azure Storage Container. (An ADLS Gen2 Blob Container)

# COMMAND ----------

# Example of manually creating a Spark DataFrame using PySpark code 
l_avengers_data = [[1, "Steve", "Rogers", "Brooklyn, NY", "Blue"]
              ,[2, "Tony", "Stark", "Manahattan, NY", "Gold"]
              ,[3, "Peter", "Parker", "Queens, NY", "Blue"]
              ,[4, "Scott", "Lang", "Coral Gables, FL", "Blue"]
              ,[5, "Natasha", "Romanoff", "Stalingrad, USSR", "Black"]
              ,[6, "Clint", "Barton", "Waverly, IA", "Purple"]]

l_avengers_col_names = ["ID", "FirstName", "LastName", "Hometown", "Favorite Color"]

sdf_avengers = spark.createDataFrame(l_avengers_data, l_avengers_col_names)

# # Example of reading a file "avengers_csv" from the Azure Storage Container
# str_avengers_csv_path = "dbfs:/mnt/isp-development/r&d/kevin/avengers.csv"
# sdf_avengers = spark.read.format("csv").option("header","True").option("delimiter",",").csv(str_avengers_csv_path)

# # Example of reading a parquet file from the Azure Storage Container
# str_avengers_parquet_path = "dbfs:/mnt/isp-development/r&d/kevin/avengers.parquet"
# sdf_avengers = spark.read.parquet(str_avengers_parquet_path)

# Example of displaying data stored in your Spark instance. 
display(sdf_avengers)

# COMMAND ----------

# MAGIC %md
# MAGIC **Uploading / downloading notebooks** is fairly straightforward. If you've ever used Jupyter Notebooks within Python before, .IPYNB files are one of the primary ways to download a notebook. This option is within the "File" menu visible when viewing a notebook.\
# MAGIC To upload, right-click within the intended workspace, and selected "Import". From there you can drag-and-drop, or select a number of file types to be used within Databricks.

# COMMAND ----------

# MAGIC %md
# MAGIC **Cloning** is a key feature of Databricks file management. You can essentially think of it as a *Copy/Paste* for the entire notebook. This can be usefully when you want to perhaps make edits to *someone else's notebook* you don't have \
# MAGIC edit permissions for, or when duplicating large sections of code in one of *your own* previously developed notebooks. Try cloning this notebook to your own workspace now.

# COMMAND ----------

# MAGIC %md
# MAGIC **Workflows/Jobs** are something I used extensively in previous roles to automated when Notebooks are run in the case of regular data pipelining operations. They are viewable on the "Workflows" tab on the left side of the screen, and new Workflows for a specific notebook \
# MAGIC can easily be made with the "Schedule" button at the top right corner of the screen while viewing a notebook. There are some nuances to workflows and the clusters they use, but we can go deeper into that at a later date if necessary.

# COMMAND ----------

# MAGIC %md
# MAGIC **Compute** is the source of setting up and monitoring clusters within Databricks. For most users, monitoring is only necessary when bottlenecking is occuring. Data engineers may use this information for optimizing pipeline functionality.

# COMMAND ----------

# MAGIC %md
# MAGIC **Additional resources from Databricks**: https://docs.databricks.com/notebooks/notebooks-use.html

# COMMAND ----------

# MAGIC %md
# MAGIC ## Languages
# MAGIC Languages are selected when creating a new notebook, but can be changed later on.

# COMMAND ----------

#The base for these notebooks is Python -- as seen next to the notebook title at the top of the screen.
str_statement_py = "Hello, Python World!"
print(str_statement_py)

# COMMAND ----------

# MAGIC %r
# MAGIC # You can also use the % on line 1 of a cell to change the language using what's called a "magic command"
# MAGIC str_statement_r <- "Hello, R World!"
# MAGIC print(str_statement_r)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- You can even write directly in SQL if that's what you want to do
# MAGIC select "Hello, SQL World!" as statement_sql

# COMMAND ----------

# MAGIC %md
# MAGIC Databricks notebooks also can utilize a built-in **Markdown language cells** to add *documentation and readability* within your code. They're designated with "%md" on line 1 of the cell. I've found headers to be particularly \
# MAGIC useful within Databricks, because of the automated navigation outlining they lead to. Click the Table of Contents button towards the upper left edge of the notebook to explore that navigation in this document!

# COMMAND ----------

# MAGIC %md
# MAGIC ## PySpark Libraries

# COMMAND ----------

import pyspark.sql.functions as F
from datetime import datetime
from pyspark.sql.types import DateType, IntegerType
#from pyspark.sql.functions import col, udf, when

# COMMAND ----------

# MAGIC %md
# MAGIC Above are examples of commonly used libraries and how to import them. If you are familiar with Python, this formatting will look very familiar. When we talk about _Pyspark_ we are frequently referring to a set of SQL functions that have been written in Python to be used on a distributed computing platform like Databricks! \
# MAGIC I suggest starting all Databricks notebooks with the command `import pyspark.sql.functions as F`. I prefer this notation over the potentially simplier `from pyspark.sql.functions import *` because the former highlights more tracability for troubleshooting and prevents any potential conflicts with function names. This conflict has appeared a handful of times in our main product code, so it's best to just use "F." notation to save integration time later on.

# COMMAND ----------

# Basic example of the SQL functions within PySpark

sdf_avengers_names = (sdf_avengers
                      .withColumn("FullName", F.concat(F.col("FirstName"), F.lit(" "), F.col("LastName")))
                      .select(F.col("ID"), F.col("FullName"))
                     )

display(sdf_avengers_names)

# COMMAND ----------

# Create a new dataframe completely from scratch

l_hero_data = [[1, "Captain America"]
              ,[2, "Iron Man"]
              ,[3, "Spiderman"]
              ,[4, "Ant-Man"]
              ,[5, "Black Widow"]
              ,[6, "Hawkeye"]]

l_hero_col_names = ["ID", "Hero"]

sdf_avengers_heroes = spark.createDataFrame(l_hero_data, l_hero_col_names)


# Create a new dataframe that matches the columns of an existing dataframe

l_new_avenger_data = [[7, "Wanda", "Maximoff", "Sokovia", "Scarlet"]]

l_new_avenger_col_names = sdf_avengers.columns # data and metadata from DFs can be called upon

sdf_avengers_new = spark.createDataFrame(l_new_avenger_data, l_new_avenger_col_names)

# Display both new dataframes
display(sdf_avengers_heroes)
display(sdf_avengers_new)

# COMMAND ----------

# Two ways of combining data
sdf_avengers_expanded = (sdf_avengers
                         .union(sdf_avengers_new)
                         .join(sdf_avengers_heroes, on = "ID", how = 'left')
                        )
# NOTE: If a table you're joing with is relatively small a "broadcast join" may improve processing time while distributed

display(sdf_avengers_expanded)

# COMMAND ----------

# Filtering is generally a good skill to be able to utilize

sdf_avengers_filtered = (sdf_avengers_expanded
                         .filter(F.col("Hero").isNotNull())
                         .filter(F.col("Favorite Color") != "Gold")
                         .withColumn("FirstInitial", F.substring("FirstName", 1,1))
                         .drop("FirstName")
                         .select("FirstInitial", "LastName", "Hometown", "Hero", F.col("Favorite Color").alias("FavoriteColor"))
                        )

display(sdf_avengers_filtered)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Distributed Cloud Computing 
# MAGIC At its core, Databricks is a platform for doing cloud computing. That's why you run notebooks on a "Cluster" -- it's a grouping of workers and a driver that process your code. \
# MAGIC When you run code distributed across the worker nodes, it's very fast until the results are collected by the driver node. This notably happens in any displays, graphing, or \
# MAGIC when you convert out of the Spark environment. This is why you install the _PySpark SQL functions_ and use _Spark DataFrames_ instead of something like Pandas DataFrames.

# COMMAND ----------

print(sdf_avengers_filtered.describe())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Common Tasks
# MAGIC Below are some examples of common tasks that might need to be done on data in Databricks. I'll add to this section of this primer notebook as necessary.

# COMMAND ----------

# Verify data stucture using the "Data Profile" option on display() output (select the "+" button next to the word "Table" on the output of this cell)
display(sdf_avengers)

# COMMAND ----------

# Aggregate sums of columns with a groupBy() on other columns

sdf_avengers_weights = (sdf_avengers_filtered
                        .withColumn('WeightLbs', F.lit(200)) # in this example, each avenger weighs 200 lbs for simplicity
                       )

sdf_avengers_sum = (sdf_avengers_weights
                    .groupBy('FavoriteColor')
                    .agg(F.sum(F.col('WeightLbs')).alias('TotalWeightLbs')) # the alias method is attached to the F.sum() function to rename the output of F.sum()
                   )

display(sdf_avengers_sum)

# COMMAND ----------

# MAGIC %md
# MAGIC A complete list of functions within PySpark can be found here: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html

# COMMAND ----------

# MAGIC %md
# MAGIC ## Window Functions
# MAGIC As with most SQL operations, windowing can dramatically improve your code's performance. I've found them to b the solution to a lot of\
# MAGIC headaches with compute limitations when using PySpark. Below are some simple syntax examples of what might be useful to know.

# COMMAND ----------

# Importing necessary class
from pyspark.sql.window import Window as W

# Partitioning within the data
sdf_avengers_partition = (sdf_avengers_weights
                          .withColumn("ColorSumWeightLbs", 
                                      F.sum("WeightLbs").over(W.partitionBy(["FavoriteColor"]))
                                      )
                          )
display(sdf_avengers_partition)



# Ordering data
sdf_avengers_ordered = (sdf_avengers_filtered
                        .withColumn("AlphaRank",
                                    F.rank().over(W.orderBy("LastName")) # NOTE: The parameter for these Window functions can be a string or list of strings
                                    )
                        .withColumn("AlphaRankWithinColor",
                                    F.rank().over(W.partitionBy("FavoriteColor").orderBy("LastName"))
                                    )
                        )
display(sdf_avengers_ordered)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Suggested Coding Style Guide Standards
# MAGIC These guidelines aid in the readability of code to prevent mistakes, help diagnose errors, organize thoughts, and storytell in code. \
# MAGIC The code belongs to the team, not to the individual. An experienced coder should be able to read any code, but it helps to focus on \
# MAGIC the output of the code instead of dedicating brain power to just reading it.\
# MAGIC If using VS Code to work on notebooks outside of Databricks, the Ruff Python linter and formatter is used to check code correct-ness and to format the code.\
# MAGIC Check with another team member for setup instructions.

# COMMAND ----------

# MAGIC %md
# MAGIC **Use %md Markdown Cells for section headers**

# COMMAND ----------

# MAGIC %md
# MAGIC **Import all used libraries and datatypes at the top of each notebook**

# COMMAND ----------

# MAGIC %md
# MAGIC **Label and name entities beginning with an abbreviation for what they are** *(Example Below)* \
# MAGIC - sdf = Spark DataFrame
# MAGIC - str = String Variable
# MAGIC - n = Integer Variable
# MAGIC - bool = Boolean Variable
# MAGIC - doub = Double Variable
# MAGIC - ctrl = Control Variable for Program Flow
# MAGIC - l = List
# MAGIC - dict = Dictionary

# COMMAND ----------

# MAGIC %md
# MAGIC **Any SQL cells should be auto-formatted with Databricks** *(Example Below)*

# COMMAND ----------

# This is a Python command to create a temporary table that is accessible by the SQL magic command cells
sdf_avengers.createOrReplaceTempView('sql_avengers')

# COMMAND ----------

# MAGIC %sql
# MAGIC select FirstName, LastName from sql_avengers where ID = 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   FirstName,
# MAGIC   LastName
# MAGIC from
# MAGIC   sql_avengers
# MAGIC where
# MAGIC   ID = 2;

# COMMAND ----------

# Note that the output of the SQL above was stored into a Spark DataFrame, "_sqldf", so it can be accessed in Python as well
display(_sqldf)

# COMMAND ----------

# MAGIC %md
# MAGIC **PySpark Specific Style Guide Adapted from Palantir's GitHub:** https://github.com/palantir/pyspark-style-guide
