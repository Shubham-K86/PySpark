import os
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession



os.environ["PYSPARK_PYTHON"] = "C:/Program Files/Python/Python37/python.exe"  # or "python" depending on your Python executable

# conf = SparkConf().set("spark.ui.port", "4041")
# sc = SparkContext(conf=conf)
# spark = SparkSession(sc)

spark = SparkSession.builder \
     .appName("spark-program") \
     .master("local[*]") \
     .getOrCreate()

# initial number of partition
# partition = df.rdd.getNumPartitions()
# print(f"number of partition : {partition}")
# repartition the initial DF
# repartition = df.repartition(5)
# new DF got created after repartition and store in variable
# df_repartition =  repartition.rdd.getNumPartitions()
# print(f"number of repartition : {df_repartition}")
# final DF partitions
# final_partition = repartition.rdd.getNumPartitions()
# print(f"number of final_partition : {final_partition}")

# df = spark.read.format("csv") \
#      .option("header", "true") \
#      .option("inferSchaema","true") \
#      .option("path", "C:/Users/SHUBHAM/Downloads/orders1.csv") \
#      .load()
# print schema of the data
# df.printSchema()

# renamed existing column name to new col name
# renamed_col_df = df.withColumnRenamed("order_statues","status")

from pyspark.sql.functions import *
from pyspark.sql.types import *
# changing data type string to timestamp
# datatype_change_df = df.withColumn("order_date_new",to_timestamp("order_date","yyyy-MM-dd HH:mm"))
# datatype_change_df.printSchema()

# filtering records from DF
# filter_where_df = df.where("customer_id == 11599")
# filter_df = df.filter("customer_id == 11599")

# converting spark DF to spark table for tabular view
# spark_table_df = df.createOrReplaceTempView("orders_table")
# spark_Sql_df = spark.sql("""select * from orders_table where order_statues = "CLOSED" """);

# convert spark table to DF
# table_df = spark.read.table("spark_Sql_df")

# spark.sql(""" create database if not exists retail_db""")
# spark.sql(""" show databases""").show()
# spark.sql("""show databases""").filter("namespace like 'retail%'").show()
# spark.sql("""use retail_db""")
# spark.sql("""show tables""").show()
# spark.sql("""create table retail_db.orders(
# order_id integer,
# order_date string,
# customer_id integer,
# order_status String)""")


# top 15 customers who palced the most number of order
# customer_df = df.groupby("customer_id").count().orderBy(col("count").desc()).limit(15)
# customer_df.show()

# number of orders under each order status
# order_status_df = df.groupBy("order_statues").count().alias("order_count").orderBy(col("count").desc())
# order_status_df.show()

# number of active customers who placed at lease one order
# distinct_df = df.select("customer_id").distinct().count()
# print(distinct_df)

# customers with most numbers of closed orders
# closed_df = df.filter("order_statues == 'CLOSED'").groupby("customer_id").count().sort(col("count").desc())
# closed_df.show()

# data = [(1,"shubham",100515,"pune",20,803302),
#         (2,"sachin",100516,"patna",67,803303),
#         (3,"divesh",100517,"bhopal",55,803304),
#         (4,"samarpit",100518,"indore",40,803305),
#         (5,"rahul",100519,"mumbai",33,803306)]

# DDL schema
# schema = "id int, name String, emp_id int, city String, age int, pincode int"

# programetic approach
# schema =StructType([
#     StructField("id",IntegerType(),False),
#     StructField("name",StringType(),False),
#     StructField("emp_id",IntegerType(),True),
#     StructField("city",StringType(),True),
#     StructField("age",IntegerType(),True),
#     StructField("pincode",IntegerType(),True)
# ])

# converting normal list to DF
# df = spark.createDataFrame(data,schema)

# schema = "id int,name String,emp_id int,join_date String,city String,age int,pincode int"
# df = spark.read \
#     .format("csv") \
#     .option("header","true") \
#     .schema(schema) \
#     .option("dateFormat"," MM-dd-yyyy") \
#     .option("mode","DROPMALFORMED") \
#     .load("C:/Users/SHUBHAM/Downloads/dummy_data.csv")

# df1 = df.withColumn("join_date",to_date("join_date","mm-dd-yyyy"))
# # df1 = df.withColumn("join_date", col("join_date").cast(DateType()))

# coverting rdd to data frame
# original_rdd = spark.sparkContext.textFile("C:/Users/SHUBHAM/Downloads/dummy_data.csv")
# schema = "id int,name String,emp_id int,join_date String,city String,age int,pincode int"

# map_rdd = original_rdd.map(lambda x:(int(x.split(",")[0]),
#                                      x.split(",")[1],
#                                      (int(x.split(",")[2])),
#                                      x.split(",")[3],
#                                      x.split(",")[4],
#                                      (int(x.split(",")[5])),
#                                      (int(x.split(",")[6]))))

# converted_df = spark.createDataFrame(original_rdd).toDF(["id","name","emp_id","join_date","city","age","pincode"])

# df = spark.read \
#     .format("csv") \
#     .option("header","true") \
#     .load("C:/Users/SHUBHAM/Downloads/dummy_data2.csv")
#
# # select_df = df.select("*",expr("quantity * product_price as subtotal"))
# selectExpr_df = df.selectExpr("*","quantity * product_price as subtotal")

# Parallelize method
# data = [1,2,3,4,5]
# print(type(data))
# rdd = spark.sparkContext.parallelize(data)
# rdd.saveAsTextFile("C:/Users/SHUBHAM/Downloads/result24.txt")

# finding average using normal list
# average_mean = rdd.mean()
# print("average is :" ,average_mean)

# reduce_rdd = rdd.reduce(lambda x,y:x+y)
# print(reduce_rdd)
# count_rdd = rdd.count()
# average_rdd = reduce_rdd/count_rdd
# print(average_rdd)

# filter condition
# finding odd count
# use collect() when we want odd number as an list format [1, 3, 5]
# filter_odd = rdd.filter(lambda x:x%2!=0).collect()
# filter_odd = rdd.filter(lambda x:x%2!=0)
# print("odd :", filter_odd)
# count_rdd = filter_odd.count()
# print("total odd coount :",count_rdd)

# finding even count
# filter_even = rdd.filter(lambda x:x%2==0)
# count_even = filter_even.count()
# print("even count:",count_even)

# data1 = [("1", "apple"), ("2", "banana"), ("3", "orange")]
# data2 = [("1", "red"), ("2", "yellow"), ("4", "green")]
#
# rdd1 = spark.sparkContext.parallelize(data1)
# rdd2 = spark.sparkContext.parallelize(data2)
#
# join_rdd = rdd1.join(rdd2)
# for i in join_rdd.collect():
#      print(i)

# data = [1,2,3,4,5,1,3,4]
# rdd = spark.sparkContext.parallelize(data)
# # distinct_rdd = rdd.distinct().collect() [1, 2, 3, 4, 5]
# # count_rdd = distinct_rdd.count()
# distinct_rdd = rdd.distinct()
# collect_rdd = distinct_rdd.collect()
# for i in collect_rdd :
#      print(i)

# substracting rdds means reutrn unmatched value between two list
# data1 = [1,2,3,4,5]
# data2 = [3,4,5]
#
# rdd1 = spark.sparkContext.parallelize(data1)
# rdd2 = spark.sparkContext.parallelize(data2)
# substract_rdd = rdd1.subtract(rdd2).collect()
# print(substract_rdd)

# Intersection return only matched value in both RDD
# data1 = [1,2,3,4,5]
# data2 = [4,5,6,7,8]
#
# rdd1 = spark.sparkContext.parallelize(data1)
# rdd2 = spark.sparkContext.parallelize(data2)
# intersection_rdd = rdd1.intersection(rdd2)
# collect_rdd = intersection_rdd.collect()
# for i in collect_rdd:
#      print(i)

# union operation
# data1 = [1,2,3]
# data2 = [3,4,5]
#
# rdd1 = spark.sparkContext.parallelize(data1)
# rdd2 = spark.sparkContext.parallelize(data2)
# union_rdd = rdd1.union(rdd2)
# collect_rdd = union_rdd.collect()
# for i in collect_rdd:
#      print(i)

# Cartesian product creates all posssible combinations of with two rdds
# data1 = [1,2,3]
# data2 = ["A","B"]
#
# rdd1 = spark.sparkContext.parallelize(data1)
# rdd2 = spark.sparkContext.parallelize(data2)
# cartesian_rdd = rdd1.cartesian(rdd2)
# collect_rdd = cartesian_rdd.collect()
# for i in collect_rdd:
#      print(i)

# filter the exact matched string
# data = ["apple", "banana", "orange", "pear", "grape"]
# rdd = spark.sparkContext.parallelize(data)
# searchItem = "pear"
# filter_rdd = rdd.filter(lambda x:x==searchItem)
# collect_rdd = filter_rdd.collect()
# for i  in collect_rdd:
#      print(i)

# filter substring value
# data = ["apple", "banana", "orange", "pear", "grape"]
# rdd = spark.sparkContext.parallelize(data)
# searchSubstring = "an"
# filter_rdd = rdd.filter(lambda x:searchSubstring in x)
# collect_rdd = filter_rdd.collect()
# for i  in collect_rdd:
#      print(i)

# replacing specific string with string
# data = ["apple", "banana", "orange", "pear", "grape"]
# rdd = spark.sparkContext.parallelize(data)
# map_row = rdd.map(lambda x:Row(x))
# schema = StructType([StructField("fruits", StringType(), True)])
# df = spark.createDataFrame(map_row,schema)
# replace_df = df.withColumn("fruits",regexp_replace("fruits","apple","A"))
# replace_df.show()

# data = ["apple", "banana", "orange", "pear", "grape"]
# rdd = spark.sparkContext.parallelize(data)
# map_row = rdd.map(lambda x:Row(x))
# for i in map_row.collect():
#      print(i)

# Using map for element-wise replacemen
# data = ["apple", "banana", "orange", "pear", "grape"]
# rdd = spark.sparkContext.parallelize(data)
# map_row = rdd.map(lambda x:Row(x))
# schema = StructType([StructField("fruits", StringType(), True)])
# def replace_fruit(fruits) :
#      if fruits[0] == "a" :
#           return "kiwi"
#      else:
#           return fruits
# replace_map = rdd.map(replace_fruit)

# ----------------------------------------Aggregation Assignments-------------------------------------------------------

# Finding the count of orders placed by each customer and the total order amount for each customer.

# schema = ("OrderID String, Customer String, Amount Int")
# orderData = [("Order1", "John", 100),("Order2", "Alice", 200),("Order3", "Bob", 150),("Order4", "Alice", 300),("Order5", "Bob", 250),
# ("Order6", "John", 400)]
# df = spark.createDataFrame(orderData,schema)
# order_count_df  = df.groupby(col("Customer")).agg(count(col("OrderID")).alias("count_orders"))
# total_amount_df = df.groupby(col("Customer")).agg(sum(col("Amount")).alias("total_amount"))
# order_count_df.show()
# total_amount_df.show()

#  Finding the average score for each subject and the maximum score for each student.
# schema = ("Student String, Subject String, Score Int")
# scoreData = [
# ("Alice", "Math", 80),
# ("Bob", "Math", 90),
# ("Alice", "Science", 70),
# ("Bob", "Science", 85),
# ("Alice", "English", 75),
# ("Bob", "English", 95)
# ]
# df = spark.createDataFrame(scoreData,schema)
# avg_score_df = df.groupby(col("subject")).agg(avg(col("score")).alias("avg_score"))
# max_score_df = df.groupby(col("Student")).agg(max(col("score")).alias("max_score"))
# avg_score_df.show()
# max_score_df.show()

# Finding the average rating for each movie and the total number of ratings for each movie.
# schema = ("User String, Movie String, Rating Float")
# ratingsData = [
# ("User1", "Movie1", 4.5),
# ("User2", "Movie1", 3.5),
# ("User3", "Movie2", 2.5),
# ("User4", "Movie2", 3.0),
# ("User1", "Movie3", 5.0),
# ("User2", "Movie3", 4.0)
# ]
# df = spark.createDataFrame(ratingsData,schema)
# avg_rating_df = df.groupby(col("movie")).agg(avg(col("rating")).alias("avg_rating"))
# count_rating_df = df.groupby(col("movie")).agg(count(col("rating")).alias("count_rating"))
# avg_rating_df.show()
# count_rating_df.show()

# Finding the count of occurrences for each word in a text document.

# s = StructType([
#      StructField("text",StringType())
# ])
# schema = ("Text String")
# textData = [
#      ("Hello, how are you?"),
#      ("I am fine, thank you!"),
#      ("How about you?")]
# rdd = spark.sparkContext.parallelize(textData)
# explode_df = rdd.select(explode(split(col("rdd"),"\\s+")).alias("word"))
#
# explode_df.show()

# Finding the minimum, maximum, and average temperature for each city in a weather dataset.
# schema = ("City String, Date String, Temperature Float")
# weatherData = [
# ("City1", "2022-01-01", 10.0),
# ("City1", "2022-01-02", 8.5),
# ("City1", "2022-01-03", 12.3),
# ("City2", "2022-01-01", 15.2),
# ("City2", "2022-01-02", 14.1),
# ("City2", "2022-01-03", 16.8)]
# df = spark.createDataFrame(weatherData,schema)
# city_df = df.groupby(col("city")).agg(min(col("Temperature")).alias("min_temp")
#                                    ,max(col("Temperature")).alias("max_temp")
#                                    ,avg(col("Temperature")).alias("avg_temp"))
# city_df.show()

# Finding the count of distinct products purchased by each customer and the total purchase amount for each customer.
# schema = ("Customer String, Product String, Amount Int")
# purchaseData = [
# ("Customer1", "Product1", 100),
# ("Customer1", "Product2", 150),
# ("Customer1", "Product3", 200),
# ("Customer2", "Product2", 120),
# ("Customer2", "Product3", 180),
# ("Customer3", "Product1", 80),
# ("Customer3", "Product3", 250)]
# df = spark.createDataFrame(purchaseData,schema)
# customer_df = df.groupby(col("customer")).agg(countDistinct(col("product")).alias("distinct_product")
#                                               ,sum(col("amount")).alias("total_amount"))
# customer_df.show()

# Finding the top N products with the highest sales amount
# schema = ("Product String, SalesAmount Int")
# salesData = [
# ("Product1", 100),
# ("Product2", 200),
# ("Product3", 150),
# ("Product4", 300),
# ("Product5", 250),
# ("Product6", 180)]
# df = spark.createDataFrame(salesData,schema)
# N=3
# sales_Amount_df = df.orderBy(col("SalesAmount").desc()).limit(N)
#
# sales_Amount_df.show()

from pyspark.sql.window import *

# Finding the cumulative sum of sales amount for each product.
# schema = ("Product String, SalesAmount Int")
# salesData = [
# ("Product1", 100),
# ("Product2", 200),
# ("Product3", 150),
# ("Product4", 300),
# ("Product5", 250),
# ("Product6", 180)]
# df = spark.createDataFrame(salesData,schema)
# sum_window = Window.partitionBy().orderBy(col("SalesAmount").asc())
# df1 = df.withColumn("cumulative_sum",sum(col("SalesAmount")).over(sum_window))
# df1.show()

# Finding the average rating given by each user for each genre in a movie rating dataset.
from pyspark.sql.types import FloatType,DoubleType
# schema = ("User String, Movie String, Genre String, Rating Double")
# ratingData = [
# ("User1", "Movie1", "Action", 4.5),
# ("User1", "Movie2", "Drama", 3.5),
# ("User1", "Movie3", "Comedy", 2.5),
# ("User2", "Movie1", "Action", 3.0),
# ("User2", "Movie2", "Drama", 4.0),
# ("User2", "Movie3", "Comedy", 5.0),
# ("User3", "Movie1", "Action", 5.0),
# ("User3", "Movie2", "Drama", 4.5),
# ("User3", "Movie3", "Comedy", 3.0)]
# df = spark.createDataFrame(ratingData,schema)
# first way
# window = Window.partitionBy("User","Genre").orderBy(col("Rating"))
# df1 = df.withColumn("avg_rating",avg(col("Rating")).over(window))
# second way
# df1 = df.groupby(col("user"),col("genre")).agg(avg(col("rating")).alias("avg_rating"))
# df1.show()

# schema = StructType([
#      StructField("text",StringType())
# ])
# schema = ("text String")
# textData = [
# "Hello, how are you?",
# "I am fine, thank you!",
# "How about you?"
# ]
# df = spark.createDataFrame(textData,schema)
# explode_df = df.withColumn("text",explode(split(col("textData")),(" ")))
# explode_df.show()

# ----------------------------------GROUP BY SET A-----------------------------------------------------------------------------------
# Calculate the total sales revenue for each product category from the given DataFrame.
# schema = ("id Int,product String,category String,revenue Float")
# data = [(1, "ProductA", "Electronics", 1000.0),
# (2, "ProductB", "Clothing", 500.0),
# (3, "ProductC", "Electronics", 800.0),
# (4, "ProductD", "Clothing", 300.0),
# (5, "ProductE", "Electronics", 1200.0)]
# df = spark.createDataFrame(data,schema)
# # df1 = df.groupby(col("category")).agg(sum(col("revenue")).alias("sum_revenue"))
# window = Window.partitionBy(col("category")).orderBy(col("revenue"))
# df1 = df.withColumn("sum_revenue",sum(col("revenue")).over(window))
# df1 = df.groupby(col("category")).agg(
#     sum(col("revenue")).alias("sum_revenue"),
#     first(col("id")).alias("id"),
#     first(col("product")).alias("product"),
#     first(col("revenue")).alias("revenue")
# )
# df1.show()

# Find the average sales price for each product category and display the result in descending order of average price
# df.show()
# df1 = df.groupby(col("category")).agg(avg(col("revenue")).alias("avg_price")).orderBy(col("avg_price").desc())
# df1.show()
#
# # Calculate the total sales revenue for each product category, and also find the category with the highest total sales
# df.show()
# window = Window.partitionBy(col("category")).orderBy(col("revenue"))
# df1 = df.select(col("id"),col("product"),col("category"),col("revenue"),sum("revenue").over(window))
# df2 = df.withColumn("total_revenue",sum(col("revenue")).rank().over(window))
# df1 = df.groupby(col("category")).agg(sum(col("revenue")))
# df1.show()

# Determine the minimum and maximum sales values for each product category
# df.show()
# df1 = df.groupby(col("category")).agg(min(col("revenue")).alias("min"),max(col("revenue")).alias("max"))
# df1.show()

# Calculate the total sales revenue for each product category and each quarter of the year from the given DataFrame, assuming
# there is a "date" column containing dates in the format ’yyyy-MM-dd’.
schema = ("id Int,product String,category String,date String,revenue Float")
data = [(1, "ProductA", "Electronics", "2023-01-15", 1000.0),
(2, "ProductB", "Clothing", "2023-02-20", 500.0),
(3, "ProductC", "Electronics", "2023-04-10", 800.0),
(4, "ProductD", "Clothing", "2023-05-05", 300.0),
(5, "ProductE", "Electronics", "2023-01-25", 1200.0)]
df = spark.createDataFrame(data,schema)
# Apply window function
# window = Window.partitionBy("category").orderBy(col("revenue"))
# df1 = df.withColumn("total_revenue",sum("revenue").over(window)) \
#                .withColumn("year", year(col("date"))) \
#                .withColumn("quarter", quarter(col("date"))) \
#                .withColumn("rank",rank().over(window))
#
# df3= df1.select(col("id"),col("product"),col("category"),col("date"),col("revenue"),col("year"),col("quarter"),col("total_revenue"),col("rank"))

# apply groupBy
# df1 = df.withColumn("year", year(col("date"))).withColumn("quarter", quarter(col("date")))
# df2 = df1.groupby(col("category"),col("year"),col("quarter")).agg(sum(col("revenue")))
# df3.show()

# Calculate the average sales price for each product category and year, and add a new column indicating whether the average
# is above a certain threshold, e.g., 800.
# df1 = df.withColumn("year", year(col("date")))
# df2 = df1.groupby(col("category"),col("year")).agg(avg(col("revenue")).alias("avg_sales"))
# df3 = df2.withColumn("threshold",when(col("avg_sales") > 800 ,"above_threshold").otherwise("low_threshold"))
# df3.show()


# df1 = df.withColumn("year", year(to_date(col("date"))))
# wd = Window.partitionBy("product", "year")
# df2 = df1.withColumn("Avg_price", avg(col("revenue")).over(wd))
# df3 = df2.withColumn("Indicater", when(col("Avg_price") > 800, "Meet threshold ").otherwise("Not meet the threshold "))
# df3.show()
# selectExpr_df.show()
# filter_where_df.show(truncate=False)
# -------------------------------------------------DATE MANIPULATION----------------------------------------------------
# You have date and time information in string format and need to convert them into proper date-time objects.
# df = [("2023-10-07", "15:30:00")]
# # schema = ("date_str String, time_str String")
# schema = StructType([
#      StructField("date_str",StringType()),
#      StructField("time_str",StringType())
# ])
# df1 = spark.createDataFrame(df,schema)
# df1.printSchema()
# # date_df = df1.withColumn("date",to_date(col("date_str"))).withColumn("time",to_timestamp(col("time_str")))
# date_df = df1.withColumn("date_str",to_date(col("date_str"))).withColumn("time_str",to_timestamp(col("time_str")))
# date_df.printSchema()
# date_df.show()

# You need to perform calculations involving date and time values, such as finding the difference between two dates
# or adding/subtracting days, months, or years.
# df = [("2023-10-07", "2023-10-10")]
# schema = StructType([
#      StructField("date1",StringType()),
#      StructField("date2",StringType())
# ])
# df1 = spark.createDataFrame(df,schema)
# df1.printSchema()
# datediff_df = df1.withColumn("date",datediff("date2","date1"))
# datediff_df.show()

# You want to group data by date and perform aggregations like sum or average on groups.
# df = [("2023-10-07", 10), ("2023-10-07", 15), ("2023-10-08", 20)]
# schema = StructType([
#      StructField("date",StringType()),
#      StructField("value",IntegerType())
# ])
# df1 = spark.createDataFrame(df,schema)
# df1.printSchema()
# # sum_df = df1.groupby(col("date")).agg(sum(col("value")).alias("sum_value"))
# sum_df = df1.groupby(trunc("date","dd")).agg(sum(col("value")).alias("sum_value"))
# sum_df.show()

# Dealing with date and time values in different time zones and converting between them.
# df = [("2023-10-07 12:00:00", "UTC"), ("2023-10-07 12:00:00", "America/New_York")]
# schema = StructType([
#      StructField("timestamp_str",StringType()),
#      StructField("timezone",StringType())
# ])
# df1 = spark.createDataFrame(df,schema)
# converted_time = df1.withColumn("converted_time",from_utc_timestamp("timestamp_str","timezone"))
# converted_time.show()

# Calculate the number of days between a date column and the current date for each row in the DataFrame.
# df = [("2023-10-07", "2023-10-10", "2023-10-01")]
# schema = StructType([
#                StructField("date_str1",StringType()),
#                StructField("date_str2", StringType()),
#                StructField("date_str3", StringType())
# ])
# df1 = spark.createDataFrame(df,schema)
# df1.show()
# df1.printSchema()
# date_df = df1.withColumn("date1", to_date("date_str1")) \
#                .withColumn("date2", to_date("date_str2")) \
#                .withColumn("date3", to_date("date_str3")) \
#                .withColumn("current_date_diff1",datediff(current_date(),"date1")) \
#                .withColumn("current_date_diff2", datediff(current_date(), "date2")) \
#                .withColumn("current_date_diff3", datediff(current_date(), "date3"))
# date_df.printSchema()
# date_df.show()

df = [("2023-10-07", "2023-10-10", "2023-10-01")]
schema = StructType([
    StructField("date_str1", StringType(), True),
    StructField("date_str2", StringType(), True),
    StructField("date_str3", StringType(), True)
])
df1 = spark.createDataFrame(df,schema)
df2 = df1.select(explode(array("date_str1", "date_str2", "date_str3")).alias("date_str"))
df3 = df2.withColumn("date_str",to_date("date_str"))
# df4 = df3.withColumn("min_date",min("date_str")) \
#         .withColumn("max_date",max("date_str"))

df3.show()

# df = [("john",50000),("jane",60000),("doe",55000)]
# schema = ("name String,salary int")
# df1=spark.createDataFrame(df,schema)
# df2 = df1.withColumn("bonus",col("salary") * 0.1)
# df2.show()


spark.stop()
