# print("hello world")

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg

os.environ["PYSPARK_PYTHON"] = "C:/Program Files/Python/Python37/python.exe" # or "python" depending on your Python executable

# Create Spark Session
spark = SparkSession.builder \
    .appName("Example") \
    .master("local[*]") \
    .getOrCreate()


schema="id int,Name string,Age int,Salary int,city string,details int,mean int"

df = spark.read \
    .format("csv") \
    .option("header", True) \
    .schema(schema) \
    .option("path", "C:/Users/SHUBHAM/Downloads/info.csv") \
    .load()

df.show()
