import os

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *

os.environ["PYSPARK_PYTHON"] = "PYTHON"

# spark = SparkSession.builder \
#     .appName("Spark-Program") \
#     .master("local[*]") \
#     .getOrCreate()

spark_conf = SparkConf()
spark_conf.set("spark.app.name", "Spark-Program")
spark_conf.set("spark.master", "local[*]")
spark_conf.set("spark.executor.memory", "4g")
# spark_conf.set("spark.executor.cores","2")
# spark_conf.set("spark.driver.memory","1g")

spark = SparkSession.builder \
    .config(conf=spark_conf) \
    .getOrCreate()

# DDL approach :
# schema_ddl = "id int,Name string,Salary int,City string"
# programmatic approach :
schema_prog = StructType(
    [
        StructField("id", IntegerType(), nullable=False),
        StructField("Name", StringType(), nullable=False),
        StructField("Salary", IntegerType(), nullable=False),
        StructField("City", StringType(), nullable=False)
    ]
)

df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("mode", "PERMISSIVE") \
    .option("columnNameOfCorruptRecord", "_corrupt_record") \
    .schema(schema_prog) \
    .load("C:/Users/SHUBHAM/Downloads/details.csv")

# permissive mode :
# required column name is missing from data ,column value populated as null.
# adding extra column in data but not define in schema in this case data load in table.
# value data type mismatch then in this case populated null instated of mismatched value


df.show()

spark.stop()
