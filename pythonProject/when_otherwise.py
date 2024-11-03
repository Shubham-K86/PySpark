import os

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

os.environ["PYSPARK_PYTHON"] = "PYTHON"

spark = SparkSession.builder \
    .appName("Spark-Program") \
    .master("local[*]") \
    .getOrCreate()

# data = [
# ("Ajay", 12, 90000),
# ("Vijay", 8, 75000),
# ("Bishal", 10, 60000),
# ("Karthik", 5, 50000),
# ("Deepak", 15, 120000),
# ("Neha", 7, 65000)
# ]
#
# schema = "name string,experience int,salary int"
#
# df = spark.createDataFrame(data,schema)
#
# df1 = df.select(
#     col("name"),
#     col("experience"),
#     col("salary"),
#     when((col("experience") >= 10) & (col("salary") > 80000), "Senior high salary")
#     .when((col("experience") >= 10) & (col("salary") <= 80000), "Senior low salary")
#     .when((col("experience") < 10) & (col("salary") > 80000), "Junior high salary")
#     .otherwise("Junior low salary")
#     .alias("job_category")
# )
#
# df1.createOrReplaceTempView("employees")
# df2_sql = spark.sql("""
#         select name ,experience, salary,
#         case
#         when experience >= 10 and salary > 80000 then 'Senior high salary'
#         when experience >= 10 and salary <= 80000 then 'Senior low salary'
#         when experience < 10 and salary > 80000 then 'Junior high salary'
#         else 'Junior low salary'
#         end as Job_Category
#         from employees;
# """)
#
# df1.show()
# df2_sql.show()

# Question 1: Employee Status Check

# employees = [
#       ("karthik", "2024-11-01"),
#       ("neha", "2024-10-20"),
#       ("priya", "2024-10-28"),
#       ("mohan", "2024-11-02"),
#       ("ajay", "2024-09-15"),
#       ("vijay", "2024-10-30"),
#       ("veer", "2024-10-25"),
#       ("aatish", "2024-10-10"),
#       ("animesh", "2024-10-15"),
#       ("nishad", "2024-11-01"),
#       ("varun", "2024-10-05"),
#       ("aadil", "2024-09-30")
#     ]
#
# employees_schema = "name String, last_checkin String"
# df = spark.createDataFrame(employees,employees_schema)
# days_interval = df.withColumn("days_interval",datediff(current_date(),col("last_checkin")))
# status_ = days_interval.select(initcap(col("name")).alias("name"),
#                  col("last_checkin"),
#                  when (col("days_interval") <= 7 , "Active").otherwise("Inactive").alias("checkin_status")
#                  )
#
# status_.show()
#
# employees_tbl = df.createOrReplaceTempView("employees")
# employees_sql = spark.sql("""
# select initcap(name) as name,last_checkin,
# case
# when datediff(current_date(),last_checkin) <= 7 then 'Active' else 'Inactive' end as checkin_status
# from employees;
# """)
# employees_sql.show()

# Question 2: Sales Performance by Agent

# sales = [
#       ("karthik", 60000),
#       ("neha", 48000),
#       ("priya", 30000),
#       ("mohan", 24000),
#       ("ajay", 52000),
#       ("vijay", 45000),
#       ("veer", 70000),
#       ("aatish", 23000),
#       ("animesh", 15000),
#       ("nishad", 8000),
#       ("varun", 29000),
#       ("aadil", 32000)
#     ]
# sales_schema = "name string, total_sales int"
# df = spark.createDataFrame(sales,sales_schema)
# sales_performance = df.select(initcap(col("name")).alias("name"),col("total_sales"),
#       when (col("total_sales") > 50000, "Excellent")
#         .when((col("total_sales") > 25000) & (col("total_sales") <=50000),"Good")
#         .otherwise("Needs Improvement").alias("sales_statues")
#     )
# sum_sales = sales_performance.groupBy("sales_statues").agg(sum(col("total_sales")).alias("total_sum"))
# sum_sales.show()
#
# sales_tbl = df.createOrReplaceTempView("sales")
# sales_sql = spark.sql("""
# with cte as(
# select initcap(name) as name
# ,total_sales ,
# case
# when total_sales > 50000 then 'Excellent'
# when total_sales between 25000 and 50000 then 'Good'
# else 'Need Improvement'
# end as sales_statues
# from sales)
# select sales_statues,sum(total_sales) as total_sum
# from cte
# group by sales_statues;
# """)
# sales_sql.show()

# Question 3: Project Allocation and Workload Analysis

# workload = [
#       ("karthik", "ProjectA", 120),
#       ("karthik", "ProjectB", 100),
#       ("neha", "ProjectC", 80),
#       ("neha", "ProjectD", 30),
#       ("priya", "ProjectE", 110),
#       ("mohan", "ProjectF", 40),
#       ("ajay", "ProjectG", 70),
#       ("vijay", "ProjectH", 150),
#       ("veer", "ProjectI", 190),
#       ("aatish", "ProjectJ", 60),
#       ("animesh", "ProjectK", 95),
#       ("nishad", "ProjectL", 210),
#       ("varun", "ProjectM", 50),
#       ("aadil", "ProjectN", 90)
#       ]
# workload_schema = "name string, project string, hours int"
# df = spark.createDataFrame(workload,workload_schema)
# df1 = df.select(initcap(col("name")).alias("name"),
#       col("project"),
#       col("hours"),
#       when(col("hours") > 200, "Overloaded")
#       .when((col("hours") >= 100) & (col("hours") <= 200), "Balanced")
#       .otherwise("Underutilized").alias("workload")
#     )
# df2 = df1.groupBy(col("name")).agg(count(col("workload")).alias("workload_count"))
#
# df2.show()
#
# workload_tbl = df.createOrReplaceTempView("workload")
# workload_sql = spark.sql("""
# with cte as (
# select initcap(name) as name,project,hours,
# case
# when  hours > 200 then 'Overloaded'
# when hours between 100 and 200 then 'Balanced'
# else 'Underutilized'
# end as workload
# from workload
# )
# select name,count(workload) as workload_count
# from cte
# group by name;
# """)
# workload_sql.show()

# 5. Overtime Calculation for Employees

# employees = [
#       ("karthik", 62),
#       ("neha", 50),
#       ("priya", 30),
#       ("mohan", 65),
#       ("ajay", 40),
#       ("vijay", 47),
#       ("veer", 55),
#       ("aatish", 30),
#       ("animesh", 75),
#       ("nishad", 60)
#     ]
# employee_schema = "name string,hours_worked int"
# df = spark.createDataFrame(employees,employee_schema)
# df1 = df.withColumn("overtime_status",
#                     when(col("hours_worked") > 60 ,"Excessive Overtime")
#                     .when(col("hours_worked").between(45,60),"Standard Overtime")
#                     .otherwise("No Overtime")) \
#                     .withColumn("name",initcap(col("name")))
# df2 = df1.groupBy(col("overtime_status")).agg(count(col("name")).alias("total_overtime_status"))
# df2.show()
#
# employees_tbl = df.createOrReplaceTempView("employees")
# employees_sql = spark.sql("""
# with cte as(
# select initcap(name) as name,hours_worked,
# case
# when hours_worked > 60 then 'Excessive Overtime'
# when hours_worked > 45 and hours_worked <=60 then 'Standard Overtime'
# else 'No Overtime'
# end as overtime_status
# from employees
# )
# select overtime_status,count("name") as total_overtime_status
# from cte
# group by overtime_status;
# """)
# employees_sql.show()

# 6. Customer Age Grouping

customers = [
    ("karthik", 22),
    ("neha", 28),
    ("priya", 40),
    ("mohan", 55),
    ("ajay", 32),
    ("vijay", 18),
    ("veer", 47),
    ("aatish", 38),
    ("animesh", 60),
    ("nishad", 25)
]
customers_schema = "name string,age int"
df = spark.createDataFrame(customers, customers_schema)
df1 = df.withColumn("customer_age",
                           when(col("age") < 25, "Youth")
                           .when(col("age").between(25, 45), "Adult")
                           .otherwise("Senior")
                           ) \
    .withColumn("name", initcap(col("name")))
df2 = df1.groupBy(col("customer_age")).agg(count(col("name")).alias("customer_count"))
df2.show()

customers_tbl = df.createOrReplaceTempView("customers")
customers_sql = spark.sql("""
with cte as (
select initcap(name) as name ,age,
case
when age < 25 then 'Youth'
when age between 25 and 45 then 'Adult'
else 'Senior'
end as customer_age
from customers
)
select customer_age,count(name) as customer_count
from cte
group by customer_age;
""")
customers_sql.show()

spark.stop()
