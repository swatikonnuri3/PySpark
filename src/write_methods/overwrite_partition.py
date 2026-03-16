import os
import sys

os.environ["PYSPARK_PYTHON"]        = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["JAVA_HOME"]   = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.18.8-hotspot"
os.environ["HADOOP_HOME"] = r"C:\Users\Swati\PycharmProjects\PySpark\hadoop"
os.environ["PATH"] = r"C:\Users\Swati\PycharmProjects\PySpark\hadoop\bin;" + os.environ["PATH"]
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("OverwritePartitionExample") \
    .master("local[*]") \
    .getOrCreate()

data = [
    (1, "Swati", "IT"),
    (2, "Rahul", "HR"),
    (3, "Anita", "IT"),
    (4, "John", "Finance")
]

columns = ["id", "name", "department"]

df = spark.createDataFrame(data, columns)

df.write \
    .mode("overwrite") \
    .partitionBy("department") \
    .parquet("C:/Users/Swati/PycharmProjects/PySpark/data/partition_example")
new_data = [
    (5, "Kiran", "IT"),
    (6, "Priya", "IT")
]

new_df = spark.createDataFrame(new_data, columns)

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

new_df.write \
    .mode("overwrite") \
    .partitionBy("department") \
    .parquet("C:/Users/Swati/PycharmProjects/PySpark/data/partition_example")