from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("StringFunctions").getOrCreate()

data = [("swati","machine learning"),
        ("rahul","big data"),
        ("anita","data science")]

columns = ["name","skill"]

df = spark.createDataFrame(data, columns)
df.select(upper("name").alias("upper_name")).show()