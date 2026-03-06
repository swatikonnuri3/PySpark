from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Cache Example") \
    .master("local[*]") \
    .getOrCreate()

data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
columns = ["Name", "Age"]

df = spark.createDataFrame(data, columns)

df.cache()

df.show()

print("DataFrame cached successfully")