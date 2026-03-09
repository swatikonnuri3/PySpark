from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("rdd_count").getOrCreate()
sc = spark.sparkContext
numbers = [3, 6, 9, 12, 15]
rdd = sc.parallelize(numbers)
total = rdd.count()
print("Total number of elements:", total)