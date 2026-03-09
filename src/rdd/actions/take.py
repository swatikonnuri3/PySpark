from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("rdd_take").getOrCreate()
sc = spark.sparkContext
numbers = [100, 200, 300, 400, 500]
rdd = sc.parallelize(numbers)
print("First element:")
print(rdd.first())
print("First three elements:")
print(rdd.take(3))