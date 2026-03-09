from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("rdd_filter").getOrCreate()
sc = spark.sparkContext
numbers = [5, 10, 15, 20, 25]
rdd = sc.parallelize(numbers)
filtered_numbers = rdd.filter(lambda x: x > 10)
print("Numbers greater than 10:")
print(filtered_numbers.collect())