from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("rdd_collect").getOrCreate()
sc = spark.sparkContext
data = [1, 2, 3, 4]
rdd = sc.parallelize(data)
result = rdd.collect()
print("Collected data:")
print(result)