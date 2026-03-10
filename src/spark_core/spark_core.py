from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CoreExample").getOrCreate()

data = [1,2,3,4,5]

rdd = spark.sparkContext.parallelize(data)

print(rdd.collect())
#parallelize() → distributes data across executors
#collect() → brings data back to driver