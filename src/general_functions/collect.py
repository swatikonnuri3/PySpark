from pyspark import SparkContext

sc=SparkContext("local","Collect Action")

rdd=sc.parallelize([10,20,30])

print(rdd.collect())