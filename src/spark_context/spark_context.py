from pyspark import SparkContext

sc=SparkContext("local","SparkContext Example")

rdd=sc.parallelize(["Spark","Python","BigData"])

print(rdd.collect())