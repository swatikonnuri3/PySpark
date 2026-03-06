from pyspark import SparkContext

sc=SparkContext("local","Map Example")

rdd=sc.parallelize([1,2,3,4])

result=rdd.map(lambda x:x*2)

print(result.collect())