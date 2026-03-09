from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("count").getOrCreate()
data = [("aaa",25),("bbb",30),("ccc",28)]
df = spark.createDataFrame(data,["Name","Age"])
print("Total rows:", df.count())
spark.stop()