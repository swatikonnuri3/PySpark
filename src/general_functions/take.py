from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("take").getOrCreate()
data = [("aaa",25),("bbb",30),("ccc",28)]
df = spark.createDataFrame(data,["Name","Age"])
rows = df.take(2)
for r in rows:
    print(r)
spark.stop()