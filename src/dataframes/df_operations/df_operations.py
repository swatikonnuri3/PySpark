from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("DF Operations").getOrCreate()

data=[("Alice",25),("Bob",30),("Charlie",35)]

df=spark.createDataFrame(data,["Name","Age"])

df.select("Name").show()

df.filter(df.Age>25).show()