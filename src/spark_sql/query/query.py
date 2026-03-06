from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Spark SQL Example").getOrCreate()

data=[("Alice",25),("Bob",30)]

df=spark.createDataFrame(data,["Name","Age"])

df.createOrReplaceTempView("people")

result=spark.sql("SELECT * FROM people WHERE Age > 25")

result.show()