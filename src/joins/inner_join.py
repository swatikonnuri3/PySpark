from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("InnerJoin").getOrCreate()

data1 = [(1,"Alice"),(2,"Bob"),(3,"Charlie")]
data2 = [(1,"HR"),(2,"IT"),(4,"Finance")]

df1 = spark.createDataFrame(data1,["id","name"])
df2 = spark.createDataFrame(data2,["id","dept"])

result = df1.join(df2,"id","inner")
result.show()

spark.stop()