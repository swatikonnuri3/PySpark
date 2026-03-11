from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("LeftJoin").getOrCreate()

data1=[(1,"Alice"),(2,"Bob")]
data2=[(2,"IT")]

df1=spark.createDataFrame(data1,["id","name"])
df2=spark.createDataFrame(data2,["id","dept"])

df1.join(df2,"id","left").show()

spark.stop()