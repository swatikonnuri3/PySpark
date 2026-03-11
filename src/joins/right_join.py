from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("RightJoin").getOrCreate()

data1=[(1,"Alice")]
data2=[(1,"HR"),(2,"IT")]

df1=spark.createDataFrame(data1,["id","name"])
df2=spark.createDataFrame(data2,["id","dept"])

df1.join(df2,"id","right").show()

spark.stop()