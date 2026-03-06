from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Create DataFrame").getOrCreate()

data=[("Swati",22),("Rahul",24),("Anita",26)]

columns=["Name","Age"]

df=spark.createDataFrame(data,columns)

df.show()