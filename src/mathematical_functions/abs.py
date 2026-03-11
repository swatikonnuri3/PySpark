from pyspark.sql import SparkSession
from pyspark.sql.functions import abs

spark=SparkSession.builder.appName("ABS").getOrCreate()

data=[(-10,),(-20,),(30,)]
df=spark.createDataFrame(data,["value"])

df.select(abs("value")).show()

spark.stop()