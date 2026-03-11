from pyspark.sql import SparkSession
from pyspark.sql.functions import log

spark=SparkSession.builder.appName("LOG").getOrCreate()

data=[(10,),(100,)]
df=spark.createDataFrame(data,["num"])

df.select(log("num")).show()

spark.stop()