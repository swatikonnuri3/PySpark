from pyspark.sql import SparkSession
from pyspark.sql.functions import sqrt

spark=SparkSession.builder.appName("SQRT").getOrCreate()

data=[(4,),(9,),(16,)]
df=spark.createDataFrame(data,["num"])

df.select(sqrt("num")).show()

spark.stop()