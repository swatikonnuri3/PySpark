from pyspark.sql import SparkSession
from pyspark.sql.functions import floor

spark=SparkSession.builder.appName("FLOOR").getOrCreate()

data=[(10.9,),(5.3,)]
df=spark.createDataFrame(data,["num"])

df.select(floor("num")).show()

spark.stop()