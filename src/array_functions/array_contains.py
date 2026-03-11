from pyspark.sql import SparkSession
from pyspark.sql.functions import array_contains

spark = SparkSession.builder.appName("ARRAY_CONTAINS").getOrCreate()

data=[([1,2,3],),([4,5,6],)]
df=spark.createDataFrame(data,["numbers"])

df.select("numbers",array_contains("numbers",2).alias("contains_2")).show()

spark.stop()