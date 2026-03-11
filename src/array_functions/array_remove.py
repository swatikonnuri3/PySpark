from pyspark.sql import SparkSession
from pyspark.sql.functions import array_remove

spark = SparkSession.builder.appName("ARRAY_REMOVE").getOrCreate()

data=[([1,2,3,2],),([4,5,6],)]
df=spark.createDataFrame(data,["numbers"])

df.select(array_remove("numbers",2).alias("removed_array")).show()

spark.stop()