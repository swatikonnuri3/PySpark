from pyspark.sql import SparkSession
from pyspark import StorageLevel

spark = SparkSession.builder.appName("Persist Example").getOrCreate()

data=[("A",10),("B",20),("C",30)]
columns=["Letter","Value"]

df=spark.createDataFrame(data,columns)

df.persist(StorageLevel.MEMORY_AND_DISK)

df.show()