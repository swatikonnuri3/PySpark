from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("ExplodeExample").getOrCreate()

data = [
    ("Swati", ["Python","SQL","Spark"]),
    ("Rahul", ["Java","Python"]),
    ("Anita", None)
]

df = spark.createDataFrame(data, ["Name","Skills"])
df.show(truncate=False)
df.select("Name", posexplode_outer("Skills")).show()