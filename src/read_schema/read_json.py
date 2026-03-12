from pyspark.sql import SparkSession
from pyspark.sql.functions import explode

spark=SparkSession.builder.appName("read_json").getOrCreate()
df=spark.read.format("json").option(key="multiline",value=True).load("../../users.json")
df.printSchema()
flat=df.select(explode("users").alias("user"))
users_df=flat.select("user.*")
users_df.show(truncate=False)