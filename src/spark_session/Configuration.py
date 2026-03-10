from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ConfigExample") \
    .config("spark.executor.memory","1g") \
    .config("spark.driver.memory","1g") \
    .getOrCreate()
#spark.executor.memory	memory for executors
#spark.driver.memory	memory for driver