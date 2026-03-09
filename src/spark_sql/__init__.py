from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("temp_table").getOrCreate()
data = [("aaa", 50000),("bbb", 60000),("ccc", 45000)]
columns = ["Name", "Salary"]
df = spark.createDataFrame(data, columns)
df.createOrReplaceTempView("employee_table")
spark.sql("SELECT Name FROM employee_table").show()
spark.stop()