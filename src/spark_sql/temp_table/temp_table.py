from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SQLExample").getOrCreate()

data = [("Swati",22),("Rahul",25),("Anita",23)]

df = spark.createDataFrame(data, ["Name","Age"])

df.show()
df.createOrReplaceTempView("students")

result = spark.sql("SELECT * FROM students WHERE Age > 22")

result.show()