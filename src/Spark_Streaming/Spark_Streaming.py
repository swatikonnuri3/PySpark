df.createOrReplaceTempView("students")

result = spark.sql("SELECT * FROM students WHERE Age > 22")

result.show()