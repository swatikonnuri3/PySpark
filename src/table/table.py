import os
import sys

os.environ["PYSPARK_PYTHON"]        = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["JAVA_HOME"]   = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.18.8-hotspot"
os.environ["HADOOP_HOME"] = r"C:\Users\Swati\PycharmProjects\PySpark\hadoop"
os.environ["PATH"] = r"C:\Users\Swati\PycharmProjects\PySpark\hadoop\bin;" + os.environ["PATH"]

from pyspark.sql import SparkSession

warehouse = "C:/Users/Swati/PycharmProjects/PySpark/data/warehouse"
base      = "C:/Users/Swati/PycharmProjects/PySpark/data"

spark = SparkSession.builder \
    .appName("TablesHandsOn") \
    .master("local[*]") \
    .config("spark.sql.warehouse.dir", warehouse) \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ── Sample Data ──
students = [
    (1, "Swati",  90),
    (2, "Rahul",  75),
    (3, "Anjali", 85),
    (4, "Vikram", 60),
    (5, "Priya",  95)
]
df = spark.createDataFrame(
    students, ["id", "name", "marks"])

print("=" * 45)
print("   TABLES HANDS ON")
print("=" * 45)
print("\nOur Data:")
df.show()

# ══════════════════════════════════════
# TASK 1 — Create Managed Table
# ══════════════════════════════════════
print("\n" + "=" * 45)
print("TASK 1 — CREATE MANAGED TABLE")
print("=" * 45)

spark.sql("DROP TABLE IF EXISTS students")
df.write.mode("overwrite").saveAsTable("students")
print("students table created ")

# ══════════════════════════════════════
# TASK 2 — Basic SQL on table
# ══════════════════════════════════════
print("\n" + "=" * 45)
print("TASK 2 — SQL ON TABLE")
print("=" * 45)

print("\nSELECT all:")
spark.sql("SELECT * FROM students").show()

print("SELECT where marks > 80:")
spark.sql("""
    SELECT * FROM students
    WHERE marks > 80
""").show()

print("ORDER BY marks DESC:")
spark.sql("""
    SELECT * FROM students
    ORDER BY marks DESC
""").show()

print("COUNT total students:")
spark.sql("""
    SELECT COUNT(*) as total
    FROM students
""").show()

print("AVG marks:")
spark.sql("""
    SELECT AVG(marks) as average
    FROM students
""").show()

# ══════════════════════════════════════
# TASK 3 — Insert new student
# ══════════════════════════════════════
print("\n" + "=" * 45)
print("TASK 3 — INSERT NEW STUDENT")
print("=" * 45)

print("Before insert:")
spark.sql("SELECT * FROM students").show()

spark.sql("""
    INSERT INTO students
    VALUES (6, 'Arjun', 88)
""")
print("After insert:")
spark.sql("""
    SELECT * FROM students
    ORDER BY id
""").show()

# ══════════════════════════════════════
# TASK 4 — Create new table from query
# ══════════════════════════════════════
print("\n" + "=" * 45)
print("TASK 4 — CREATE TABLE FROM QUERY")
print("=" * 45)

spark.sql("DROP TABLE IF EXISTS top_students")
spark.sql("""
    CREATE TABLE top_students
    USING parquet
    AS
    SELECT * FROM students
    WHERE marks >= 85
""")
print("top_students table created ")
spark.sql("SELECT * FROM top_students").show()

# ══════════════════════════════════════
# TASK 5 — Show all tables
# ══════════════════════════════════════
print("\n" + "=" * 45)
print("TASK 5 — SHOW ALL TABLES")
print("=" * 45)
spark.sql("SHOW TABLES").show()

# ══════════════════════════════════════
# TASK 6 — Describe table
# ══════════════════════════════════════
print("\n" + "=" * 45)
print("TASK 6 — DESCRIBE TABLE")
print("=" * 45)

print("Structure of students table:")
spark.sql("DESCRIBE students").show()

# ══════════════════════════════════════
# TASK 7 — External Table
# ══════════════════════════════════════
print("\n" + "=" * 45)
print("TASK 7 — EXTERNAL TABLE")
print("=" * 45)

my_path = f"{base}/my_students"
df.coalesce(1) \
    .write \
    .mode("overwrite") \
    .parquet(my_path)
print(f"Data saved to: {my_path} ")

spark.sql("DROP TABLE IF EXISTS students_external")
spark.sql(f"""
    CREATE EXTERNAL TABLE students_external (
        id    INT,
        name  STRING,
        marks INT
    )
    USING parquet
    LOCATION '{my_path}'
""")
print("External table created ")

print("Query external table:")
spark.sql("SELECT * FROM students_external").show()

# ══════════════════════════════════════
# TASK 8 — Drop and check
# ══════════════════════════════════════
print("\n" + "=" * 45)
print("TASK 8 — DROP AND CHECK")
print("=" * 45)

spark.sql("DROP TABLE IF EXISTS students_external")
print("External table dropped ")

count = spark.read.parquet(my_path).count()
print(f"Data still exists: {count} rows ")
print("External table data is SAFE ")

spark.sql("DROP TABLE IF EXISTS students")
spark.sql("DROP TABLE IF EXISTS top_students")
print("Managed tables dropped ")
print("Managed table data DELETED ⚠️")

print("\nTables after dropping all:")
spark.sql("SHOW TABLES").show()

print("\nTables Hands On Done!")
spark.stop()