from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from src.dataframes.df_creation.common_df import create_sales_df
spark = SparkSession.builder.appName("month").getOrCreate()
df = create_sales_df(spark)
df = df.withColumn("sale_date", current_date())
df.select(month("sale_date")).show()