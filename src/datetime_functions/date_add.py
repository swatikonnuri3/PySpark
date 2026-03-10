from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from src.dataframes.df_creation.common_df import create_sales_df
spark = SparkSession.builder.appName("date_add").getOrCreate()
df = create_sales_df(spark)
df = df.withColumn("sale_date", current_date())
df.select(date_add("sale_date",5)).show()