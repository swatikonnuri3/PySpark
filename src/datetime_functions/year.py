from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from src.dataframes.df_creation.common_df import create_sales_df
spark = SparkSession.builder.appName("year").getOrCreate()
df = create_sales_df(spark)
df = df.withColumn("sale_date", current_date())
df.select(year("sale_date")).show()