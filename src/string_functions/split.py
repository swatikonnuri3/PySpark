from pyspark.sql import SparkSession
from pyspark.sql.functions import split
from src.dataframes.df_creation.common_df import create_sales_df
spark = SparkSession.builder.appName("split").getOrCreate()
df = create_sales_df(spark)
df.select(split("city","a")).show(truncate=False)