from pyspark.sql import SparkSession
from src.dataframes.df_creation.common_df import create_sales_df
spark = SparkSession.builder.appName("filter").getOrCreate()
df = create_sales_df(spark)
df.filter(df.price > 20000).show()