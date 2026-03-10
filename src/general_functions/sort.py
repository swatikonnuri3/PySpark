from pyspark.sql import SparkSession
from src.dataframes.df_creation.common_df import create_sales_df
spark = SparkSession.builder.appName("sort").getOrCreate()
df = create_sales_df(spark)
df.sort("price").show()
df.sort(df.price.desc()).show()