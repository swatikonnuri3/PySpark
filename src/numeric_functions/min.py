from pyspark.sql import SparkSession
from pyspark.sql.functions import min
from src.dataframes.df_creation.common_df import create_sales_df
spark = SparkSession.builder.appName("min").getOrCreate()
df = create_sales_df(spark)
df.select(min("price")).show()