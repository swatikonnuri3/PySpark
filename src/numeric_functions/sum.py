from pyspark.sql import SparkSession
from pyspark.sql.functions import sum
from src.dataframes.df_creation.common_df import create_sales_df
spark = SparkSession.builder.appName("sum").getOrCreate()
df = create_sales_df(spark)
df.select(sum("price")).show()