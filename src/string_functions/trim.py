from pyspark.sql import SparkSession
from pyspark.sql.functions import trim
from src.dataframes.df_creation.common_df import create_sales_df
spark = SparkSession.builder.appName("trim").getOrCreate()
df = create_sales_df(spark)
df.select(trim("product")).show()