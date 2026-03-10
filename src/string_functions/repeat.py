from pyspark.sql import SparkSession
from pyspark.sql.functions import repeat
from src.dataframes.df_creation.common_df import create_sales_df
spark = SparkSession.builder.appName("repeat").getOrCreate()
df = create_sales_df(spark)
df.select(repeat("product",2)).show()