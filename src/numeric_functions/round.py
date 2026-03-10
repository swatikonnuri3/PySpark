from pyspark.sql import SparkSession
from pyspark.sql.functions import round
from src.dataframes.df_creation.common_df import create_sales_df
spark = SparkSession.builder.appName("round").getOrCreate()
df = create_sales_df(spark)
df.select(round("price",2)).show()