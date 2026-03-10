from pyspark.sql import SparkSession
from pyspark.sql.functions import avg
from src.dataframes.df_creation.common_df import create_sales_df
spark = SparkSession.builder.appName("avg").getOrCreate()
df = create_sales_df(spark)
df.select(avg("price")).show()