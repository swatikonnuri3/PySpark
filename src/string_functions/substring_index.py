from pyspark.sql import SparkSession
from pyspark.sql.functions import substring_index
from src.dataframes.df_creation.common_df import create_sales_df
spark = SparkSession.builder.appName("substring_index").getOrCreate()
df = create_sales_df(spark)
df.select(substring_index("city","a",1)).show()