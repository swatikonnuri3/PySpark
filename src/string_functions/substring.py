from pyspark.sql import SparkSession
from pyspark.sql.functions import substring
from src.dataframes.df_creation.common_df import create_sales_df
spark = SparkSession.builder.appName("substring").getOrCreate()
df = create_sales_df(spark)
df.select(substring("product",1,3)).show()