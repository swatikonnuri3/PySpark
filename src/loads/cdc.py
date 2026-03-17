from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from src.dataframes.df_creation.common_df import create_sales_df
spark = SparkSession.builder.appName("cdc").getOrCreate()
old_sales = create_sales_df(spark, 50)
new_sales = create_sales_df(spark, 50)
changes = old_sales.alias("o").join(
    new_sales.alias("n"),
    col("o.sale_id") == col("n.sale_id")
).filter(col("o.price") != col("n.price"))
changes.show()