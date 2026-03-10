from pyspark.sql import SparkSession
from pyspark.sql.functions import instr
from src.dataframes.df_creation.common_df import create_sales_df
spark = SparkSession.builder.appName("instr").getOrCreate()
df = create_sales_df(spark)
df.select(instr("product","Lap")).show()