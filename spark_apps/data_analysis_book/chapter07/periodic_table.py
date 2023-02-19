import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Ch07 - Perdiodic table - PySpark vs SQL").getOrCreate()

data_dir = "/opt/spark/data"

elements = spark.read.csv(
    f"{data_dir}/elements/Periodic_Table_Of_Elements.csv", header=True, inferSchema=True
)

elements.where(F.col("phase") == "liq").groupby("period").count().show()

# SQL:
# SELECT period, COUNT(*)
# FROM elements
# WHERE phase = 'liq'
# GROUP BY period
