from functools import reduce

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName(
    "Ex7_2n"
).getOrCreate()

data_dir = "/opt/spark/data/backblaze_data"

DATA_FILES = ["data_Q3_2019"]

data = [
    spark.read.csv(f"{data_dir}/{file_dir}", header=True, inferSchema=True)
    for file_dir in DATA_FILES
]

common_columns = list(
    reduce(
        lambda acc, element: acc.intersection(element), [set(df.columns) for df in data]
    )
)

assert {"model", "capacity_bytes", "date", "failure"}.issubset(set(common_columns))

full_data = reduce(
    lambda acc, df: acc.select(common_columns).union(df.select(common_columns)), data
)

# full_data.printSchema()

# Methods that accept SQL-type statements:
# selectExpr, epxr, where/filter
# selectExpr() is just like the select() method with the exception that it will pro- cess SQL-style operations.

full_data = full_data.selectExpr(
    "model", "(capacity_bytes / pow(1024, 3)) as capacity_GB", "date", "failure"
)

# Alternative:
# full_data = full_data.select(
#     F.col("model"),
#     (F.col("capacity_bytes") / F.pow(F.lit(1024), 3)).alias("capacity_GB"),
#     F.col("date"),
#     F.col("failure")
# )


summarized_data = (
    full_data.groupBy("model", "capacity_GB")
    .agg(
        F.count("*").alias("drive_days"),
        F.sum(F.col("failure")).alias("failures"),
    )
    .selectExpr("model", "capacity_GB", "(failures / drive_days) AS failure_rate")
)

summarized_data.show(5)
