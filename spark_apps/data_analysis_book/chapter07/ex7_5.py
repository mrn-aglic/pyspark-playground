from functools import reduce

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName(
    "Ex7_5"
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

# Methods that accept SQL-type statements:
# selectExpr, epxr, where/filter
# selectExpr() is just like the select() method with the exception that it will pro- cess SQL-style operations.

# Group by model, capacity and failure - to get the first date that a failure is reported
# When looking at the reliability of each drive model,
# we can use drive days as a unit and count the failures versus drive days.

capacity_count = full_data.groupby("model", "capacity_bytes").agg(
    F.count("*").alias("capacity_occurrence")
)

most_common_capacity = capacity_count.groupby("model").agg(
    F.max("capacity_occurrence").alias("most_common_capacity_occurrence")
)

sol = most_common_capacity.join(
    capacity_count,
    on=(
        capacity_count["model"]
        == most_common_capacity["model"] & capacity_count["capacity_occurrence"]
        == most_common_capacity["most_common_capacity_occurrence"]
    ),
).select(most_common_capacity["model"], "capacity_bytes")

sol.show(5)

full_data = full_data.drop("capacity_bytes").join(sol, on="model")
