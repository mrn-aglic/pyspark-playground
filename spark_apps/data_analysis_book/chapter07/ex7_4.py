from functools import reduce

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName(
    "Ex7_4"
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
full_data = (
    full_data.selectExpr(
        "cast(date as date) as date",
        "capacity_bytes / pow(1024, 4) as capacity_TB",
    )
    .where("extract(day from date) = 1")
    .groupby("date")
    .sum("capacity_TB")
)

full_data.orderBy("date").show()
