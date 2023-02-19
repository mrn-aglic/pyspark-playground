from functools import reduce

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName(
    "Ch07 - Backblaze data - Blending SQL and Python"
).getOrCreate()

# spark.sparkContext.setLogLevel("WARN")

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

drive_days = full_data.groupby("model", "capacity_GB").agg(
    F.count("*").alias("drive_days")
)

failures = (
    full_data.where("failure = 1")
    .groupby("model", "capacity_GB")
    .agg(F.count("*").alias("failures"))
)

# Alternative
# failures = (
#     full_data.where("failure = 1")
#     .groupby("model", "capacity_GB")
#     .agg(F.expr("count(*) failures"))
# )

# failures.show(5)

summarized_data = (
    drive_days.join(failures, on=["model", "capacity_GB"], how="left")
    .fillna(0.0, "failures")
    .selectExpr("model", "capacity_GB", "(failures / drive_days) AS failure_rate")
    .cache()
)

# pprint("Summarized data:")
# summarized_data.show(5)


def most_reliable_drive_for_capacity(data, capacity_GB=2048.0, precision=0.25, top_n=3):
    """Return the top 3 drive for a given approximate capacity.

    Given a capacity in GB and a precision as a decimal number, we keep the N
    drives where:
    - the capacity is between (capacity * 1 / (1 + precision)), capacity * (1 + precision)
    - the failure rate is the lowest

    """
    capacity_min = capacity_GB / (1 + precision)
    capacity_max = capacity_GB * (1 + precision)

    answer = (
        data.where(f"capacity_GB between {capacity_min} and {capacity_max}")
        .orderBy("failure_rate", "capacity_GB", ascending=[True, False])
        .limit(top_n)
    )

    return answer


most_reliable_drive_for_capacity(summarized_data, capacity_GB=11176.0).show()
