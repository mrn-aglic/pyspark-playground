import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Ch07 - Backblaze data - subquery and cte").getOrCreate()

data_dir = "/opt/spark/data"

# only the minimal example from the book included
q3 = spark.read.csv(
    f"{data_dir}/backblaze_data/data_Q3_2019", header=True, inferSchema=True
)

backblaze_2019 = q3

backblaze_2019 = backblaze_2019.select(
    [
        F.col(x).cast(T.LongType()) if x.startswith("smart") else F.col(x)
        for x in backblaze_2019.columns
    ]
)

backblaze_2019.createOrReplaceTempView("drive_stats")

spark.sql(
    """
SELECT
    failures.model,
    failures / drive_days failure_rate
FROM (
    SELECT
        model,
        count(*) as drive_days
    FROM drive_stats
    GROUP BY model
) AS drive_days
INNER JOIN (
    SELECT
        model,
        count(*) as failures
    FROM drive_stats
    WHERE failure = 1
    GROUP BY model
) AS failures
ON drive_days.model = failures.model
ORDER BY 2 DESC
"""
).show(5)

spark.sql(
    """
WITH drive_days AS (
    SELECT
        model,
        count(*) AS drive_days
    FROM drive_stats
    GROUP BY model
),
failures AS (
    SELECT
        model,
        count(*) AS failures
    FROM drive_stats
    WHERE failure = 1
    GROUP BY model
)
SELECT
    failures.model,
    (failures / drive_days) failure_rate
FROM drive_days
INNER JOIN failures
ON drive_days.model = failures.model
ORDER BY 2 DESC
"""
).show(5)


def failure_rate(drive_stats):
    drive_days = drive_stats.groupby(F.col("model")).agg(
        F.count(F.col("*")).alias("drive_days")
    )

    failures = (
        drive_stats.where(F.col("failure") == 1)
        .groupby(F.col("model"))
        .agg(F.count(F.col("*")).alias("failures"))
    )

    answer = (
        drive_days.join(failures, on="model", how="inner")
        .withColumn("failure_rate", F.col("failures") / F.col("drive_days"))
        .orderBy(F.col("failure_rate").desc())
    )

    return answer


failure_rate(backblaze_2019).show(5)

# We are testing if we have a variable drive_days in scope
# once the function returned confirms that our intermediate
# frames are neatly confined inside the function scope.
print("drive_days" in dir())
