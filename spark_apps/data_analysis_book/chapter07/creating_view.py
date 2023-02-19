import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Ch07 - Backblaze data - creating view").getOrCreate()

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
CREATE OR REPLACE TEMP VIEW drive_days AS
    SELECT model, COUNT(*) as drive_days
    FROM drive_stats
    GROUP BY model
"""
)

spark.sql(
    """
CREATE OR REPLACE TEMP VIEW failures AS
    SELECT model, count(*) AS failures
    FROM drive_stats
    WHERE failure = 1
    GROUP BY model"""
)


drive_days = backblaze_2019.groupby(F.col("model")).agg(
    F.count(F.col("*")).alias("drive_days")
)

failures = (
    backblaze_2019.where(F.col("failure") == 1)
    .groupby(F.col("model"))
    .agg(F.count(F.col("*")).alias("failures"))
)

spark.sql(
    """
SELECT
    drive_days.model,
    drive_days,
    failures
FROM drive_days
LEFT JOIN failures ON drive_days.model = failures.model
"""
).show(5)

drive_days.join(failures, on="model", how="left").show(5)
