import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Ch07 - Backblaze data").getOrCreate()

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

view_name = "backblaze_stats_2019"
backblaze_2019.createOrReplaceTempView(view_name)

# Querying with SQL

spark.sql(f"select serial_number from {view_name} where failure = 1").show(5)

backblaze_2019.where("failure = 1").select(F.col("serial_number")).show(5)

spark.sql(
    f"""
    SELECT
        model,
        min(capacity_bytes / pow(1024, 3)) min_GB,
        max(capacity_bytes / pow(1024, 3)) max_GB
    FROM {view_name}
    GROUP BY 1
    HAVING min_GB != max_GB
    ORDER BY 3 DESC
    """
).show(5)

backblaze_2019.groupby(F.col("model")).agg(
    F.min(F.col("capacity_bytes") / F.pow(F.lit(1024), 3)).alias("min_GB"),
    F.max(F.col("capacity_bytes") / F.pow(F.lit(1024), 3)).alias("max_GB"),
).where(F.col("min_GB") != F.col("max_GB")).orderBy(
    F.col("max_GB"), ascending=False
).show(
    5
)
