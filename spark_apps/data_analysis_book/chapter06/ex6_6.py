import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Ex6_6").getOrCreate()

data_dir = "/opt/spark/data"

three_shows = spark.read.json(f"{data_dir}/shows/shows-*.json", multiLine=True)

print(three_shows.count())

assert three_shows.count() == 3

three_shows.printSchema()

# From book solution:
# sol6_6 = three_shows.select(
#     "name",
#     F.array_min("_embedded.episodes.airdate").cast("date").alias("first"),
#     F.array_max("_embedded.episodes.airdate").cast("date").alias("last"), ).select("name", (
#             F.col("last") - F.col("first")).alias("tenure"))
#
# sol6_6.show(truncate=50)

data = three_shows.select(
    "id",
    "name",
    F.to_timestamp(F.col("_embedded.episodes.airstamp").getItem(0)).alias(
        "first_ep_airstamp"
    ),
    F.to_timestamp(F.element_at(F.col("_embedded.episodes.airstamp"), -1)).alias(
        "last_ep_airstamp"
    ),
)

data = data.select(
    "id",
    "name",
    (
        F.unix_timestamp("last_ep_airstamp") - F.unix_timestamp("first_ep_airstamp")
    ).alias("duration_seconds"),
).orderBy("duration_seconds", ascending=False)

data.show(5)

data.printSchema()
