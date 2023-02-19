from pprint import pprint

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Ch06 - reading json explode collection").getOrCreate()

data_dir = "/opt/spark/data"

three_shows = spark.read.json(f"{data_dir}/shows/shows-*.json", multiLine=True)

assert three_shows.count() == 3

three_shows.printSchema()

# EXPLODE AND COLLECT START HERE
episodes = three_shows.select("id", F.explode("_embedded.episodes").alias("episodes"))

episodes.show(5, truncate=70)

pprint(episodes.count())

episode_name_id = three_shows.select(
    F.map_from_arrays(
        F.col("_embedded.episodes.id"), F.col("_embedded.episodes.name")
    ).alias("name_id")
)

episode_name_id.show(5, truncate=70)

episode_name_id = episode_name_id.select(
    F.posexplode("name_id").alias("position", "id", "name")
)

episode_name_id.show(5)

collected = episodes.groupby("id").agg(F.collect_list("episodes").alias("episodes"))

pprint(collected.count())

collected.printSchema()
