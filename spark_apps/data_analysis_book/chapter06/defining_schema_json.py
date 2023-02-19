import json
import pprint

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Ch06 - defining schema json").getOrCreate()

data_dir = "/opt/spark/data"

three_shows = spark.read.json(f"{data_dir}/shows/shows-*.json", multiLine=True)

assert three_shows.count() == 3


episode_links_schema = T.StructType(
    [T.StructField("self", T.StructType([T.StructField("href", T.StringType())]))]
)

episode_image_schema = T.StructType(
    [T.StructField("medium", T.StringType()), T.StructField("original", T.StringType())]
)

episode_schema = T.StructType(
    [
        T.StructField("_links", episode_links_schema),
        T.StructField("airdate", T.DateType()),
        T.StructField("airstamp", T.TimestampType()),
        T.StructField("airtime", T.StringType()),
        T.StructField("id", T.StringType()),
        T.StructField("image", episode_image_schema),
        T.StructField("name", T.StringType()),
        T.StructField("number", T.LongType()),
        T.StructField("runtime", T.LongType()),
        T.StructField("season", T.LongType()),
        T.StructField("summary", T.StringType()),
        T.StructField("url", T.StringType()),
    ]
)

embedded_schema = T.StructType(
    [
        T.StructField(
            "_embedded",
            T.StructType([T.StructField("episodes", T.ArrayType(episode_schema))]),
        )
    ]
)

data_dir = "/opt/spark/data"

shows_with_schema = spark.read.json(
    f"{data_dir}/shows/shows-*.json",
    multiLine=True,
    schema=embedded_schema,
    mode="FAILFAST",
)

shows_with_schema.printSchema()

pprint.pprint(
    shows_with_schema.select(F.explode("_embedded.episodes").alias("episode"))
    .select("episode.airtime")
    .schema.jsonValue()
)

# pprint.pprint(T.StructField("array_example", T.ArrayType(T.StringType())).jsonValue())
#
# pprint.pprint(
#     T.StructField("map_example", T.MapType(T.StringType(), T.LongType())).jsonValue()
# )

pprint.pprint(
    T.StructType(
        [
            T.StructField("map_example", T.MapType(T.StringType(), T.LongType())),
            T.StructField("array_example", T.ArrayType(T.StringType())),
        ]
    ).jsonValue()
)

other_shows_schema = T.StructType.fromJson(json.loads(shows_with_schema.schema.json()))

assert other_shows_schema == shows_with_schema.schema
