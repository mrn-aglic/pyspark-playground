import pyspark.sql.functions as F
import pyspark.sql.types as T
from py4j.protocol import Py4JJavaError
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Ch06 - defining schema").getOrCreate()

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

for column in ["airdate", "airstamp"]:
    shows_with_schema.select(f"_embedded.episodes.{column}").select(
        F.explode(column)
    ).show(5)

# Wrong schema example
episode_schema_WRONG = T.StructType(
    [
        T.StructField("_links", episode_links_schema),
        T.StructField("airdate", T.DateType()),
        T.StructField("airstamp", T.TimestampType()),
        T.StructField("airtime", T.LongType()),
        T.StructField("id", T.StringType()),
        T.StructField("image", episode_image_schema),
        T.StructField("name", T.LongType()),
        T.StructField("number", T.LongType()),
        T.StructField("runtime", T.LongType()),
        T.StructField("season", T.LongType()),
        T.StructField("summary", T.StringType()),
        T.StructField("url", T.StringType()),
    ]
)

embedded_schema_WRONG = T.StructType(
    [
        T.StructField(
            "_embedded",
            T.StructType(
                [T.StructField("episodes", T.ArrayType(episode_schema_WRONG))]
            ),
        )
    ]
)

shows_with_WRONG_schema = spark.read.json(
    f"{data_dir}/shows/shows-*.json",
    multiLine=True,
    schema=embedded_schema_WRONG,
    mode="FAILFAST",
)

try:
    shows_with_WRONG_schema.show()
except Py4JJavaError:
    pass
