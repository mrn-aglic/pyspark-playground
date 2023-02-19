import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Ch06 - reading json struct").getOrCreate()

data_dir = "/opt/spark/data"

three_shows = spark.read.json(f"{data_dir}/shows/shows-*.json", multiLine=True)

assert three_shows.count() == 3

three_shows.printSchema()

# STRUCT STARTS HERE
struct_ex = three_shows.select(
    F.struct(F.col("status"), F.col("weight"), F.lit(True).alias("has_watched")).alias(
        "info"
    )
)

struct_ex.show(3, False)

struct_ex.printSchema()
