import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Ch06 - reading json").getOrCreate()

data_dir = "/opt/spark/data"

shows = spark.read.json(f"{data_dir}/shows/shows-silicon-valley.json")

print(shows.count())

assert shows.count() == 1

three_shows = spark.read.json(f"{data_dir}/shows/shows-*.json", multiLine=True)

print(three_shows.count())

assert three_shows.count() == 3

three_shows.printSchema()

# ARRAY EXAMPLES START HERE
array_subset = three_shows.select("name", "genres")

array_subset.show(3, False)

array_subset = array_subset.select(
    "name",
    array_subset.genres[0].alias("dot_and_index"),
    F.col("genres")[0].alias("col_and_index"),
    array_subset.genres.getItem(0).alias("dot_and_method"),
    F.col("genres").getItem(0).alias("col_and_method"),
)

array_subset.show()

array_subset_repeated = array_subset.select(
    "name",
    F.lit("Comedy").alias("one"),
    F.lit("Horror").alias("two"),
    F.lit("Drama").alias("three"),
    F.col("dot_and_index"),
).select(
    "name",
    F.array("one", "two", "three").alias("Some_Genres"),
    F.array_repeat("dot_and_index", 5).alias("Repeated_Genres"),
)

array_subset_repeated.show(3, False)

print("SIZE")
array_subset_repeated.select(
    "name", F.size("Some_Genres"), F.size("Repeated_Genres")
).show(3, False)

print("ARRAY_DISTINCT")
array_subset_repeated.select(
    "name", F.array_distinct("Some_Genres"), F.array_distinct("Repeated_Genres")
).show(3, False)

print("ARRAY_INTERSECT")
array_subset_repeated.select(
    "name", F.array_intersect("Some_Genres", "Repeated_Genres").alias("Genres")
).show(3, False)

# MAP EXAMPLES START HERE
columns = ["name", "language", "type"]

shows_map = three_shows.select(
    *[F.lit(column) for column in columns], F.array(*columns).alias("values")
)

shows_map.show(truncate=False)

shows_map = shows_map.select(F.array(*columns).alias("keys"), "values")

shows_map.show(truncate=False)

# Now we create the actual Map
shows_map = shows_map.select(F.map_from_arrays("keys", "values").alias("mapped"))

shows_map.printSchema()

shows_map.show(truncate=False)

shows_map.select(
    F.col("mapped.name"),
    F.col("mapped")["name"],
    shows_map.mapped["name"],
).show()
