from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Ex6_7").getOrCreate()

data_dir = "/opt/spark/data"

three_shows = spark.read.json(f"{data_dir}/shows/shows-*.json", multiLine=True)

print(three_shows.count())

assert three_shows.count() == 3

three_shows.printSchema()

# I don't get this assignment
data = three_shows.select(
    "id", "name", "_embedded.episodes.name", "_embedded.episodes.airdate"
)

data.show()

data.printSchema()
