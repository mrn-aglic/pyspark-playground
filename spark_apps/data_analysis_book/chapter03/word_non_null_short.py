import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName(
    "Analyzing the vocabulary of Pride and Prejudice."
).getOrCreate()

book = spark.read.text("/opt/spark/data/pride-and-prejudice.txt")

results = (
    book.select(F.split(F.col("value"), " ").alias("line"))
    .select(F.explode(F.col("line")).alias("word"))
    .select(F.lower(F.col("word")).alias("word_lower"))
    .select(F.regexp_extract(F.col("word_lower"), "[a-z]*", 0).alias("word"))
    .where(F.col("word") != "")
    .groupby(F.col("word"))
    .count()
)

results.orderBy(F.col("word").desc()).show(10)

results.coalesce(1).write.csv("/opt/spark/data/results/chapter03/simple_count.csv")
