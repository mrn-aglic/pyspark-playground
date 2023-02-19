import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName(
    "Ch03 - Analyzing the vocabulary of Pride and Prejudice."
).getOrCreate()

book = spark.read.text("/opt/spark/data/pride-and-prejudice.txt")

lines = book.select(F.split(F.col("value"), " ").alias("line"))

words = lines.select(F.explode(F.col("line")).alias("word"))

words_lower = words.select(F.lower(F.col("word")).alias("word_lower"))
words_clean = words_lower.select(
    F.regexp_extract(F.col("word_lower"), "[a-z]*", 0).alias("word")
)
words_nonull = words_clean.where(F.col("word") != "")

results = words_nonull.groupby(F.col("word")).count()

results.orderBy(F.col("count").desc()).show(10)

results.coalesce(1).write.csv("/opt/spark/data/results/chapter03/simple_count.csv")
