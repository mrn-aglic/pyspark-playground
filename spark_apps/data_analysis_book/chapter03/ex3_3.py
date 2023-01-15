import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.appName(
    "Analyzing the vocabulary of Pride and Prejudice. Exercise 3.3"
).getOrCreate()


def get_distinct_words(filename):
    book = spark.read.text(filename)

    results = (
        book.select(F.split(F.col("value"), " ").alias("line"))
        .select(F.explode(F.col("line")).alias("word"))
        .select(F.lower(F.col("word")).alias("word_lower"))
        .select(F.regexp_extract(F.col("word_lower"), "[a-z]*", 0).alias("word"))
        .where(F.col("word") != "")
        .distinct()
    )

    return spark.createDataFrame([results.count()], IntegerType()).toDF(
        "distinct words"
    )


result = get_distinct_words("/opt/spark/data/gutenberg_books/*.txt")

result.show()

result.coalesce(1).write.mode("overwrite").csv(
    "/opt/spark/data/results/chapter03/simple_count.csv"
)
