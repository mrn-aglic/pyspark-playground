import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName(
    "Ex3_5"
).getOrCreate()


def get_distinct_words(filename):
    book = spark.read.text(filename)

    results = (
        book.select(F.split(F.col("value"), " ").alias("line"))
        .select(F.explode(F.col("line")).alias("word"))
        .select(F.lower(F.substring(F.col("word"), 1, 1)).alias("letter"))
        .where(F.col("letter").rlike("[a-z]"))
        .groupby(F.col("letter"))
        .count()
    )

    results = results.orderBy(F.col("count").desc()).limit(5)

    return results


result = get_distinct_words("/opt/spark/data/pride-and-prejudice.txt")

result.show()

result.coalesce(1).write.mode("overwrite").csv(
    "/opt/spark/data/results/chapter03/simple_count.csv"
)
