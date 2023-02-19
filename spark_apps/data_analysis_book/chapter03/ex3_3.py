import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.appName(
    "ex3_3"
).getOrCreate()


def get_distinct_words(filename):
    book = spark.read.text(filename)

    return (
        book.select(F.split(F.col("value"), " ").alias("line"))
        .select(F.explode(F.col("line")).alias("word"))
        .select(F.lower(F.col("word")).alias("word_lower"))
        .select(F.regexp_extract(F.col("word_lower"), "[a-z]*", 0).alias("word"))
        .where(F.col("word") != "")
        .distinct()
        .count()
    )


result = get_distinct_words("/opt/spark/data/gutenberg_books/*.txt")

result = spark.createDataFrame([result], IntegerType()).toDF("distinct words")
result.show()

result.coalesce(1).write.mode("overwrite").csv(
    "/opt/spark/data/results/chapter03/simple_count.csv"
)
