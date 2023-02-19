import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Ex6_8").getOrCreate()

exo6_8 = spark.createDataFrame([[1, 2], [2, 4], [3, 9]], ["one", "square"])

exo6_8 = exo6_8.select(
    F.map_from_arrays(F.collect_list("one"), F.collect_list("square")).alias("my_map")
)

exo6_8.show(truncate=False)
exo6_8.printSchema()
