from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

# ex 6.1
data = """{"name": "Sample name",
    "keywords": ["PySpark", "Python", "Data"]}"""

df = spark.read.json(sc.parallelize([data]))

df.show()
df.printSchema()

# ex 6.2
data = """{"name": "Sample name",
    "keywords": ["PySpark", 3.2, "Data"]}"""

df = spark.read.json(sc.parallelize([data]))

df.show()
df.printSchema()
