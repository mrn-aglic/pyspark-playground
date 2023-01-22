import pyspark.sql.types as T
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

# ex 6.3
# missing field definition

# ex 6.4
# Probably will need to escape those characters which will make it more difficult to read what we are trying to do

# ex 6.5
dict_schema = T.StructType(
    [
        T.StructField("one", T.IntegerType()),
        T.StructField("two", T.ArrayType(T.IntegerType())),
    ]
)

df = spark.createDataFrame(
    [{"one": 1, "two": [1, 2, 3]}], schema=dict_schema, verifySchema=True
)

df.show()
df.printSchema()
