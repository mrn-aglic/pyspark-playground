import pyspark.sql.types as T
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Ex6").getOrCreate()
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
# A StructType() will take a list of StructField(), not the types directly.
# We need to wrap T.StringType(), T.LongType(), and T.LongType() into a StructField(), giv- ing them an appropriate name.

# ex 6.4
# If we have a struct with a field that corresponds to that column name. The column will become unreachable.
# E.g. info.status -> column, info -> struct with field status

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
