from pprint import pprint

from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

# The spark catalog is a way for Spark to manage its SQL namespace
spark = SparkSession.builder.appName("Ch07 - Perdiodic table - spark catalog").getOrCreate()

data_dir = "/opt/spark/data"

elements = spark.read.csv(
    f"{data_dir}/elements/Periodic_Table_Of_Elements.csv", header=True, inferSchema=True
)

elements.createOrReplaceTempView(
    "elements"
)  # register the data frame so that we can query it with Spark SQL

try:
    spark.sql(
        "select period, count(*) from elements  where phase='liq' group by period"
    ).show(5)
except AnalysisException as e:
    print(e)

pprint(spark.catalog.listTables())

spark.catalog.dropTempView("elements")

pprint(spark.catalog.listTables())
