import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType
)

spark = SparkSession.builder.appName("PysparkOnKubernetes").getOrCreate()

schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("value", IntegerType(), True)
])

# Generate random data
data = [(i, i * 2) for i in range(10)]

# Create DataFrame from random data
df = spark.createDataFrame(data, schema)
df = df.coalesce(1)
df.show()
