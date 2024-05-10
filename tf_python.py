import sys
sys.path.append(f"/Workspace/Shared/demo-tf-git-repos/")
from tf_module import print_to_console
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

print_to_console()
# Create DataFrame from random data
df = spark.createDataFrame(data, schema)
df = df.coalesce(1)
df.show()
