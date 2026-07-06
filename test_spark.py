import os
import sys

# Pastikan Spark memakai Python yang sama dengan venv yang sedang aktif,
# supaya tidak terganggu oleh Windows App Execution Alias untuk "python".
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Test").getOrCreate()
df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "value"])
df.show()
spark.stop()