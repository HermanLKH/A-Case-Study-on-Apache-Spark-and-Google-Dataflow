# benchmark 4 - data cleaning
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
import time
import psutil

spark = SparkSession.builder \
    .appName("Google Colab - PySpark") \
    .config("spark.driver.memory", "12g") \
    .getOrCreate()

# benchmark starts
start_time = time.time()

# Schema Validation
schema = StructType([
    StructField("event_time", TimestampType(), True),
    StructField("event_type", StringType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("category_id", IntegerType(), True),
    StructField("category_code", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("user_session", StringType(), True)
])

df = spark.read.parquet('/content/drive/MyDrive/FODM/Dataset/2019-Oct.parquet', header=True, inferSchema=True, schema=schema)

# Handle missing values
df = df.na.drop()

# Dropping Duplicates
df = df.dropDuplicates()

# Value Validation
df = df.filter(df['price'] >= 0)

# Produce a cleaned Parquet
df.write.parquet('/content/drive/MyDrive/FODM/Dataset/2019-Oct-cleaned.parquet')

spark.stop()

# Record resource utilization and throughput
elapsed_time = time.time() - start_time

print('RAM Used (GB):', psutil.virtual_memory()[3]/1000000000)
print(f"Elapsed Time (s): {elapsed_time}")