# benchmark 1 - load data
from pyspark.sql import SparkSession
import time
import psutil

spark = SparkSession.builder \
    .appName("Google Colab - PySpark") \
    .config("spark.driver.memory", "12g") \
    .getOrCreate()

# benchmark starts
start_time = time.time()

df = spark.read.csv("/content/drive/MyDrive/FODM/Dataset/2019-Oct.csv", header=True, inferSchema=True)

spark.stop()

# Record resource utilization and throughput
elapsed_time = time.time() - start_time

print('RAM Used (GB):', psutil.virtual_memory()[3]/1000000000)
print(f"Elapsed Time (s): {elapsed_time}")