# benchmark 3 - data exploration
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum
import time
import psutil

spark = SparkSession.builder \
    .appName("Google Colab - PySpark") \
    .config("spark.driver.memory", "12g") \
    .getOrCreate()

# benchmark starts
start_time = time.time()

df = spark.read.parquet('/content/drive/MyDrive/FODM/Dataset/2019-Oct.parquet', header=True, inferSchema=True)

# descriptive statistics
df.describe()

# Find the number of unique users & products
df.select('user_id').distinct().count()
df.select('product_id').distinct().count()

# Find the most viewed product 
df.filter(df.event_type == 'view').groupBy('product_id').count().orderBy('count', ascending=False).show(1)
# Find the most purchased product
df.filter(df.event_type == 'purchase').groupBy('product_id').count().orderBy('count', ascending=False).show(1)

# Find the total revenue
df.filter(df.event_type == 'purchase').agg(_sum('price')).show()

# Count the number of events per type
df.groupBy('event_type').count().show()

# Find the most popular category
df.groupBy('category_code').count().orderBy('count', ascending=False).show(1)

spark.stop()

# Record resource utilization and throughput
elapsed_time = time.time() - start_time

print('RAM Used (GB):', psutil.virtual_memory()[3]/1000000000)
print(f"Elapsed Time (s): {elapsed_time}")