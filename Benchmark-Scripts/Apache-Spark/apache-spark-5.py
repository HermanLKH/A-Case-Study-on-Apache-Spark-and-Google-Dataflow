from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.sql import SparkSession
from pyspark.ml.stat import Correlation
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
import pyspark.sql.functions as F
import time
import psutil

spark = SparkSession.builder \
    .appName("Google Colab - PySpark") \
    .config("spark.driver.memory", "12g") \
    .getOrCreate()

# benchmark starts
start_time = time.time()

df = spark.read.parquet("/content/drive/MyDrive/FODM/Dataset/2019-Oct-cleaned.parquet", header=True, inferSchema=True)

# Select relevant features for clustering
df = df.select('category_code', 'price', 'brand', 'event_type')

# Convert categorical variables into numerical form
category_indexer = StringIndexer(inputCol='category_code', outputCol='category_code_index')
brand_indexer = StringIndexer(inputCol='brand', outputCol='brand_index')

# Apply the transformations
df = category_indexer.fit(df).transform(df)
df = brand_indexer.fit(df).transform(df)

# Assemble the features into a single vector
assembler = VectorAssembler(inputCols=['category_code_index', 'brand_index', 'price'], outputCol='features')
df = assembler.transform(df)

# Convert 'event_type' into binary form (1 if 'purchase', 0 otherwise)
df = df.withColumn('label', F.when(F.col('event_type') == 'purchase', 1).otherwise(0))

# Split data into training and test sets
train_data, test_data = df.randomSplit([0.7, 0.3])

# Define and fit the model
lr = LogisticRegression(featuresCol='features', labelCol='label')
lr_model = lr.fit(train_data)

# Make predictions on the test data
predictions = lr_model.transform(test_data)
predictions.show()

# Select (prediction, true label) and compute test error
evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print("Test Accuracy = %g" % accuracy)

evaluator = MulticlassClassificationEvaluator(
    labelCol="label", predictionCol="prediction", metricName="f1")
f1 = evaluator.evaluate(predictions)
print("Test F1 Score = %g" % f1)

# For ROC and AUC
binary_evaluator = BinaryClassificationEvaluator(labelCol='label')
auc = binary_evaluator.evaluate(predictions, {binary_evaluator.metricName: "areaUnderROC"})
print("Test Area Under ROC = %g" % auc)

spark.stop()

# Record resource utilization and throughput
end_time = time.time()
elapsed_time = end_time - start_time

print('RAM Used (GB):', psutil.virtual_memory()[3]/1000000000)
print(f"Elapsed Time (s): {elapsed_time}")