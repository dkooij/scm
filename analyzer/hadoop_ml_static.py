"""
Train ML models to predict page text changes using static features.
Author: Daan Kooij
Last modified: January 12th, 2021
"""

from pyspark import SparkContext
from pyspark.ml.classification import DecisionTreeClassifier, LinearSVC, LogisticRegression, NaiveBayes, RandomForestClassifier
from pyspark.ml.linalg import DenseVector
from pyspark.mllib.evaluation import MulticlassMetrics
from pyspark.sql import Row, SparkSession


INPUT_PATH = "extracted/static-training-pairs-combined-sample.csv"


# Initialize Spark and SparkSQL context.
sc = SparkContext(appName="SCM-EXTRACT-STATIC-TRAINING-PAIRS")
sc.setLogLevel("ERROR")
spark = SparkSession.builder.getOrCreate()


df = spark.read.csv(INPUT_PATH)
df = df.rdd.map(lambda row: Row(**{"features": DenseVector([int(x) for x in row[:9]]),
                                   "target": int(row[9])})).toDF()
count_zero, count_one = df.filter(df.target == 0).count(), df.filter(df.target == 1).count()
weight_zero, weight_one = count_one / count_zero, 1.0
df = df.rdd.map(lambda row: Row(**{"features": row["features"],
                                   "target": row["target"],
                                   "weight": weight_zero if row["target"] == 0 else weight_one})).toDF()
(data_train, data_test) = df.randomSplit([0.8, 0.2], seed=42)


model_type = "lr"
if model_type == "lr":
    blank_model = LogisticRegression(labelCol="target", featuresCol="features", weightCol="weight")
elif model_type == "svm":
    blank_model = LinearSVC(labelCol="target", featuresCol="features", weightCol="weight")
elif model_type == "nb":
    blank_model = NaiveBayes(labelCol="target", featuresCol="features", weightCol="weight")
elif model_type == "dt":
    # TODO: weights
    blank_model = DecisionTreeClassifier(labelCol="target", featuresCol="features")
else:  # model_type == "rf"
    # TODO: weights
    blank_model = RandomForestClassifier(labelCol="target", featuresCol="features")

model = blank_model.fit(data_train)
predictions = model.transform(data_test)


predictions_rdd = predictions.select("target", "prediction").rdd.map(
    lambda row: (float(row["target"]), float(row["prediction"])))

metrics = MulticlassMetrics(predictions_rdd)
confusion_matrix = metrics.confusionMatrix()
print(confusion_matrix)
