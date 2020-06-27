# Databricks notebook source
# MAGIC %md ##Train PySpark Random Forest Model & Gradient-boosted trees (GBTs)
# MAGIC 
# MAGIC The notebook contains the following sections:
# MAGIC 
# MAGIC #### Train a PySpark Pipeline model
# MAGIC * Load training data
# MAGIC * Define the PySpark data structure
# MAGIC * Train the model: Random Forest Model and Gradient-boosted trees 

# COMMAND ----------

# MAGIC %md ### Load pipeline training data
# MAGIC 
# MAGIC Load data to train the PySpark Pipeline model. 
# MAGIC This model uses the [Kaggle Credit Card Fraud Data Set](https://www.kaggle.com/mlg-ulb/creditcardfraud) 

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/creditcard.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)
df.cache()
display(df)

# COMMAND ----------

# MAGIC %md ### Define the PySpark data structure
# MAGIC 
# MAGIC Define a PySpark dataset and classifies them compare the models

# COMMAND ----------

from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint
#  Random Forest model settings

SEED = 1356

# Labeled Point - label is last column in the data set
# Features the all other colunms without label column  
transformed_df = df.rdd.map(lambda row: LabeledPoint(row[-1], Vectors.dense(row[0:-1])))

# Split data on training and test 
splits = [0.7, 0.3]
training_data, test_data = transformed_df.randomSplit(splits, SEED)

print("Total number of rows: %d" % df.count())
print("Number of training set rows: %d" % training_data.count())
print("Number of test set rows: %d" % test_data.count())

# COMMAND ----------

# MAGIC %md ### Train Gradient Boosted trees and Random Forest model

# COMMAND ----------

from pyspark.mllib.tree import RandomForest
from time import *
from pyspark.mllib.tree import GradientBoostedTrees

start_time = time()

# Train a model Gradient Boosted Trees
modelGBT = GradientBoostedTrees.trainClassifier(training_data, categoricalFeaturesInfo={})

end_time = time()
elapsed_time_GBT = end_time - start_time
print("Time to train GBT model: %.3f seconds" % elapsed_time_GBT)

# Train a model Random Forest 
start_time = time()
model = RandomForest.trainClassifier(training_data, numClasses=2, categoricalFeaturesInfo={}, \
   numTrees=3, featureSubsetStrategy="auto", impurity="gini", \
   maxDepth=4, maxBins=32, seed=SEED)

end_time = time()
elapsed_time_RF = end_time - start_time
print("Time to train Random Forest model: %.3f seconds" % elapsed_time_RF)

# COMMAND ----------

# MAGIC %md ### Prediction using test data set

# COMMAND ----------

# Make prediction using training data Random Forest
predictions_train = model.predict(training_data.map(lambda x: x.features))
labels_and_predictions_train = training_data.map(lambda x: x.label).zip(predictions_train)
acc_train = labels_and_predictions_train.filter(lambda x: x[0] == x[1]).count() / float(training_data.count())
print("Random ForestModel training accuracy: %.3f%%" % (acc_train * 100))

# Make prediction using test data Random Forest

predictions = model.predict(test_data.map(lambda x: x.features))
labels_and_predictions = test_data.map(lambda x: x.label).zip(predictions)
acc = labels_and_predictions.filter(lambda x: x[0] == x[1]).count() / float(test_data.count())
print("Random Forest Model test accuracy: %.3f%%" % (acc * 100))

# Make prediction using training data GBT
predictions_train_GBT = modelGBT.predict(training_data.map(lambda x: x.features))
labels_and_predictions_train_GBT = training_data.map(lambda x: x.label).zip(predictions_train_GBT)
acc_train_GBT = labels_and_predictions_train_GBT.filter(lambda x: x[0] == x[1]).count() / float(training_data.count())
print("GBT Model training accuracy: %.3f%%" % (acc_train_GBT * 100))

# Make prediction using test data GBT

predictions_GBT = modelGBT.predict(test_data.map(lambda x: x.features))
labels_and_predictions_GBT = test_data.map(lambda x: x.label).zip(predictions_GBT)
acc_GBT = labels_and_predictions_GBT.filter(lambda x: x[0] == x[1]).count() / float(test_data.count())
print("GBT Model test accuracy: %.3f%%" % (acc_GBT * 100))

# COMMAND ----------

# compute Precision/Recall (PR) and Receiver Operating Characteristic (ROC)

from pyspark.mllib.evaluation import BinaryClassificationMetrics

start_time = time()

metrics = BinaryClassificationMetrics(labels_and_predictions)
print("Random Forest Area under Precision/Recall (PR) curve: %.f" % (metrics.areaUnderPR * 100))
print("Random Forest Area under Receiver Operating Characteristic (ROC) curve: %.3f" % (metrics.areaUnderROC * 100))

end_time = time()
elapsed_time_RF_M = end_time - start_time
print("Random Forest Time to evaluate model: %.3f seconds" % elapsed_time_RF_M)

start_time = time()

metrics_GBT = BinaryClassificationMetrics(labels_and_predictions_GBT)
print("GBT Area under Precision/Recall (PR) curve: %.f" % (metrics_GBT.areaUnderPR * 100))
print("GBT Area under Receiver Operating Characteristic (ROC) curve: %.3f" % (metrics_GBT.areaUnderROC * 100))

end_time = time()
elapsed_time_GBT_M = end_time - start_time
print("GBT Time to evaluate model: %.3f seconds" % elapsed_time_GBT_M)

# COMMAND ----------

# MAGIC %md ### Table of Comparison: Random Forest and Gradient Boosted Tree 

# COMMAND ----------

print("Table of Comparison: Random Forest (RTF) and Gradient Boosted Tree (BST)")

print(" ----------------------------------------------------------------")
print(" |  RFT     |   GBT    | Description \t\t\t\t|")
print(" ----------------------------------------------------------------")

print(" | %.3f%%" % (acc_train * 100), " | %.3f%%" % (acc_train_GBT * 100), " | training accuracy \t\t\t|")
print(" | %.3f%%" % (acc * 100), " | %.3f%%" % (acc_GBT * 100), " | test accuracy \t\t\t\t|")

print(" | %.3f" % (metrics.areaUnderPR * 100),"  | %.3f" % (metrics_GBT.areaUnderPR * 100) , "  | Precision/Recall \t\t\t|")
print(" | %.3f" % (metrics.areaUnderROC * 100), "  | %.3f" % (metrics_GBT.areaUnderROC * 100) , "  | Receiver Operating Characteristic (ROC)|")

print(" | %.3f s" % elapsed_time_RF, " | %.3f s" %  elapsed_time_GBT, "| time to train models \t\t\t|")
print(" | %.3f s" % elapsed_time_RF_M, " | %.3f s" %  elapsed_time_GBT_M, " | time to evaluate models \t\t|")
print(" ----------------------------------------------------------------")

# COMMAND ----------


