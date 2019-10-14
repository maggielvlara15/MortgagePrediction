import os
import re
import sys
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from pyspark.sql.functions import lit
from pyspark.sql.functions import udf
from pyspark.sql.functions import regexp_replace
from pyspark.sql.types import *
from pyspark.sql.functions import unix_timestamp
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.classification import LinearSVC
from pyspark.ml.feature import StringIndexer, VectorIndexer, VectorAssembler, SQLTransformer
from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator
from pyspark.mllib.evaluation import MulticlassMetrics
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
import numpy as np
import functools
from pyspark.ml.feature import OneHotEncoder
from pyspark.ml.feature import StandardScaler

spark = SparkSession\
        .builder\
        .getOrCreate()

def load_data():
    df = spark.read \
         .format("jdbc") \
         .option("url", "jdbc:postgresql://10.0.0.11:5432/mortgage") \
         .option("dbtable", "mldata") \
         .option("user", "postgres") \
         .option("password", "Hm636188mei!") \
         .option("driver", "org.postgresql.Driver") \
         .load()
    return df

def balance_data(df):
    ratio_adjust = 1.2
    counts = df.select('serious_delinquency').groupBy('serious_delinquency').count().collect()
    lowbound = counts[0][1]
    highbound = counts[1][1]
    threhold = ratio_adjust*lowbound/highbound
    non_default = df.filter(col('serious_delinquency')=='N')
    default = df.filter(col('serious_delinquency')=='Y')
    no_default_sample = no_default.sample(False,threhold,42)
    total = non_default_sample.union(default)
    return total

def encode(df):
    column_vec_in = ['loan_purpose','number_of_borrowers','channel','occupancy_status','first_time_homebuyer_flag','property_state','property_type','agency_name']
    column_vec_out = [(x + '_catVec') for x in column_vec_in]
    indexers = [StringIndexer(inputCol=x, outputCol=x+'_tmp') for x in column_vec_in]
    encoders = [OneHotEncoder(dropLast=False, inputCol=x+"_tmp", outputCol=y) for x,y in zip(column_vec_in, column_vec_out)]
    tmp = [[i,j] for i,j in zip(indexers, encoders)]
    tmp = [i for sublist in tmp for i in sublist]
    cols_now = ['original_dti', 'credit_score', 'original_upb', 'mip','number_of_units','original_ltv', 'original_interest_rate']+column_vec_out
    assembler_features = VectorAssembler(inputCols=cols_now, outputCol='features')
    labelIndexer = StringIndexer(inputCol='serious_delinquency', outputCol="label")
    tmp += [assembler_features, labelIndexer]
    pipeline = Pipeline(stages=tmp)
    allData = pipeline.fit(df).transform(df)
    return allData

def RF(df):
    trainingData, testData = df.randomSplit([0.7,0.3], seed=0)
    rf = RandomForestClassifier(labelCol='label', featuresCol='features')
    rfparamGrid = (ParamGridBuilder()\
                  .addGrid(rf.maxDepth, [2, 5, 10])\
                  .addGrid(rf.maxBins, [5, 10, 20])\
                  .addGrid(rf.numTrees, [5, 20, 50])\
                  .build())
    evaluator = BinaryClassificationEvaluator()
    rfcv = CrossValidator(estimator = rf,
                          estimatorParamMaps = rfparamGrid,
                          evaluator = rfevaluator,
                          numFolds = 5)
    model = rfcv.fit(trainingData)
    model.save("s3a://ffinsight/model_rf")
    prediction = model.transform(testData)
    return prediction


def SVM(df):
    trainingData, testData = df.randomSplit([0.7,0.3], seed=0)
    svm = LinearSVC(labelCol='label', featuresCol='features',maxIter=10, regParam=0.1)
    model = svm.fit(trainingData)
    model.save("s3a://ffinsight/model_svm")
    prediction = model.transform(testData)
    return prediction

def evaluate(df_prediction):
    evaluator = BinaryClassificationEvaluator()
    rc = evaluator.evaluate(df_prediction, {evaluator.metricName: "areaUnderROC"})
    pr = evaluator.evaluate(df_prediction, {evaluator.metricName: "areaUnderPR"})
    predictionRDD = df_prediction.select(['label', 'prediction']).rdd.map(lambda line: (line[1], line[0]))
    metrics = MulticlassMetrics(predictionRDD)
    f1 = metrics.fMeasure()
    return [roc,pr,f1]

def main():
    df = load_data()
    df_drop = df_filling.na.drop()
    df_balance = balance_data(df_drop)
    df_encode = encode(df_balance)
    return df_encode

df = main()
svm_prediction = SVM(df)
svm_eva = evaluate(svm_prediction)
rf_prediction = RF(df)
rf_eva = evaluate(rf_prediction)
print("rf[roc,pr,f1]:")
print(rf_eva)
print("svm[roc,pr,f1]:")
print(svm_eva)

