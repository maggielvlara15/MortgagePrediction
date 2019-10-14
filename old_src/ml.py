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
from pyspark.ml.classification import RandomForestClassifier as RF
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.classification import LinearSVC
from pyspark.ml.feature import StringIndexer, VectorIndexer, VectorAssembler, SQLTransformer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator
from pyspark.mllib.evaluation import MulticlassMetrics
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
import numpy as np
import functools
from pyspark.ml.feature import OneHotEncoder
from pyspark.ml.feature import StandardScaler
from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.ml import PipelineModel
# set up pyspark #
spark = SparkSession.builder.getOrCreate()

#read text files in S3#
#url : a string of the file path ('s3a://ffinsight/test/fannie_performance*.txt')
#real_column : real_column name

def read_files(url,real_column):
    data = spark.read.format('csv').option('header',False).load(url)
    df = data.toDF(*real_column)
    return df


def processdata_load():
    processdata_u='s3a://ffinsight/ml_upsample/*.csv'
    processdata_c= [#"loan_seq_no",\
                    "loan_purpose",\
                    "channel",\
                    "original_ltv",\
                    "occupancy_status",\
                    "original_dti",\
                    "original_upb",\
                    "number_of_borrowers",\
                    "first_time_homebuyer_flag",\
                    "originate_year",\
                    "mip",\
                    "property_state",\
                    "number_of_units",\
                    "agency_name",\
                    "original_interest_rate",\
                    "credit_score",\
                    "property_type",\
                    "default"]
    df = read_files(processdata_u, processdata_c)
    return df

def change_type(df):
    df = df.withColumn('credit_score',col('credit_score').cast(IntegerType()))
    df = df.withColumn('original_dti',col('original_dti').cast(IntegerType()))
    df = df.withColumn('original_upb',col('original_upb').cast(IntegerType()))
    df = df.withColumn('original_ltv',col('original_ltv').cast(IntegerType()))
    df = df.withColumn('original_interest_rate',col('original_interest_rate').cast(DoubleType()))
    df = df.withColumn('number_of_units',col('number_of_units').cast(IntegerType()))
    df = df.withColumn('mip',col('mip').cast(IntegerType()))
    return df


def encode(df):
    column_vec_in = ['loan_purpose','number_of_borrowers','channel','occupancy_status','first_time_homebuyer_flag','property_state','property_type']
    column_vec_out = [(x + '_catVec') for x in column_vec_in]
    indexers = [StringIndexer(inputCol=x, outputCol=x+'_tmp') for x in column_vec_in]
    encoders = [OneHotEncoder(dropLast=False, inputCol=x+"_tmp", outputCol=y) for x,y in zip(column_vec_in, column_vec_out)]
    tmp = [[i,j] for i,j in zip(indexers, encoders)]
    tmp = [i for sublist in tmp for i in sublist]
    cols_now = ['original_dti', 'credit_score', 'original_upb', 'mip','number_of_units','original_ltv', 'original_interest_rate']+column_vec_out
    assembler_features = VectorAssembler(inputCols=cols_now, outputCol='features')
    labelIndexer = StringIndexer(inputCol='default', outputCol="label")
    tmp += [assembler_features, labelIndexer]
    pipeline = Pipeline(stages=tmp)
    pl = pipeline.fit(df)
    allData = pl.transform(df)
    pl.save("s3a://ffinsight/pipeline")
    return allData

def rf(df):
    trainingData, testData = df.randomSplit([0.7,0.3], seed=0)
    rf = RF(labelCol='label', featuresCol='features',numTrees=100)
    fit = rf.fit(trainingData)
   # featureImp = fit.featureImportances
    fit.save("s3a://ffinsight/model_rf")
    prediction = fit.transform(testData)
    return prediction

def evaluate(df_prediction):
    evaluator = BinaryClassificationEvaluator()
    roc = evaluator.evaluate(df_prediction, {evaluator.metricName: "areaUnderROC"})
    pr = evaluator.evaluate(df_prediction, {evaluator.metricName: "areaUnderPR"})
    predictionRDD = df_prediction.select(['label', 'prediction']).rdd.map(lambda line: (line[1], line[0]))
    metrics = MulticlassMetrics(predictionRDD)
    f1 = metrics.fMeasure()
    return [roc,pr,f1]  

def main():
    df = processdata_load()
    df = df.drop(*['agency_name','originate_year'])
    df_type = change_type(df)
    df_drop = df_type.na.drop()
#    test = df_drop.limit(10)
    alldata = encode(df_drop)
#    rf_pipeline = PipelineModel.load("s3a://ffinsight/pipeline")
#    rf_model = RandomForestClassificationModel.load("s3a://ffinsight/model_rfc")
    rf_prediction = rf(alldata) 
#    lr_prediction = lr(df)
    rf_eva = evaluate(rf_prediction)
#    lr_eva = evaluate(lr_prediction)
#    print("rf[roc,pr,f1]:")
#    print(rf_eva)
    print(rf_prediction.show(10))
#    print("lr[roc.pr,f1]:")
#    print(lr_eva)

main()

