""" This file largely follows the steps outlined in the Insight Flask tutorial, except data is stored in a
flat csv (./assets/births2012_downsampled.csv) vs. a postgres database. If you have a large database, or
want to build experience working with SQL databases, you should refer to the Flask tutorial for instructions on how to
query a SQL database from here instead.

May 2019, Donald Lee-Brown
"""

from flask import render_template
from flaskexample import app
#from flaskexample.a_model import ModelIt
from flask import request
import pyspark
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from pyspark.sql.functions import lit
from pyspark.sql.functions import udf
from pyspark.sql.types import *
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.ml import PipelineModel
import time
#Load in onehotencoder and rf model
spark = SparkSession.builder.getOrCreate()
rf_pipeline = PipelineModel.load("s3a://ffinsight/pipeline")
rf_model = RandomForestClassificationModel.load("s3a://ffinsight/model_rf")

def change_type(df):
    df = df.withColumn('credit_score',col('credit_score').cast(IntegerType()))
    df = df.withColumn('original_dti',col('original_dti').cast(IntegerType()))
    df = df.withColumn('original_upb',col('original_upb').cast(IntegerType()))
    df = df.withColumn('original_ltv',col('original_ltv').cast(IntegerType()))
    df = df.withColumn('original_interest_rate',col('original_interest_rate').cast(DoubleType()))
    df = df.withColumn('number_of_units',col('number_of_units').cast(IntegerType()))
    df = df.withColumn('mip',col('mip').cast(IntegerType()))
    return df

def ModelIt(df):
   # print("In modelIt:")
   # modelItStime = time.time()
    df = change_type(df)
   # print("In modelIt, after calling change_type(): %s seconds" % (time.time() - modelItStime))
    df_encode = rf_pipeline.transform(df)
    rf_prediction = rf_model.transform(df_encode)
    prediction = rf_prediction.collect()[0]['prediction']
    return prediction

def create_figure(current_feature_name, bins):
    p = Histogram(iris_df, current_feature_name, title=current_feature_name, color='Species', bins=bins, legend='top_right', width=600, height=400)
	# Set the x axis label
    p.xaxis.axis_label = current_feature_name

	# Set the y axis label
    p.yaxis.axis_label = 'Default Rate'
    return p

@app.route('/')
@app.route('/mortgage')
def prediction_page():
   return render_template("model_input.html")

@app.route('/model_output')
def prediction_output():
    startTime = time.time()
    fn = request.args.get('First_Name')
    ln = request.args.get('Last_Name')
    ssn =  request.args.get('SSN')
    url ='s3a://ffinsight/flask.csv'
    test = spark.read.format('csv').option('header',True).load(url)
    test = test.filter(col('ssn')==ssn)
    raw_predict = ModelIt(test)
    dic = {0.0:"NO DEFAULT",1.0:'DEFAULT'}
    prediction = dic[raw_predict]
    
    dic = test.toPandas().to_dict(orient='list')
    #credit_score = test.collect()[0]['credit_score']
    #interest_rate = test.collect()[0]['original_interest_rate']
    #ltv = test.collect()[0]['original_ltv']
    print(type(dic))
    return render_template("model_output.html",dic = dic,result=prediction, note = "Note: Default means there is more than 90 days delinquency")
