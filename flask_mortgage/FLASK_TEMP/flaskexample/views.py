""" This file largely follows the steps outlined in the Insight Flask tutorial, except data is stored in a
flat csv (./assets/births2012_downsampled.csv) vs. a postgres database. If you have a large database, or
want to build experience working with SQL databases, you should refer to the Flask tutorial for instructions on how to
query a SQL database from here instead.

May 2019, Donald Lee-Brown
"""

from flask import render_template
from flaskexample import app
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
print("In load_model:")
loadtime = time.time()
rf_pipeline = PipelineModel.load("/home/ubuntu/pipeline")
rf_model = RandomForestClassificationModel.load("/home/ubuntu/model_rf")
print("after load_model: %s seconds" % (time.time() - loadtime))

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
    df = change_type(df)
    df_encode = rf_pipeline.transform(df)
    rf_prediction = rf_model.transform(df_encode)
    prediction = rf_prediction.collect()[0]['prediction']
    return prediction


@app.route('/')
@app.route('/mortgage')
def input_page():
      #create lists for categories
   Number_of_Borrowers=['2','1']
   Property_State = ["AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DC", "DE", "GA",
                     "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
                     "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
                     "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
                     "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "FL"]
   Property_Type = ['CO','PU','MH','CP','SF']
   Occupancy_Status = ['I','S','P']
   Number_of_Units=['2','1']
   First_Buyer=['N','Y']
   return render_template("model_input.html",number_of_borrowers=Number_of_Borrowers,property_state=Property_State,property_type=Property_Type,occupancy_status=Occupancy_Status,first_buyer=First_Buyer,number_of_units=Number_of_Units)

@app.route('/user_guide')
def user_guide_page():
    return render_template("user_guide.html")

@app.route('/model_output')
def prediction_page():
   #pass value from model_input
   first_name = request.args.get('First_Name')
   last_name = request.args.get('Last_Name')
   credit_score = request.args.get('Credit_Score')
   interest_rate = request.args.get('Interest_Rate')
   ltv = request.args.get('ltv')
   dti = request.args.get('dti')
   upb = request.args.get('upb')
   property_state = request.args.get('Property_State')
   number_of_units = request.args.get('Number_of_Units')
   number_of_borrowers = request.args.get('Number_of_Borrowers')
   property_type = request.args.get('Property_Type')
   occupancy_status = request.args.get('Occupancy_Status')
   first_buyer = request.args.get('First_Buyer')
   mip = request.args.get('mip')
   channel = "R"
   loan_purpose = "P"
   #create dataframe
   col_name = ['first_name','last_name','loan_purpose','channel','original_ltv','occupancy_status','original_dti','original_upb','number_of_borrowers','first_time_homebuyer_flag','mip','property_state','number_of_units','original_interest_rate','credit_score','property_type']
   df = spark.createDataFrame([(first_name,last_name,loan_purpose,channel,ltv,occupancy_status,dti,upb,number_of_borrowers,first_buyer,mip,property_state,number_of_units,interest_rate,credit_score,property_type)],col_name)
   df_type = change_type(df)
   raw_prediction = ModelIt(df_type)
   dic = {0.0:'NO DEFAULT',1.0:'DEFAULT'}
   prediction = dic[raw_prediction]
   return render_template("model_output.html",result = prediction,note = "Note: Default means there is more than 90 days delinquency",first_name=first_name,last_name=last_name,credit_score=credit_score,interest_rate=interest_rate,ltv=ltv,dti=dti,upb=upb,property_state=property_state,number_of_units=number_of_units,number_of_borrowers=number_of_borrowers,property_type=property_type,occupancy_status=occupancy_status,first_buyer=first_buyer, mip=mip)
#
