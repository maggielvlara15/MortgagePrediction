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

spark = SparkSession.builder.getOrCreate()

#read text files in S3#
def read_files(url,real_column):
    data = spark.read.format('csv').option('header',True).option('delimiter',"|").load(url)
    df = data.toDF(*real_column)
    return df

def fannie_performance_load():
    # fannie_pu = fannie_performance_url
    # fannie_pc = fannie_performance_column
    fannie_pu = 's3a://ffinsight/fannie/performance/Performance*.txt'
    fannie_pc=["loan_seq_no",\
	       "report_period",\
	       "servicer_name",\
	       "cur_interest_rate",\
	       "cur_actual_upb",\
	       "loan_age",\
	       "mon_to_maturity",\
	       "adjusted_mon_to_maturity",\
	       "maturity_date",\
	       "msa",\
	       "cur_delinquency",\
	       "modification",\
	       "zero_balance_code",\
	       "zero_balance_date",\
	       "last_paid_installment_date",\
	       "foreclosure_date",\
	       "disposition_date",\
	       "foreclosure_costs",\
	       "property_preservation_repair_costs",\
	       "asset_recovery_costs",\
	       "miscellaneous_expenses",\
	       "associated_taxes",\
	       "net_sale_proceeds",\
	       "credit_enhancement_proceeds",\
	       "repurchase_make_whole_proceeds",\
	       "other_foreclousure_proceeds",\
	       "non_interest_bearing_upb",\
	       "principal_forgiveness_amount",\
	       "repurchase_make_whole_proceeds_flag",\
	       "foreclousure_principle_write_off_amount",\
	       "servicing_activity_indicator"]
    df_fannie_performance = read_files(fannie_pu,fannie_pc)
    return df_fannie_performance

                  
def fannie_acquisition_load():
    fannie_au='s3a://ffinsight/fannie/aquisition/Acquisition*.txt'
    fannie_ac=["loan_seq_no",\
               "channel",\
               "seller_name",\
               "original_interest_rate",\
               "original_upb",\
               "original_loan_term",\
               "origination_date",\
               "first_payment_date",\
               "original_ltv",\
               "original_cltv",\
               "number_of_borrowers",\
               "original_dti",\
               "credit_score",\
               "first_time_homebuyer_flag",\
               "loan_purpose",\
               "property_type",\
               "number_of_units",\
               "occupancy_status",\
               "property_state",\
               "postal_code",\
               "mip",\
               "product_type",\
               "co_borrower_credit_score",\
               "mortgage_insurance_type",\
               "relocation_mortgage_indicator"]
    df_fannie_acquisition = read_files(fannie_au,fannie_ac)
    return df_fannie_acquisition

def freddie_performance_load():
    freddie_pu ='s3a://ffinsight/freddie/historical_data1_time_Q*.txt'
    freddie_pc= ["loan_seq_no",\
                 "report_period",\
                 "cur_actual_upb",\
                 "cur_delinquency",\
                 "loan_age",\
                 "mon_to_maturity",\
                 "repurchase",\
                 "modification",\
                 "zero_balance_code",\
                 "zero_balance_date",\
                 "cur_interest_rate",\
                 "cur_deferred_upb",\
                 "ddlpi",\
                 "mi_recoveries",\
                 "net_sale_proceeds",\
                 "non_mi_recoveries",\
                 "expenses",\
                 "legal_costs",\
                 "maintain_costs",\
                 "tax_insurance",\
                 "miscellaneous_expenses",\
                 "actual_loss",\
                 "modification_cost",\
                 "step_modification",\
                 "deferred_payment_modification",\
                 "estimated_ltv"]
    df_freddie_performance = read_files(freddie_pu,freddie_pc)
    return df_freddie_performance

def freddie_origin_load():
    freddie_ou = 's3a://ffinsight/freddie/historical_data1_Q*.txt'
    freddie_oc = ["credit_score",\
                  "first_payment_date",\
                  "first_time_homebuyer_flag",\
                  "maturity_date",\
                  "msa",\
                  "mip",\
                  "number_of_units",\
                  "occupancy_status",\
                  "original_cltv",\
                  "original_dti",\
                  "original_upb",\
                  "original_ltv",\
                  "original_interest_rate",\
                  "channel",\
                  "prepayment_penalty_flag",\
                  "product_type",\
                  "property_state",\
                  "property_type",\
                  "postal_code",\
                  "loan_seq_no",\
                  "loan_purpose",\
                  "original_loan_term",\
                  "number_of_borrowers",\
                  "seller_name",\
                  "servicer_name",\
                  "super_conforming_flag"]
    df_freddie_origin = read_files(freddie_ou,freddie_oc)
    return df_freddie_origin

def add_agency(data,agency_name):
    df = data.withColumn('agency_name',lit(agency_name))
    return df

#define default
def default(df):
    df_serious_delinquency = df.filter((col('cur_delinquency')>=3)|(col("cur_delinquency")=='R')|(col("zero_balance_code")=='02')|(col("zero_balance_code")=='03')|(col("zero_balance_code")=='09')|(col("zero_balance_code")=='15')|(col("zero_balance_code")=='16')).select("loan_seq_no").distinct()
    df = df_serious_delinquency.withColumn('serious_delinquency',lit('Y'))
    return df

    
def fannie_unify(df):
    df = df.na.replace(["R", "B", "C"], ["R", "B", "C"], "channel")
    df = df.withColumn("originate_year", F.year(F.to_date(df.first_payment_date, "MM/yyyy")))
    df = df.withColumn("number_of_borrowers", F.when(df["number_of_borrowers"] >= 2,'2').otherwise('1'))
    return df


def freddie_unify(df):   
    df = df.na.replace(["R", "B", "C", "T", "9"], ["R", "B", "C", "U", "U"], "channel")
    df = df.withColumn("originate_year", F.year(F.to_date(df.first_payment_date, "yyyyMM")))
    df = df.withColumn('loan_purpose',regexp_replace('loan_purpose','N','R'))
    df = df.replace(['9','99'],'U')
    df = df.replace(['999','9999','000'],None)
    df = df.withColumn('number_of_borrowers',regexp_replace('number_of_borrowers','01','1'))
    df = df.withColumn('number_of_borrowers',regexp_replace('number_of_borrowers','02','2'))
    df = df.withColumn('number_of_borrowers',regexp_replace('number_of_borrowers','U','1'))
    return df 

def change_type(df):
    df = df.withColumn('credit_score',col('credit_score').cast(IntegerType()))
    df = df.withColumn('original_dti',col('original_dti').cast(IntegerType()))
    df = df.withColumn('number_of_borrowers',col('number_of_borrowers').cast(IntegerType()))
    df = df.withColumn('original_upb',col('original_upb').cast(IntegerType()))
    df = df.withColumn('original_ltv',col('original_ltv').cast(IntegerType()))
    df = df.withColumn('original_cltv',col('original_cltv').cast(IntegerType()))
    df = df.withColumn('original_loan_term',col('original_loan_term').cast(IntegerType()))
    df = df.withColumn('number_of_units',col('number_of_units').cast(IntegerType()))
    df = df.withColumn('original_interest_rate',col('original_interest_rate').cast(DoubleType()))
    df = df.withColumn('mip',col('mip').cast(IntegerType()))
    return df

#Concat fannie and Freddie data (first have to decide the common columns)
def ff_union(df_fannie,df_freddie):
    l1 = df_fannie.columns
    l2 = df_freddie.columns
    common_list = list(set(l1)-(set(l1)-set(l2)))
    df1_update = df_fannie.select(common_list)
    df2_update = df_freddie.select(common_list)
    df_union = df1_update.union(df2_update)
    return df_union

#combine original data with default data (original data would be features and default data would be labels)    
def combine_default(df_origin,df_default):
    complete_data = df_origin.join(df_default,on='loan_seq_no',how='outer')
    complete_data = complete_data.fillna({'serious_delinquency':'N'})
    return complete_data 

def process_data():
    df_fannie_p = fannie_performance_load()
    df_fannie_o = fannie_acquisition_load()
    df_freddie_p = freddie_performance_load()
    df_freddie_o = freddie_origin_load()
    df_fannie_p = add_agency(df_fannie_p,'fannie')
    df_fannie_o = add_agency(df_fannie_o,'fannie')
    df_freddie_p = add_agency(df_freddie_p,'freddie')
    df_freddie_o = add_agency(df_freddie_o,'freddie')
    df_fannie_o_unify = fannie_unify(df_fannie_o)
    df_freddie_o_unify = freddie_unify(df_freddie_o)
    df_fannie_o_unify = change_type(df_fannie_o_unify)
    df_freddie_o_unify = change_type(df_freddie_o_unify)
    df_fannie_default_id = default(df_fannie_p)
    df_freddie_default_id = default(df_freddie_p)
    df_fannie_combine = combine_default(df_fannie_o_unify,df_fannie_default_id)
    df_freddie_combine = combine_default(df_freddie_o_unify,df_freddie_default_id)
    df_union = ff_union(df_fannie_combine,df_freddie_combine)
    return df_union

def delete(df):
    df = df.drop(*['original_cltv','product_type','original_loan_term','postal_code','seller_name','first_payment_date'])
    return df

def fill_missing(df):
    df = df.fillna({'mip':0})
    credit_median = df.approxQuantile("credit_score", [0.5], 0.25)
    df = df.fillna({"credit_score":credit_median[0]})
    return df

def ml_data():
    df = process_data()
    df_delete = delete(df)
    df_filling =fill_missing(df)
    df_drop = df_filling.na.drop()
    url = 'postgresql://10.0.0.11:5432/mortgage'
    properties = {'user':'postgres','password':'Hm636188mei!','driver':'org.postgresql.Driver'}
    df_drop.write.jdbc(url='jdbc:%s' % url, table='mldata', mode='overwrite',  properties=properties)
    return

ml_data()
