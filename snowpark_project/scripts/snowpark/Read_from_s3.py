from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
import time
from pyspark import rdd

import pandas as pd
#from snowpark_connector import open_snowflake_session
from snowflake.snowpark.types import IntegerType, StringType, StructField, StructType, DateType

# Replace the below connection_parameters with your respective snowflake account,user name and password
import json

with open('connetion_parameters.json') as f:
    connection_properties=json.load(f)


session = Session.builder.configs(connection_properties).create()
#session = open_snowflake_session()

session.sql("use warehouse compute_wh").collect()
session.sql("use database COPMPUTE_DB").collect()



employee_s3 = session.read.csv('@sf_s3_stage/employee/')

schema = StructType([StructField("id", StringType()),
StructField("cat", StringType()),
StructField("name", StringType()),
StructField("author", StringType()),
StructField("price", IntegerType()),
 StructField("pages_i",IntegerType())])

print(schema.names)
# Use session.read.schema and session.read.csv and mention the command to read data from s3
employee_s3 = session.read.schema(schema).csv('@sf_s3_stage/Employee/')
employee_s3.show()
employee_s3 = session.read.options({"ON_ERROR":"CONTINUE"}).schema(schema).csv('@sf_s3_stage/Employee/')
employee_s3.show()
type(employee_s3)

employee_s3 = employee_s3.cache_result()
employee_s3.is_cached

employee_s4=employee_s3.cache_result()

type(employee_s4)

employee_s3.columns

employee_s5=employee_s3.select("FIRST_NAME","LAST_NAME").filter(col("FIRST_NAME")=='Nyssa')
employee_s5.show()

employee_s5.show()



employee_s3.queries

book_json_df =session.read.json('@sf_s3_stage/json_folder/books1.json')
book_json_df.count()
book_json_pd_df=book_json_df.select_expr("$1:id","$1:cat","$1:name","$1:author","$1:price","$1:pages_i").cache_result()

#assignment 1



schema1=StructType([StructField("id",IntegerType())])
help(session.create_dataframe)
df1=session.create_dataframe([[1,2,3,4],[5,6,7,8]],schema=["a","b","c","d"])
df1.show()

#assignement 2

df_orders_info=session.table("SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.ORDERS")
df_orders_info.show()
df_orders_info.count()
df_orders_select=df_orders_info.select("O_ORDERKEY","O_ORDERSTATUS","O_TOTALPRICE")
df_orders_select.show(20)
df_orders_select.describe().show()