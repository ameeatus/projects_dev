from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
import time

from snowflake.snowpark.types import IntegerType, StringType, StructField, StructType, DateType

# Replace the below connection_parameters with your respective snowflake account,user name and password
import json

with open('connetion_parameters.json') as f:
    connection_properties=json.load(f)


session = Session.builder.configs(connection_properties).create()

session.sql("use warehouse compute_wh").collect()
session.sql("use database COPMPUTE_DB").collect()

schema = StructType([StructField("one", IntegerType()),
StructField("two",  IntegerType()),
StructField("three",  IntegerType()),
StructField("four",  DateType())])

test = session.create_dataframe([[1, 2, 3, '2022-01-26'],[1, 2, 3, '2022-01-26'],[1, 2, 3, '2022-01-26'],[1, 2, 3, '2022-01-26']], schema=["a","b","c","d"])
test.show()

test1 = session.create_dataframe([[1, 2, 3, '2022-01-26'],[1, 2, 3, '2022-01-26'],[1, 2, 3, '2022-01-26'],[1, 2, 3, '2022-01-26']], schema=schema)
test1.show()

test2=test1.cache_result()
type(test2)
type(test1)

help(test2.delete)

test3=session.table("SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.Customer")
print(test3.schema)
print(test3.schema)

schema = StructType([StructField('C_CUSTKEY', LongType(), nullable=False), 
                StructField('C_NAME', StringType(25), nullable=False),
                    StructField('C_ADDRESS', StringType(40), nullable=False), 
                        StructField('C_NATIONKEY', LongType(), nullable=False), 
                            StructField('C_PHONE', StringType(15), nullable=False), 
                                StructField('C_ACCTBAL', DecimalType(12, 2), nullable=False),
                                    StructField('C_MKTSEGMENT', StringType(10), nullable=True),
                                        StructField('C_COMMENT', StringType(117), nullable=True)])

test3.select(col("C_MKTSEGMENT")).distinct().show()

test3.filter(col("C_MKTSEGMENT")=='AUTOMOBILE').print_schema()