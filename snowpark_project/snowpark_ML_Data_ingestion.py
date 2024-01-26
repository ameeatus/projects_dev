from snowflake.snowpark import Session
from snowflake.snowpark.version import VERSION
from snowflake.snowpark.types import StructType, StructField, DoubleType, StringType
import snowflake.snowpark.functions as F
# data science libs
import numpy as np

# misc
import json

with open('connetion_parameters.json') as f:
    connection_properties=json.load(f)

#print("User name is:{}".format(connection_properties['user']))

try:
    session = Session.builder.configs(connection_properties).create()
except Exception as e:
    raise e

print(" Connection session satrted for user :{} ".format(connection_properties['user']))

session.sql_simplifier_enabled = True

snowflake_environment = session.sql('SELECT current_user(), current_version()').collect()

print(snowflake_environment)

snowpark_version = VERSION

# Current Environment Details
print('\nConnection Established with the following parameters:')
print('User                        : {}'.format(snowflake_environment[0][0]))
print('Role                        : {}'.format(session.get_current_role()))
print('Database                    : {}'.format(session.get_current_database()))
print('Schema                      : {}'.format(session.get_current_schema()))
print('Warehouse                   : {}'.format(session.get_current_warehouse()))
print('Snowflake version           : {}'.format(snowflake_environment[0][1]))
print('Snowpark for Python version : {}.{}.{}'.format(snowpark_version[0],snowpark_version[1],snowpark_version[2]))

#############################################################
#  Data frame reader to read data from S3 - external stages #
#############################################################

#show the file

session.sql("LS @DIAMONDS_ASSETS;").show()

#read the file to DF

diamonds_df = session.read.options({"field_delimiter": ",",
                                    "field_optionally_enclosed_by": '"',
                                    "infer_schema": True,
                                    "parse_header": True}).csv("@DIAMONDS_ASSETS")

diamonds_df.show()

#descriptive stats
diamonds_df.describe().show()

 



