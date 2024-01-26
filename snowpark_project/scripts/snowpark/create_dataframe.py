from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
import time
#from snowpark_connector import open_snowflake_session
from snowflake.snowpark.types import IntegerType, StringType, StructField, StructType, DateType

# Replace the below connection_parameters with your respective snowflake account,user name and password
import json

with open('connetion_parameters.json') as f:
    connection_properties=json.load(f)


session = Session.builder.configs(connection_properties).create()
#session = open_snowflake_session()

session.sql("use warehouse compute_wh").collect()

test = session.create_dataframe({1, 2, 3, 4}, schema=["a"])

# create a dataframe by inferring a schema from the data

test = session.create_dataframe([1, 2, 3, 4], schema=["a"])

test.show()
type(test)

test = session.create_dataframe([[1, 2, 3, 123],[1, 2, 3, "ABC"],[1, 2, 3, "HPC"],[1, 2, 3, "EMD"]], schema=["a","b","c","d"])
test.show()

test = session.create_dataframe([[1, 2, 3, '26-01-2022'],[1, 2, 3, '26-01-2022'],[1, 2, 3, '26-01-2022'],[1, 2, 3, '26-01-2022']], schema=["a","b","c","d"])
test.show()

test = session.create_dataframe([[1, 2, 3, 26.897],[1, 2, 3, 27.897],[1, 2, 3, 29.897],[1, 2, 3, 39.897]], schema=["a","b","c","d"])
test.show(1)

test = session.create_dataframe([[1, 2, 3, None],[1, 2, 3, None],[1, 2, 3, None],[1, 2, 3, None]], schema=["a","b","c","d"])
test.show()

test = session.create_dataframe([[1, 2, 3, {"a":"hi"}],[1, 2, 3, None],[1, 2, 3, {"a":"Bye"}],[1, 2, 3, {"a":"hello"}]], schema=["a","b","c","d"])
test.show()

test = session.create_dataframe([[1, 2, 3, ["Hi"]],[1, 2, 3, None],[1, 2, 3,["Hello"] ],[1, 2, 3, ["Namaste"]]], schema=["a","b","c","d"])

test1 = test.cache_result()
test1.show()
type(test1)

# Check performance

begin = time.time()
test.show()
end = time.time()
print(f"Total runtime of the program is {end - begin}")


begin = time.time()
test1.show()
end = time.time()
print(f"Total runtime of the program is {end - begin}")


