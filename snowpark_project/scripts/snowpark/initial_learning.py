import snowflake.snowpark.functions
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
#create a connection_propeties.json file at root directory and import as dict
import json

with open('connetion_parameters.json') as f:
    connection_properties=json.load(f)

print(type(connection_properties))

print("User name is:{}".format(connection_properties['user']))

session = Session.builder.configs(connection_properties).create()

# sql_test= """ select current_warehouse(), current_database(),
#               current_schema() 
#               """
              
# #sql_test1= """ select count(*) from orders 
#               """              
# print(session.sql(sql_test).collect())

# print(session.sql(sql_test1).collect())

df_customer_info=session.table("customer")

df_filtered = df_customer_info.filter((col("C_MKTSEGMENT") =='HOUSEHOLD'))
df_filtered = df_customer_info.select(col("C_NAME"),col("C_ADDRESS"))
df_filtered.show()

df_filtered.describe().show()



