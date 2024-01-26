import snowflake.snowpark.functions
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
#create a connection_propeties.json file at root directory and import as dict
import json

def open_snowflake_session():
    
    with open('connetion_parameters.json') as f:
        connection_properties=json.load(f)

    session = Session.builder.configs(connection_properties).create()

    return session


