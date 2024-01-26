from os import environ as env
from dotenv import load_dotenv
import snowflake.connector

load_dotenv()
print('user is : {}'.format(env['SF_USER']))
print('Pwd is : {}'.format(env['SF_PASSWORD']))
print('SF_ACCOUNT is : {}'.format(env['SF_ACCOUNT']))

## Creating connection using snowflake connector
conn=snowflake.connector.connect(
    user=format(env['SF_USER']),
    password=format(env['SF_PASSWORD']),
    account=format(env['SF_ACCOUNT'])
)

cur=conn.cursor()

cur.execute("select current_version()")



