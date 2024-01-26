# Write data after reading from snow table.

from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
import time
from snowpark_connector import open_snowflake_session
from snowflake.snowpark.types import IntegerType, StringType, StructField, StructType, DateType


session=open_snowflake_session()


session.get_current_database()
session.get_current_schema()
# Replace the below connection_parameters with your respective snowflake account,user name and password

customer = session.table("SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.CUSTOMER")
#customer.show()
customer = customer.filter(col("C_NATIONKEY")=='23').select("C_NAME")

session.sql("use database COPMPUTE_DB")

customerwrt = customer.write.mode("overwrite").save_as_table("COPMPUTE_DB.public.SNOW_CUSTOMER")
customerwrt_temp = customer.write.mode("overwrite").save_as_table("COPMPUTE_DB.public.SNOW_CUSTOMER_temp",table_type= "temporary")

customerwrt_VT = customer.write.mode("overwrite").save_as_table("COPMPUTE_DB.public.SNOW_CUSTOMER_vt",table_type= "transient")
session.sql("show tables").show()

session.table("COPMPUTE_DB.public.SNOW_CUSTOMER_vt").count()
session.table("COPMPUTE_DB.public.SNOW_CUSTOMER_temp").count()
# Write data after reading from s3


# Mention command to read data from '@my_s3_stage/json_folder/'
employee_s3_json = session.read.json('@sf_s3_stage/json_folder/')
#employee_s3_json = employee_s3_json.select(col("$1").as_("book_tbl"))
employee_s3_json.write.mode("append").save_as_table("DEMO_DB.PUBLIC.JSON_BOOK")


employee_s3_json = employee_s3_json.select_expr("$1:author","$1:id","$1:cat")
employee_s3_json.show()
employee_s3_json.columns
employee_s3_json = employee_s3_json.select(col('"$1:AUTHOR"').as_("author"),col('"$1:ID"').as_("id"),col('"$1:CAT"').as_("cat"))
#employee_s3_json.select(col('"$1:AUTHOR"').as_("author").cast(to=StringType())).show()
employee_s3_json.columns
employee_s3_json_str=employee_s3_json.select(col())
employee_s3_json.write.mode("overwrite").save_as_table("PUBLIC.JSON_BOOK_PARSED")

employee_s3_json.write.mode("overwrite").

# Write csv data from s3 to snowflake.

schema = StructType([StructField("FIRST_NAME", StringType()),
StructField("LAST_NAME", StringType()),
StructField("EMAIL", StringType()),
StructField("ADDRESS", StringType()),
StructField("CITY", StringType()),
 StructField("DOJ",DateType())])

employee_s3_csv = session.read.schema(schema).csv('@my_s3_stage/employee/')
employee_s3_csv.show()
employee_s3_csv = session.read.options({"ON_ERROR":"CONTINUE"}).schema(schema).csv('@my_s3_stage/employee/')
employee_s3_csv.write.mode("append").save_as_table("DEMO_DB.PUBLIC.EMPLOYEE_CSV")

employee_s3_csv.write.mode("append").saveAsTable("DEMO_DB.PUBLIC.EMPLOYEE_CSV")

# wrtiting Json asignmnt


session.sql("show tables").show()
emp_schema = StructType([StructField("FIRST_NAME", StringType()),StructField("LAST_NAME", StringType()),StructField("EMAIL", StringType()),\
    StructField("ADDRESS", StringType()),StructField("CITY", StringType()), \
        StructField("DOJ",DateType())])

session.sql("list @SF_S3_STAGE/Employee/").show()

employee_s3_csv=session.read.options({"ON_ERROR":"CONTINUE"}).schema(emp_schema).csv('@SF_S3_STAGE/Employee/')


employee_s3_csv.show()

column=["FIRST_NAME","LAST_NAME","EMAIL","ADDRESS","CITY","DOJ"]


# To see the rejected recoeds in a query_id and load statement
with session.query_history() as query_history:
    emloyee_copy_result=employee_s3_csv.copy_into_table("employee",FORCE=True,target_columns=column,on_error="CONTINUE")


query=query_history.queries

query
query_id = [id.query_id  for id in query if 'COPY' in id.sql_text]

query_id[0]
type(query_id)
sql_text=f"select *  from table(validate(employee , job_id =>"+ "'"+ str(query_id[0]) +"'))"


rejects = session.sql(sql_text)
rejects.select(col("ERROR"),col("CODE")).show()

emloyee_copy_result_df=session.create_dataframe(emloyee_copy_result)
