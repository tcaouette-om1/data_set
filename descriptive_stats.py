from datetime import datetime, timedelta
import snowflake.connector
import pandas as pd
import numpy as np
from math import pi,sqrt
from pandas.io.json import json_normalize
import json
import os
import random
import string
import decimal
import hashlib
import sqlalchemy
import time

role = 'ngr_exact_sciences'
database = 'ngr_exact_sciences'
schema ='UNIVERSITY_HOSPITALS_TRANSFORMED_mapped'
ctx_id = snowflake.connector.connect(
    user = 'tcaouette',
    account = "om1id",
    authenticator = 'externalbrowser',
    role = role,
    database = database,
    schema = schema,
    warehouse = 'LOAD_WH',
    autocommit = False
    )

cs_id = ctx_id.cursor()

schema = 'UNIVERSITY_HOSPITALS_TRANSFORMED_mapped'


def fetch_pandas_old(cur, sql):
    cur.execute(sql)
    rows = 0
    while True:
        dat = cur.fetchmany(50000)
        if not dat:
            break
        df = pd.DataFrame(dat, columns=cur.description)
        rows += df.shape[0]
    return df

sql_tables = f'''show tables in {schema} ''' #always ordered by table_name alphabetically

df_tables = fetch_pandas_old(cs_id,sql_tables)
df_tables.columns =['created_on','table_name','database_name','schema_name','kind','comment',
'cluster_by','rows','bytes','owner','retention_time','auto_cluster','change_tracking','search_op',
'search_op_prog','search_op_bytes','is_external']

print(df_tables.head())

print(df_tables.columns.to_list())
table_names = df_tables['table_name'].to_list()
print(table_names)


# query each table in list  --> create df for each table --> find the dtype of each field in the table and run a stats query on each field in table

# need to create the dictionary here ---  {table_name:[columns]}   # this gets the column names to re-name each table...
table_col =['table_name',	'schema_name',	'column_name',	'data_type',	'null',	'default',	'kind',	'expression',	'comment',	'database_name',	'autoincrement']
df_all_list =[]
for table in table_names:
    select_columns = f'''show columns in table {table}''' #always from left to right
    df_all_list.append(fetch_pandas_old(cs_id,select_columns))

#normalize and apply the column headers to the df so that is in the correct format.
norm_df_list =[]
for df in df_all_list:
    df.columns = table_col
    norm_df_list.append(df)
#create dictionary that has table name the list of column names... apply at will
#column names are needed to rename the columns in the next section. without these lists the headers are not in a good format.

    #df_all_list.append(fetch_pandas_old(cs_id,select_all))

print(norm_df_list)
#creates list of lists, those lists are the column names of every ---> should create dictionary with key being table name values list of column names
list_columns =[]
for df in norm_df_list:
    list_columns.append(df['column_name'].tolist())


#print(list_columns)

#dictionary! zip table_names & list_columns --> {table_name:[[column_names]]}
table_col_dict = {}
for k, v in zip(table_names, list_columns):
   table_col_dict.setdefault(k, []).append(v)

#to access column headers for each df i=table_name [0] accesses the column value list which will be used to apply the new df headers
##for i in table_names:
#    print(table_col_dict[i][0])

#test run will be limit of 10 rows per dataframe
# new dfs
all_sql_list =[]
for k in table_names:
    df_sql = f'''select * from {k} limit 10'''
    all_sql_list.append(df_sql)   
print(all_sql_list)
    
    
#all_df_list.append(fetch_pandas_old(cs_id,df_sql))
    #time.sleep(5.5)

#df_dict ={}
#for k,v in zip(table_names,all_df_list):
##     df_dict.setdefault(k,v)

#print(df_dict)

#.columns = table_col_dict[i][0]

#print(table_col_dict['ENCOUNTER'][0])
# the calculation of the descriptive stats can be done with python or sql... will mostlikely go with python and iterating through each df will be easier that way

all_sql = f'''
select * from {table} limit 10
'''




#continuous_sql = f'''
#SELECT MIN({column}) AS value_min, 
#       MAX({column}) AS value_max, 
#       AVG({column}) AS value_avg, 
#       STDDEV({column}) AS value_stddev, 
#       VAR({column}) AS value_var
#FROM {table}
#WHERE {column} IS NOT NULL
#'''