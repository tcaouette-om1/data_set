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

#refactor... with the new function that works a lot better
def fetch_pandas_old(cur, sql):
    cur.execute(sql)
    rows = 0
    while True:
        dat = cur.fetchmany(500000000000000)
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
# new dfs add limit 10 for testing
all_sql_list =[]
for k in table_names:
    df_sql = f'''select * from {k} '''
    all_sql_list.append(df_sql)   
print(all_sql_list)
df_list =[]   

# new take on the query function--- this one works better the other one doesn't account for empty tables
def new_query(query,cs_id):
    cs_id.execute(query)
    df = pd.DataFrame.from_records(iter(cs_id), columns=[x[0] for x in cs_id.description])
    return df


for sql in all_sql_list:
    #df_list.append(fetch_pandas_old(cs_id,sql))
    df_list.append(new_query(sql,cs_id))
    #time.sleep(5.5)
print(df_list)
print(len(df_list))

#dictionary tablenames and dataframes
df_dict = {}
for k, v in zip(table_names, df_list):
   df_dict.setdefault(k, []).append(v)
print(df_dict)

#select the 
for i in table_names:
    df_dict[i]
print(df_dict['ENCOUNTER'][0])
df_encounter = df_dict['ENCOUNTER'][0] #dataframe!

#table column dictionary
print(table_col_dict)



#accessing the dictionary df_dict['KEY'][value = 0 is the dataframe][columns in the value] 
#dictionary form so it's easier to maintain and keep track of which dataframe is which -->faster compute too
#will need column list dictionary built to apply the table, and columns
#['TABLE'][list 0 index]['COLUMN'].stat or functions
print(df_dict['ENCOUNTER'][0]['SOURCE_PATIENT_ID'].unique())
print(df_dict['ENCOUNTER'][0].groupby("SOURCE_PATIENT_ID")["SOURCE_PATIENT_ID"].count()) #titanic["Pclass"].value_counts() same
print(df_dict['ENCOUNTER'][0].count())
# start on the calculation bit next then refactor
# the calculation of the descriptive stats can be done with python or sql... will mostlikely go with python and iterating through each df will be easier that way
for k, v in table_col_dict.items():
    print(df_dict[k][0][v[0]])


#key value pairs... (table, column), chose tuple so it's not mutable
#this will be a function with output being the stats and counts
#this runs through every table and every column... will need to add more info on the output showing which column and table it is from.
#need to add stats to this ---- every column is an object ---> int or float and use describe ----> need to figure out which columns are best for this
# table and column level
# directory where I'll test output then will just write to snowflake db '/Users/tobiascaouette/Documents/Process_Validation/data_set_files_testing/result.csv'
# add mean, mode, std i.e. variance... to this and the summary/descriptive calculations will be done. 
# after summary stats done... need to flag when out of spec... Not just the comparison between raw and transformed... which is already completed by profiler... 
# but this flag will be displayed that it is under or over predefined limits.
pairs = [   (key, value) 
            for key, values in table_col_dict.items() 
            for value in values[0] ]
for pair in pairs:
    print(f'''Table {pair[0]} and Column {pair[1]} Unique Values == {df_dict[pair[0]][0][pair[1]].unique()}''')
    print(f'''Table {pair[0]} and Column {pair[1]} Counts Group By Column == {df_dict[pair[0]][0].groupby(pair[1])[pair[1]].count()}''')
    print(f'''Table {pair[0]} and Column {pair[1]} Counts Group By Column Percentage == {(df_dict[pair[0]][0].groupby(pair[1])[pair[1]].count()/df_dict[pair[0]][0][pair[1]].count())*100}''')
    print(f'''Table {pair[0]} and Column {pair[1]} Counts Group By Column 50%, 75% and 95% Quantile== {df_dict[pair[0]][0].groupby(pair[1])[pair[1]].count().quantile([.5,.75,.95])}''')
    print(f'''Table {pair[0]} and Column {pair[1]} Counts Group By Column MEAN == {df_dict[pair[0]][0].groupby(pair[1])[pair[1]].count().mean()}''')
    print(f'''Table {pair[0]} and Column {pair[1]} Counts Group By Column MEDIAN == {df_dict[pair[0]][0].groupby(pair[1])[pair[1]].count().median()}''')
    print(f'''Table {pair[0]} and Column {pair[1]} Counts Group By Column Standard Deviation == {df_dict[pair[0]][0].groupby(pair[1])[pair[1]].count().std()}''')
    print(f'''Table {pair[0]} and Column {pair[1]} Counts Group By Column Standard Max Value == {df_dict[pair[0]][0].groupby(pair[1])[pair[1]].count().max()}''')
    print(f'''Table {pair[0]} and Column {pair[1]} Counts Group By Column Standard Min Value == {df_dict[pair[0]][0].groupby(pair[1])[pair[1]].count().min()}''')


#by table
for table in table_names:
    print(f'''Describing Table {table}  == {df_dict[table][0].astype('object').describe()}''')


# ------next suite will be the implausible values ------


#add if statements --- when column i.e. pair[1] name is like age df_dict[pair[0]][0][pair[1]].astype('int').describe() ---something like this might do the trick
#if 'AGE' in pair[1]:
#  df_dict[pair[0]][0][pair[1]].astype('int').describe()     



#count is the count of objects in the column --- does not include null
#unique is the count of unique items in the column
#top is the Max count in the column
#freq is the number of time the top appears in column




#pair[0] = table name, pair[1] = column name
# create function to loop through all tables and all columns... apply summary/descriptive stats ____ 
# the stats will have to be exported somewhere... CSV for now... database better for tableau hookup... or pyscript.

#print(df_dict['PATIENT'][0].astype('object').describe())

