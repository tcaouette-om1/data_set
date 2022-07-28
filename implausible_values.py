from datetime import datetime, timedelta
from matplotlib.pyplot import get
from pyparsing import col
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
from sqlalchemy import create_engine
import time
import matplotlib
from snowflake.connector.pandas_tools import write_pandas
from snowflake.connector.pandas_tools import pd_writer
from snowflake.sqlalchemy import URL
import getopt
import os
import sys
import datetime



def get_terminal():
    try:
        opts, args = getopt.getopt(sys.argv[1:], "u:r:d:s:x:y:w:z")
    except getopt.GetoptError as err:
        print (str(err))
        #usage()
        sys.exit(2)
    for o, a in opts:
        if o == "-u":
            user = a ## user name used for authentication, e.g., alafontant
        elif o == "-r":
            role = a ## data access role	
        elif o == "-d":
            database = a ## Name of database that contains QC tables        
        elif o == "-s":
            schema1 = a ## Name of schema that contains the tables that are going to be analyzed 
        elif o == "-z":
            schema = a ## Name of public schema
#        elif o == "-x":
#            source_db = a ## Name of source database 
#        elif o == "-y":
#            account_type = a ## om1 = deid snowflake, om1id = id snowflake
        elif o in ("-h"):
            sys.exit()
        else:
            assert False, "unhandled option"

    role = role#'ngr_exact_sciences'
    database = database#'ngr_exact_sciences'
    schema1 = schema1#'UNIVERSITY_HOSPITALS_TRANSFORMED_mapped'
    user = user #'tcaouette'
    ctx_id = snowflake.connector.connect(
        user = user,
        account = "om1id",
        authenticator = 'externalbrowser',
        role = role,
        database = database,
        schema = schema1,
        warehouse = 'LOAD_WH',
        autocommit = False
        )

    cs_id = ctx_id.cursor() #where the data lives that we are analyzing


    schema = 'public'
    ctx_id_new = snowflake.connector.connect(
        user = user,
        account = "om1id",
        authenticator = 'externalbrowser',
        role = role,
        database = database,
        schema = schema,
        warehouse = 'LOAD_WH',
        autocommit = False
        )

    cs_id_new = ctx_id_new.cursor() #where the analysis tables go


    return cs_id, ctx_id,schema1,cs_id_new,ctx_id_new,schema,user,database,role

def new_query(query,cs_id): #this one is the better one
    cs_id.execute(query)
    df = pd.DataFrame.from_records(iter(cs_id), columns=[x[0] for x in cs_id.description])
    return df.fillna('NULL')


def tables_schema(schema,cs_id):
    sql_tables = f'''show tables in {schema} ''' #always ordered by table_name alphabetically

    df_tables = new_query(sql_tables,cs_id)
    df_tables.columns =['created_on','table_name','database_name','schema_name','kind','comment',
    'cluster_by','rows','bytes','owner','retention_time','auto_cluster','change_tracking','search_op',
    'search_op_prog','search_op_bytes','is_external']

    table_names = df_tables['table_name'].to_list()
    return table_names    

# dynamically grab tables in schema, then each table into dataframe, column by column --- focus on diagnosis codes, patient age, other codes - length
# 
def rename_columns(table_names,cs_id):
    table_col =['table_name',	'schema_name',	'column_name',	'data_type',	'null',	'default',	'kind',	'expression',	'comment',	'database_name',	'autoincrement']
    df_all_list =[]
    for table in table_names:
        select_columns = f'''show columns in table {table}''' #always from left to right
        df_all_list.append(new_query(select_columns,cs_id))

    #normalize and apply the column headers to the df so that is in the correct format.
    #create dictionary that has table name the list of column names... apply at will
    #column names are needed to rename the columns in the next section. without these lists the headers are not in a good format.

    norm_df_list =[]
    for df in df_all_list:
        df.columns = table_col
        norm_df_list.append(df)
    print(norm_df_list)
    #   creates list of lists, those lists are the column names of every table---> should create dictionary with key being table name values list of column names
    list_columns =[]
    for df in norm_df_list:
        list_columns.append(df['column_name'].tolist())
    #dictionary! zip table_names & list_columns --> {table_name:[[column_names]]}
    table_col_dict = {}
    for k, v in zip(table_names, list_columns):
        table_col_dict.setdefault(k, []).append(v)
    #to access column headers for each df i=table_name [0] accesses the column value list which will be used to apply the new df headers
    #test run will be limit of 10 rows per dataframe
    # new dfs add limit 10 for testing
    all_sql_list =[]
    for k in table_names:
        df_sql = f'''select * from {k} '''
        all_sql_list.append(df_sql)   
    #print(all_sql_list)
    df_list =[]   

    for sql in all_sql_list:
    #df_list.append(fetch_pandas_old(cs_id,sql))
        df_list.append(new_query(sql,cs_id))
    #time.sleep(5.5)


    #dictionary tablenames and dataframes
    df_dict = {}
    for k, v in zip(table_names, df_list):
        df_dict.setdefault(k, []).append(v)
    #print(df_dict)

    return df_dict, table_col_dict


def buil_dfs(some_dict,some_key):

    res = [val for key, val in some_dict.items() if some_key in key]
    df = pd.DataFrame(res[0][0])
    return df

def patient_tests(df_dict,schema,user):
    patient_key = 'PATIENT'
    df_pts = buil_dfs(df_dict, patient_key)
    return print(df_pts.head())

        #if str(key).lower() in 'patient':
          #  print(df_dict[key][0])
#def build_big_df(df_dict,table_col_dict,schema,user):
#     list_df=[]

#     pairs = [   (key, value) 
#             for key, values in table_col_dict.items() 
#             for value in values[0] ]
#     for pair in pairs:
#     #print(f'''Table {pair[0]} and Column {pair[1]} Unique Values == {df_dict[pair[0]][0][pair[1]].unique()}''')
#         #df_dict[pair[0]][0] is the data frame its self.
#         #print(f'''Table {pair[0]} and Column {pair[1]} Counts Group By Column == {df_dict[pair[0]][0].groupby(pair[1])[pair[1]].count()}''')
#         #df_all_count = pd.DataFrame(df_dict[pair[0]][0][pair[1]])
#         #df_all_count = df_all_count.count().reset_index(name='Object_Count')
#         #df_all_count.columns =['Column_column','Object_Count']

#         df1 = pd.DataFrame(df_dict[pair[0]][0]) #list of groupby count dfs
#       #  if 'PATIENT'
#         #df1.insert(0,'Table_column',pair[0],True)
#     #    df1.insert(2,'Column_column',pair[1],True)

#         list_df.append(df1)
#         imp_df = pd.concat(list_df)
#     return imp_df

# 
# build functions for specific tests. Dates 
def date_checker(df):
    list_of_dates =datetime.datetime.now()
    # grab the table name and column name to insert in the what test it is.
    # might be able to just use dtypes here. 
    # look at patient table to see if it needs conversion to datetime or not.
    # df.columns.tolist()if column lower(name) is like date, dttm
    #for i in df.columns.tolist():
    #    if 'date' or 'dttm' in i.lower():
        #df.i 

    #   if table_column is like patient and if column is like birth and date field is year only, then year today - year birth = age
    # if age is <2 and >110 then flag false = out of range
    return list_of_dates
    

def percent_threshold(df):
    # add column to df with % threshold, true false flag for each field.
    # this is for tables that generally have patient ids
    # tables --> patient, diagnosis, encounter, med_order, med_admin
    return df

def plausibility(df):
    #plausibility of whether code is assigned typically to male or female genders use codes_procedures function
    #plausibility of dates in tables, less than 1950 this comes from OMOP will use date checker function
    return df

def codes_procedures(df):
    #function to get procedures and map to either male or female genders.
    return df

def build_final_df(df):
    main_df=pd.DataFrame()
    #builds final df from previous functions.
    return main_df


def main():
    #clean the code and add back the original percentage and quantiles... possibly min/max
    #cs_id, ctx_id,schema1,cs_id_new,ctx_id_new,schema,user,database,role
    cs_id, ctx_id,schema1,cs_id_new,ctx_id_new,schema,user,database,role = get_terminal() #new function for terminal input. 
    table_names = tables_schema(schema1,cs_id)
    #print(table_names)
    df_dict, table_col_dict = rename_columns(table_names,cs_id)
    print("START OF DICTIONARY")
    #print(df_dict)
    #build_big_df(df_dict,table_col_dict)
    patient_tests(df_dict,schema,user)
    #filter_df(df_dict,schema,user)
    #df_list = build_big_df(df_dict,table_col_dict,schema1,user)
    #print(df_list)

    cs_id.close()

    print('DATAFRAMES BUILT')
    # create df
    # now that the table is created, append to it
    # directory where I'll test output then will just write to snowflake db '/Users/tobiascaouette/Documents/Process_Validation/data_set_files_testing/result.csv'
    file_name ='/Users/tobiascaouette/Documents/Process_Validation/data_set_files_testing/implausible_testing.csv'
    #df_list.to_csv(file_name, sep='\t', encoding='utf-8') # investigate percent
    #append_table('table_test', 'append', None, df2)

    #send_df_snow(user,database,role,df_list,schema1,cs_id_new,schema)
    cs_id_new.close()

    print('CHECK DATABASE---FINISHED PROCESSING')
   
if __name__ == "__main__":
  main()