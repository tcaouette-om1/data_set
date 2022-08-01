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
    #datelist= ['date','year','DATE','YEAR','dttm','DTTM']
    patient_key = 'PATIENT'
    df = buil_dfs(df_dict, patient_key)
    #filter_col = [col for col in df_pts.columns if col in datelist] # probably create another helper function with this.
    #filter_col = df_pts[df_pts.str.contains('|'.join(datelist))]
    #filter_col= df_pts.filter(regex='|'.join(datelist))
    return df

def encounter_tests(df_dict):
    #datelist= ['date','year','DATE','YEAR','dttm','DTTM']
    encounter_key ='ENCOUNTER'
    df = buil_dfs(df_dict,encounter_key)
    #filter_col= df_enc.filter(regex='|'.join(datelist))
    #print(filter_col.dtypes)
    return df

def date_join(df_pts, df_enc, df_diag):
    df_pts_enc = pd.merge(df_pts,df_enc,left_on=['PATIENT_ID'], right_on=['PATIENT_ID'])
    df_pts_diag = pd.merge(df_pts,df_diag,left_on=['PATIENT_ID'], right_on=['PATIENT_ID'])
    return df_pts_enc, df_pts_diag

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
def age_groups(x):
    if x < 2 or x >= 140:
        return 'F'
    elif x == 'NULL':
        return 'N'
    else:
        return 'P'

def birth_date_tester(x): #helper functions
    #need to add in NULL string into this... return N
    fail_birth_date = datetime.datetime.strptime('1900-01-01','%Y-%m-%d')
    if x < fail_birth_date:
        return 'F'
    elif x > fail_birth_date:
        return 'P'
    else:
        return 'N'

def date_tester(x):
    #need to add in NULL string into this... return N
    fail_date =datetime.datetime.strptime('1950-01-01','%Y-%m-%d')
    if x < fail_date:
        return 'F'
    elif x > fail_date:
        return 'P'
    else:
        return 'N'

# build functions for specific tests. Dates 
def date_checker(df):
    date_today =datetime.datetime.now()
    currentDay = datetime.datetime.now().day
    currentMonth = datetime.datetime.now().month
    currentYear = datetime.datetime.now().year
    column_name = df.columns.tolist()
    print(currentYear)
    #initial column filtering
    datelist= ['date','year','DATE','YEAR','dttm','DTTM']
    df = df.filter(regex='|'.join(datelist))

    for i in df.columns.tolist():
        if 'YEAR' in i and 'BIRTH' in i:
            #df[i].mask(df[i] == 'NULL', datetime.datetime(1, 1, 1, 0, 0), inplace=True)
            df[f'{i}_TEST'] = df[i].astype(int).subtract(int(currentYear)).abs()
            df[f'{i}_TEST'] =  df[f'{i}_TEST'].apply(age_groups)
        if 'DATE' in i and 'BIRTH' in i:
            if df[i].dtypes == 'object':
                df[i].mask(df[i] == 'NULL', datetime.datetime(1, 1, 1, 0, 0), inplace=True)
                df[f'{i}_TEST'] = pd.to_datetime(df[i], format="%Y-%m-%d %H:%M:%S.%f",errors = 'coerce')
                df[f'{i}_TEST'] = df[f'{i}_TEST'].dt.strftime("%Y-%m-%d %H:%M:%S.%f")
                df[f'{i}_TEST'] = pd.to_datetime(df[f'{i}_TEST'], format="%Y-%m-%d %H:%M:%S.%f",errors = 'coerce')
                
                #df['newage'] = (df['AGE_2'] - date_today).astype('timedelta64[Y]').astype('int')
                df[f'{i}_TEST'] = df[f'{i}_TEST'].apply(birth_date_tester)
            if df[i].dtypes == 'datetime64[ns]':
                #df[i].mask(df[i] == 'NULL', datetime.datetime(1, 1, 1, 0, 0), inplace=True)
                df['newage'] = (df['AGE_2'] - date_today).astype('timedelta64[Y]').astype('int')
                df[f'{i}_TEST'] = df[f'{i}_TEST'].apply(birth_date_tester)

        if 'DTTM' in i:
            if  df[i].dtypes == 'datetime64[ns]':
                #df[i].mask(df[i] == 'NULL', datetime.datetime(1, 1, 1, 0, 0), inplace=True)
                df[f'{i}_TEST'] = df[i].apply(date_tester)
            if df[i].dtypes =='object':
                df[i].mask(df[i] == 'NULL', datetime.datetime(1, 1, 1, 0, 0), inplace=True) # this is needed in the other sections to transform null, don't forget to add errors='coerce' to_datetime               
                df[f'{i}_TEST'] = pd.to_datetime(df[i], format="%Y-%m-%d %H:%M:%S.%f",errors = 'coerce')
                df[f'{i}_TEST'] = df[f'{i}_TEST'].dt.strftime("%Y-%m-%d %H:%M:%S.%f")
                df[f'{i}_TEST'] = pd.to_datetime(df[f'{i}_TEST'], format="%Y-%m-%d %H:%M:%S.%f",errors = 'coerce')
                df[f'{i}_TEST'] = df[f'{i}_TEST'].apply(date_tester)
                
        if 'DATE' in i and 'BIRTH' not in i:
            if  df[i].dtypes == 'datetime64[ns]':
                #df[i].mask(df[i] == 'NULL', datetime.datetime(1, 1, 1, 0, 0), inplace=True)
                df[f'{i}_TEST'] = df[i].apply(date_tester)
            if df[i].dtypes =='object':
                df[i].mask(df[i] == 'NULL', datetime.datetime(1, 1, 1, 0, 0), inplace=True) # this is needed in the other sections to transform null, don't forget to add errors='coerce' to_datetime               
                df[f'{i}_TEST'] = pd.to_datetime(df[i], format="%Y-%m-%d %H:%M:%S.%f",errors = 'coerce')
                df[f'{i}_TEST'] = df[f'{i}_TEST'].dt.strftime("%Y-%m-%d %H:%M:%S.%f")
                df[f'{i}_TEST'] = pd.to_datetime(df[f'{i}_TEST'], format="%Y-%m-%d %H:%M:%S.%f",errors = 'coerce')
                df[f'{i}_TEST'] = df[f'{i}_TEST'].apply(date_tester)
               # else:
                #    df[f'{i}_TEST'] = pd.to_datetime(df[i], format="%Y-%m-%d %H:%M:%S.%f")
                #    df[f'{i}_TEST'] = df[f'{i}_TEST'].dt.strftime("%Y-%m-%d %H:%M:%S.%f")
                #    df[f'{i}_TEST'] = pd.to_datetime(df[f'{i}_TEST'], format="%Y-%m-%d %H:%M:%S.%f")
                    #df[f'{i}_TEST'] = df[f'{i}_TEST'].apply(date_tester)
    
    # next date checks will be birthdate - diagnosis date join on patient_id ---this will be for encounter as well    
    # grab the table name and column name to insert in the what test it is.
    # might be able to just use dtypes here. 
    # look at patient table to see if it needs conversion to datetime or not.

    return df
    

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

    df = patient_tests(df_dict,schema,user)
    pts_date_df =date_checker(df)
    #df = encounter_tests(df_dict)
    #encounter_date_df = date_checker(df)
    #filter_df(df_dict,schema,user)
    #df_list = build_big_df(df_dict,table_col_dict,schema1,user)
    #print(df_list)

    cs_id.close()

    print('DATAFRAMES BUILT')
    # create df
    # now that the table is created, append to it
    # directory where I'll test output then will just write to snowflake db '/Users/tobiascaouette/Documents/Process_Validation/data_set_files_testing/result.csv'
    file_name ='/Users/tobiascaouette/Documents/Process_Validation/data_set_files_testing/implausible_testing.csv'
    pts_date_df.to_csv(file_name, sep='\t', encoding='utf-8') # investigate percent
    #append_table('table_test', 'append', None, df2)

    #send_df_snow(user,database,role,df_list,schema1,cs_id_new,schema)
    cs_id_new.close()

    print('CHECK DATABASE---FINISHED PROCESSING')
   
if __name__ == "__main__":
  main()