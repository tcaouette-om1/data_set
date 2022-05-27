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
import matplotlib

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
# new take on the query function--- this one works better the other one doesn't account for empty tables
def new_query(query,cs_id): #this one is the better one
    cs_id.execute(query)
    df = pd.DataFrame.from_records(iter(cs_id), columns=[x[0] for x in cs_id.description])
    return df

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

def tables_schema(schema):
    sql_tables = f'''show tables in {schema} ''' #always ordered by table_name alphabetically

    df_tables = fetch_pandas_old(cs_id,sql_tables)
    df_tables.columns =['created_on','table_name','database_name','schema_name','kind','comment',
    'cluster_by','rows','bytes','owner','retention_time','auto_cluster','change_tracking','search_op',
    'search_op_prog','search_op_bytes','is_external']

    table_names = df_tables['table_name'].to_list()
    return table_names



# query each table in list  --> create df for each table --> find the dtype of each field in the table and run a stats query on each field in table

# need to create the dictionary here ---  {table_name:[columns]}   # this gets the column names to re-name each table...
def rename_columns(table_names):
    table_col =['table_name',	'schema_name',	'column_name',	'data_type',	'null',	'default',	'kind',	'expression',	'comment',	'database_name',	'autoincrement']
    df_all_list =[]
    for table in table_names:
        select_columns = f'''show columns in table {table}''' #always from left to right
        df_all_list.append(fetch_pandas_old(cs_id,select_columns))

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
    ##for i in table_names:
            #    print(table_col_dict[i][0])

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



    #df_all_list.append(fetch_pandas_old(cs_id,select_all))








#select the 
#for i in table_names:
##    df_dict[i]
#print(df_dict['ENCOUNTER'][0])
#df_encounter = df_dict['ENCOUNTER'][0] #dataframe!

#table column dictionary
#print(table_col_dict)



#accessing the dictionary df_dict['KEY'][value = 0 is the dataframe][columns in the value] 
#dictionary form so it's easier to maintain and keep track of which dataframe is which -->faster compute too
#will need column list dictionary built to apply the table, and columns
#['TABLE'][list 0 index]['COLUMN'].stat or functions
#print(df_dict['ENCOUNTER'][0]['SOURCE_PATIENT_ID'].unique())
#print(df_dict['ENCOUNTER'][0].groupby("SOURCE_PATIENT_ID")["SOURCE_PATIENT_ID"].count()) #titanic["Pclass"].value_counts() same
#print(df_dict['ENCOUNTER'][0].count())
# start on the calculation bit next then refactor
# the calculation of the descriptive stats can be done with python or sql... will mostlikely go with python and iterating through each df will be easier that way
#for k, v in table_col_dict.items():
#    print(df_dict[k][0][v[0]])


#key value pairs... (table, column), chose tuple so it's not mutable
#this will be a function with output being the stats and counts
#this runs through every table and every column... will need to add more info on the output showing which column and table it is from.
#need to add stats to this ---- every column is an object ---> int or float and use describe ----> need to figure out which columns are best for this
# table and column level
# directory where I'll test output then will just write to snowflake db '/Users/tobiascaouette/Documents/Process_Validation/data_set_files_testing/result.csv'
# add mean, mode, std i.e. variance... to this and the summary/descriptive calculations will be done. 
# after summary stats done... need to flag when out of spec... Not just the comparison between raw and transformed... which is already completed by profiler... 
# but this flag will be displayed that it is under or over predefined limits.
table_names = tables_schema(schema)

df_dict, table_col_dict = rename_columns(table_names)
df =pd.DataFrame
df1 =pd.DataFrame
df_quant =pd.DataFrame
df_mean=pd.DataFrame
df_median =pd.DataFrame
df_std =pd.DataFrame
df_min =pd.DataFrame
df_max =pd.DataFrame

file_name ='/Users/tobiascaouette/Documents/Process_Validation/data_set_files_testing/group_by_count.csv'
list_stat_df =[]
list_count_df =[]
list_quant_df =[]
list_median_df =[]
list_std_df =[]
list_min_max_df =[]
perc =[.20, .40, .60, .80]
include =['object', 'float', 'int']
def q1(x):
    return x.quantile(0.25)

def q2(x):
    return x.median()

def q3(x):
    return x.quantile(0.95)


def percentile(n):
    def percentile_(x):
        return np.percentile(x, n)
    percentile_.__name__ = 'percentile_%s' % n
    return percentile_

# create a list for each dataframe type... append the DF's with normalized column names and concat the list of DF's into ONE DF per type. These will be the DF's exported to SF.
pairs = [   (key, value) 
            for key, values in table_col_dict.items() 
            for value in values[0] ]
for pair in pairs:
    #print(f'''Table {pair[0]} and Column {pair[1]} Unique Values == {df_dict[pair[0]][0][pair[1]].unique()}''')
    #print(f'''Table {pair[0]} and Column {pair[1]} Counts Group By Column == {df_dict[pair[0]][0].groupby(pair[1])[pair[1]].count()}''')
    df1 = pd.DataFrame(df_dict[pair[0]][0].groupby(pair[1])[pair[1]].count().reset_index(name = 'Groupby Count')) #list of groupby count dfs
    df1.insert(0,'Schema',schema,True)
    df1.insert(1,'Table',pair[0],True)
    df1.insert(2,'Column',pair[1],True)
    df1.columns =['Schema','Table','Column','Unique Item','Groupby Count']
    #print(f'''Table {pair[0]} and Column {pair[1]} Counts Group By Column Percentage == {pd.DataFrame(((df_dict[pair[0]][0].groupby(pair[1])[pair[1]].count()/df_dict[pair[0]][0][pair[1]].count())*100).reset_index(name='Groupby Count Percentage'))}''')
    df = pd.DataFrame(((df_dict[pair[0]][0].groupby(pair[1])[pair[1]].count()/df_dict[pair[0]][0][pair[1]].count())*100).reset_index(name='Groupby Count Percentage'))
    df.insert(0,'Schema',schema,True)
    df.insert(1,'Table',pair[0],True)
    df.insert(2,'Column',pair[1],True)
    #df.insert(4,'Groupby Count',df_dict[pair[0]][0].groupby(pair[1])[pair[1]].count(),True) GOING TO ADD GROUP BY COUNTS IN THE DATAFRAME... Create DF then merge into percentage
    df.columns =['Schema','Table','Column','Unique Item','Groupby Count Percentage']
    #print(df)
    df_ljoin = df.merge(df1,on='Unique Item',how='left',indicator=True)
    df_new = df_ljoin[['Schema_x','Table_x','Column_x','Unique Item','Groupby Count','Groupby Count Percentage','_merge']]
    df_new.columns =['Schema','Table','Column','Unique Item','Groupby Count','Groupby Count Percentage','Validation']
    list_count_df.append(df_new)
    #print(df_new)

    #print(f'''Table {pair[0]} and Column {pair[1]} Counts Group By Column 50%, 75% and 95% Quantile== {df_dict[pair[0]][0].groupby(pair[1])[pair[1]].count().quantile([.5,.75,.95])}''')
    df_quant =pd.DataFrame(df_dict[pair[0]][0].groupby(pair[1])[pair[1]].count().quantile([.5,.75,.95]).reset_index(name='Group By Quantiles'))
    df_news = pd.DataFrame(df_dict[pair[0]][0].groupby(pair[1])[pair[1]].count().quantile([.5,.75,.95]))
    #print(df_news)
    df_quant.insert(0,'Schema',schema,True)
    df_quant.insert(1,'Table',pair[0],True)
    df_quant.insert(2,'Column',pair[1],True)
    df_quant.columns=['Schema','Table','Column','Quantile_50_75_95','Groupby Count']
    df_quant['Groupby Count']=df_quant['Groupby Count'].fillna(0).astype(np.int64)
    df1['Groupby Count']=df1['Groupby Count'].fillna(0).astype(np.int64)
    #list_quant_df.append(df_quant)
    #print(df_quant)
    df_quant_join = df_quant.merge(df1, on=['Table','Column','Groupby Count'],how='left', indicator=True)
    df_quant_join.drop_duplicates('Quantile_50_75_95',keep='first',inplace=True)
    df_new_quant=df_quant_join[['Schema_x','Table','Column','Unique Item','Groupby Count','Quantile_50_75_95','_merge']]
    df_new_quant.columns=['Schema','Table','Column','Unique Value','Groupby Count','Quantile_50_75_95','Validation']
    list_quant_df.append(df_new_quant)




    df_mean = pd.DataFrame(df_dict[pair[0]][0].groupby(pair[1])[pair[1]].count()).mean().to_frame().reset_index()
    
    df_mean.insert(0,'Schema',schema,True)
    df_mean.insert(1,'Table',pair[0],True)
    df_mean.columns =['Schema','Table','Column','Groupby Count Mean']
    list_stat_df.append(df_mean)
    #print(f'''Table {pair[0]} and Column {pair[1]} Counts Group By Column MEAN == {df_dict[pair[0]][0].groupby(pair[1])[pair[1]].count().mean()}''')

#MEDIAN is NEXT

    #print(f'''Table {pair[0]} and Column {pair[1]} Counts Group By Column MEDIAN == {df_dict[pair[0]][0].groupby(pair[1])[pair[1]].count().median()}''')
    df_median = pd.DataFrame(df_dict[pair[0]][0].groupby(pair[1])[pair[1]].count()).median().to_frame().reset_index()
    df_median.insert(0,'Schema',schema,True)
    df_median.insert(1,'Table',pair[0],True)
    df_median.columns =['Schema','Table','Column','Groupby Count']
    df_median['Groupby Count']=df_median['Groupby Count'].fillna(0).astype(np.int64)
    df1['Groupby Count']=df1['Groupby Count'].fillna(0).astype(np.int64)
    df_median_join = df_median.merge(df1, on=['Table','Column','Groupby Count'],how='left', indicator=True)
    df_median_join.drop_duplicates('Groupby Count',keep='first',inplace=True)
    df_new_median=df_median_join[['Schema_x','Table','Column','Unique Item','Groupby Count','_merge']]
    df_new_median.columns=['Schema','Table','Column','Unique Value','Median Groupby Count','Validation']
    #print(df_new_median)
    list_median_df.append(df_new_median)
    #print(df_median)

    df_std =pd.DataFrame(df_dict[pair[0]][0].groupby(pair[1])[pair[1]].count()).std().reset_index(name='STD')
    df_std.insert(0,'Schema',schema,True)
    df_std.insert(1,'Table',pair[0],True)
    df_std.columns=['Schema','Table','Column','STD Groupby Count']
    list_std_df.append(df_std)
    #df_min_max=pd.DataFrame(df_dict[pair[0]][0].groupby(pair[1])[pair[1]].count().reset_index(name='Value')).agg({'count': ['mean','std','min', 'max']}).T
    df_min_max=pd.DataFrame(df_dict[pair[0]][0].groupby(pair[1])[pair[1]].count().reset_index(name='Value'))#.describe()#.agg({'Value': ['describe']}) #.reset_index(name='count')
    df_min_max.columns =['Unique Value','Groupby Count']
    df_min_max.insert(0,'Schema',schema,True)
    df_min_max.insert(1,'Table',pair[0],True)
    df_min_max.insert(2,'Column',pair[1],True)
    df_allofem=pd.DataFrame(df_min_max.describe(percentiles = perc, include = include))
    df_allofem.insert(0,'Schema',schema,True)
    df_allofem.insert(1,'Table',pair[0],True)
    df_allofem.insert(2,'Column',pair[1],True)
    df_allofem.reset_index(inplace=True)
    df_allofem.rename(columns={'index':'Info'},inplace=True)
    df_info =df_allofem.pop('Info')
    df_allofem.insert(3,'Info',df_info)
    #df_allofem=df_allofem[['Schema1','Table1','Column1','Info','Unique Value','Groupby Count']]
    #print(df_allofem)
    list_min_max_df.append(df_allofem)
    #df_min_max.columns=['Schema','Table','Column','Count']
    #print(df_min_max)
    #df_min_max.insert(0,'Schema',schema,True)
    #df_min_max.insert(1,'Table',pair[0],True)
    #df_min_max.insert(2,'Column',pair[1],True)
    #print(df_min_max)
    #df_min_max.columns=['Schema','Table','Column','Count','Mean','STD','MIN','Q1_25','Q2_50','Q3_75','MAX'] # min add the min value, max add the max value ---categorical values here... merge on count, unique value where x=min y=max
    #list_min_max_df.append(df_min_max)
    #print(df_min_max)
    #print(f'''Table {pair[0]} and Column {pair[1]} Counts Group By Column Standard Deviation == {df_dict[pair[0]][0].groupby(pair[1])[pair[1]].count().std()}''')
    #print(f'''Table {pair[0]} and Column {pair[1]} Counts Group By Column Max Value == {df_dict[pair[0]][0].groupby(pair[1])[pair[1]].count().max()}''')
    #print(f'''Table {pair[0]} and Column {pair[1]} Counts Group By Column Min Value == {df_dict[pair[0]][0].groupby(pair[1])[pair[1]].count().min()}''')
mean_df = pd.concat(list_stat_df)
count_df =pd.concat(list_count_df)
quant_df =pd.concat(list_quant_df)
median_df =pd.concat(list_median_df)
std_df =pd.concat(list_std_df)
min_max_mean_std=pd.concat(list_min_max_df)
print(min_max_mean_std)
#print(std_df)
#print(quant_df)
#print(quant_df)
#.reset_index(name='Count')
#by table
#for table in table_names:
#    print(f'''Describing Table {table}  == {df_dict[table][0].astype('object').describe()}''')


#------comaprative raw and transformed-- run stats---- df_raw minus df_tansformed 
#----- visulaize the counts------
#------next suite will be the implausible values ------

#for dfs in list_stat_df:
#    print(dfs)
#add if statements --- when column i.e. pair[1] name is like age df_dict[pair[0]][0][pair[1]].astype('int').describe() ---something like this might do the trick
#if 'AGE' in pair[1]:   
#  df_dict[pair[0]][0][pair[1]].astype('int').describe()     

#to output properly add schema, table then normaize the columns counts might need to pivot after creation... create tables in snowflake
#pd.concat(list_stat_df).to_csv(file_name) 

#for i in list_stat_df:
#    print(i)


#count is the count of objects in the column --- does not include null
#unique is the count of unique items in the column
#top is the Max count in the column
#freq is the number of time the top appears in column




#pair[0] = table name, pair[1] = column name
# create function to loop through all tables and all columns... apply summary/descriptive stats ____ 
# the stats will have to be exported somewhere... CSV for now... database better for tableau hookup... or pyscript.

#print(df_dict['PATIENT'][0].astype('object').describe())

