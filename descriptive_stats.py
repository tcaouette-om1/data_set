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

#add in target role, database, schema terminal entry

#perfect place for a class connection:
#   class Connection:
#       def __init__(self):
#           self.role
#           self.database
#           self.schema
#       def connect_me(self):
            # ctx_id = snowflake.connector.connect(
            #     user = 'tcaouette',
            #     account = "om1id",
            #     authenticator = 'externalbrowser',
            #     role = role,
            #     database = database,
            #     schema = schema,
            #     warehouse = 'LOAD_WH',
            #     autocommit = False
            # )
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

#    schema = 'UNIVERSITY_HOSPITALS_TRANSFORMED_mapped'

#refactor... with the new function that works a lot better
# new take on the query function--- this one works better the other one doesn't account for empty tables
def new_query(query,cs_id): #this one is the better one
    cs_id.execute(query)
    df = pd.DataFrame.from_records(iter(cs_id), columns=[x[0] for x in cs_id.description])
    return df.fillna('NULL')

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

def tables_schema(schema,cs_id):
    sql_tables = f'''show tables in {schema} ''' #always ordered by table_name alphabetically

    df_tables = fetch_pandas_old(cs_id,sql_tables)
    df_tables.columns =['created_on','table_name','database_name','schema_name','kind','comment',
    'cluster_by','rows','bytes','owner','retention_time','auto_cluster','change_tracking','search_op',
    'search_op_prog','search_op_bytes','is_external']

    table_names = df_tables['table_name'].to_list()
    return table_names



# query each table in list  --> create df for each table --> find the dtype of each field in the table and run a stats query on each field in table

# need to create the dictionary here ---  {table_name:[columns]}   # this gets the column names to re-name each table...
def rename_columns(table_names,cs_id):
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







#accessing the dictionary df_dict['KEY'][value = 0 is the dataframe][columns in the value] 
#dictionary form so it's easier to maintain and keep track of which dataframe is which -->faster compute too
#will need column list dictionary built to apply the table, and columns



#key value pairs... (table, column), chose tuple so it's not mutable
#this will be a function with output being the stats and counts
#this runs through every table and every column... will need to add more info on the output showing which column and table it is from.
#need to add stats to this ---- every column is an object ---> int or float and use describe ----> need to figure out which columns are best for this
# table and column level
# directory where I'll test output then will just write to snowflake db '/Users/tobiascaouette/Documents/Process_Validation/data_set_files_testing/result.csv'
# add mean, mode, std i.e. variance... to this and the summary/descriptive calculations will be done. 
# after summary stats done... need to flag when out of spec... Not just the comparison between raw and transformed... which is already completed by profiler... 
# but this flag will be displayed that it is under or over predefined limits.
#table_names = tables_schema(schema)

#df_dict, table_col_dict = rename_columns(table_names)

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
list_min =[]
list_max =[]
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
def build_big_df(df_dict,table_col_dict,schema):
    pairs = [   (key, value) 
            for key, values in table_col_dict.items() 
            for value in values[0] ]
    for pair in pairs:
    #print(f'''Table {pair[0]} and Column {pair[1]} Unique Values == {df_dict[pair[0]][0][pair[1]].unique()}''')

        #print(f'''Table {pair[0]} and Column {pair[1]} Counts Group By Column == {df_dict[pair[0]][0].groupby(pair[1])[pair[1]].count()}''')
        #df_all_count = pd.DataFrame(df_dict[pair[0]][0][pair[1]])
        #df_all_count = df_all_count.count().reset_index(name='Object_Count')
        #df_all_count.columns =['Column_column','Object_Count']

        df1 = pd.DataFrame(df_dict[pair[0]][0].groupby(pair[1])[pair[1]].count().reset_index(name = 'GroupbyCount')) #list of groupby count dfs
        #print(df1)
        df1.insert(0,'Schema_column',schema,True)
        df1.insert(1,'Table_column',pair[0],True)
        df1.insert(2,'Column_column',pair[1],True)
        df1.columns =['Schema_column','Table_column','Column_column','Unique_Item','Groupby_Count']
        #print(f'''Table {pair[0]} and Column {pair[1]} Counts Group By Column Percentage == {pd.DataFrame(((df_dict[pair[0]][0].groupby(pair[1])[pair[1]].count()/df_dict[pair[0]][0][pair[1]].count())*100).reset_index(name='Groupby Count Percentage'))}''')
        df = pd.DataFrame(((df_dict[pair[0]][0].groupby(pair[1])[pair[1]].count()/df_dict[pair[0]][0][pair[1]].count())*100).reset_index(name='GroupbyCountPercentage'))
        #df = pd.DataFrame(((df_dict[pair[0]][0].pair[1].value_counts(dropna=False)/df_dict[pair[0]][0][pair[1]].fillna(-1).count())*100).reset_index(name='GroupbyCountPercentage'))
        df.insert(0,'Schema_column',schema,True)
        df.insert(1,'Table_column',pair[0],True)
        df.insert(2,'Column_column',pair[1],True)
        #df.insert(5,'Groupby_Counts',pd.DataFrame(df_dict[pair[0]][0].groupby(pair[1])[pair[1]].count().reset_index(name = 'GroupbyCount')),True)

        df.insert(4,'Groupby Count',df1['Groupby_Count'],True) 
        df.insert(5,'Object_Count',df_dict[pair[0]][0][pair[1]].count(),True)#GOING TO ADD GROUP BY COUNTS IN THE DATAFRAME... Create DF then merge into percentage
        df.columns =['Schema_column','Table_column','Column_column','Unique_Item','Groupby_Count','Object_Count','Groupby_Count_Percentage']#'Object_Count',
        #print(df)


        #both_list=['both']
        ##df_ljoin = df.merge(df1,on='Unique_Item',how='left',indicator=True)
        ##df_new = df_ljoin[['Schema_column_x','Table_column_x','Column_column_x','Unique_Item','Groupby_Count','Groupby_Count_Percentage','_merge']]
        ##df_new.columns =['Schema_column','Table_column','Column_column','Unique_Item','Groupby_Count','Groupby_Count_Percentage','Validation']
        #df_new[df_new['Validation'].isin(both_list)] #only allowing for both
        list_count_df.append(df)#df_new, df1
        #print(df_new)

        #print(f'''Table {pair[0]} and Column {pair[1]} Counts Group By Column 50%, 75% and 95% Quantile== {df_dict[pair[0]][0].groupby(pair[1])[pair[1]].count().quantile([.5,.75,.95])}''')
        df_quant =pd.DataFrame(df_dict[pair[0]][0].groupby(pair[1])[pair[1]].count().quantile([.5,.75,.95]).reset_index(name='GroupByQuantiles'))
        df_news = pd.DataFrame(df_dict[pair[0]][0].groupby(pair[1])[pair[1]].count().quantile([.5,.75,.95]))
        #print(df_news)
        df_quant.insert(0,'Schema_column',schema,True)
        df_quant.insert(1,'Table_column',pair[0],True)
        df_quant.insert(2,'Column_column',pair[1],True)
        df_quant.columns=['Schema_column','Table_column','Column_column','Quantile_50_75_95','Groupby_Count']
        df_quant['Groupby_Count']=df_quant['Groupby_Count'].fillna(0).astype(np.int64)
        df1['Groupby_Count']=df1['Groupby_Count'].fillna(0).astype(np.int64)
        #list_quant_df.append(df_quant)
        #print(df_quant)
        df_quant_join = df_quant.merge(df1, on=['Table_column','Column_column','Groupby_Count'],how='left', indicator=True)
        df_quant_join.drop_duplicates('Quantile_50_75_95',keep='first',inplace=True)
        df_new_quant=df_quant_join[['Schema_column_x','Table_column','Column_column','Unique_Item','Groupby_Count','Quantile_50_75_95','_merge']]
        df_new_quant.columns=['Schema_column','Table_column','Column_column','Unique_Value','Groupby_Count','Quantile_50_75_95','Validation']
        list_quant_df.append(df_quant)


        df_mean = pd.DataFrame(df_dict[pair[0]][0].groupby(pair[1])[pair[1]].count()).mean().to_frame().reset_index()
    
        df_mean.insert(0,'Schema_column',schema,True)
        df_mean.insert(1,'Table_column',pair[0],True)
        df_mean.columns =['Schema_column','Table_column','Column_column','Groupby_Count_Mean']
        list_stat_df.append(df_mean)
        #print(f'''Table {pair[0]} and Column {pair[1]} Counts Group By Column MEAN == {df_dict[pair[0]][0].groupby(pair[1])[pair[1]].count().mean()}''')

        #MEDIAN is NEXT

        #print(f'''Table {pair[0]} and Column {pair[1]} Counts Group By Column MEDIAN == {df_dict[pair[0]][0].groupby(pair[1])[pair[1]].count().median()}''')
        df_median = pd.DataFrame(df_dict[pair[0]][0].groupby(pair[1])[pair[1]].count()).median().to_frame().reset_index()
        df_median.insert(0,'Schema_column',schema,True)
        df_median.insert(1,'Table_column',pair[0],True)
        df_median.columns =['Schema_column','Table_column','Column_column','Median_Groupby_Count']
        df_median['Median_Groupby_Count']=df_median['Median_Groupby_Count'].fillna(0).astype(np.int64)
        #df1['Groupby_Count']=df1['Groupby_Count'].fillna(0).astype(np.int64)
        #df_median_join = df_median.merge(df1, on=['Table_column','Column_column','Groupby_Count'],how='left', indicator=True)
        #df_median_join.drop_duplicates('Groupby_Count',keep='first',inplace=True)
        #df_new_median=df_median_join[['Schema_column_x','Table_column','Column_column','Unique_Item','Groupby_Count','_merge']]
        #df_new_median.columns=['Schema_column','Table_column','Column_column','Unique_Value','Median_Groupby_Count','Validation']
        #print(df_new_median)
        list_median_df.append(df_median)
        #print(df_median)

        df_std =pd.DataFrame(df_dict[pair[0]][0].groupby(pair[1])[pair[1]].count()).std().reset_index(name='STD')
        df_std.insert(0,'Schema_column',schema,True)
        df_std.insert(1,'Table_column',pair[0],True)
        df_std.columns=['Schema_column','Table_column','Column_column','STD_Groupby_Count']
        list_std_df.append(df_std)
        #df_min_max=pd.DataFrame(df_dict[pair[0]][0].groupby(pair[1])[pair[1]].count().reset_index(name='Value')).agg({'count': ['mean','std','min', 'max']}).T
        df_min_max=pd.DataFrame(df_dict[pair[0]][0].groupby(pair[1])[pair[1]].count().reset_index(name='Value'))#.describe()#.agg({'Value': ['describe']}) #.reset_index(name='count')
        df_min_max.columns =['Unique_Value','Groupby_Count']
        #print(df_min_max)
        # this needs the min and max unique categorical values
        if len(df_min_max)>0:


            df_allofem=pd.DataFrame(df_min_max.describe(percentiles = perc, include = 'all',datetime_is_numeric=True)) #include
            df_allofem.insert(0,'Schema_column',schema,True)
            df_allofem.insert(1,'Table_column',pair[0],True)
            df_allofem.insert(2,'Column_column',pair[1],True)
            df_allofem.reset_index(inplace=True)
            df_allofem.rename(columns={'index':'Info'},inplace=True)
            df_info =df_allofem.pop('Info')
            df_allofem.insert(3,'Info',df_info)
            #print(df_allofem)
            list_min_max_df.append(df_allofem)

        df_max =pd.DataFrame(df_dict[pair[0]][0].groupby(pair[1])[pair[1]].count()).max().reset_index(name='MAX_column')
        df_max.insert(0,'Schema_column',schema,True)
        df_max.insert(1,'Table_column',pair[0],True)
        df_max.insert(2,'Column_column',pair[1],True)
        df_new_max =df_max.merge(df1, how='inner',left_on='MAX_column', right_on='Groupby_Count')
        df_max_context = df_new_max[['Schema_column_x','Table_column_x','Column_column_x','Unique_Item','MAX_column']]
        df_max_context.columns =['Schema_column','Table_column','Column_column','Item_Max','MAX_column']
        list_max.append(df_max)
        #print(df_max_context)

        df_min =pd.DataFrame(df_dict[pair[0]][0].groupby(pair[1])[pair[1]].count()).min().reset_index(name='MIN_column')
        df_min.insert(0,'Schema_column',schema,True)
        df_min.insert(1,'Table_column',pair[0],True)
        df_min.insert(2,'Column_column',pair[1],True)
        #df_mm_join = df_max.merge(df_min, on=['Schema_column','Table_column','Column_column'],how='left',indicator=True)
        #df_new_mm=df_mm_join[['Schema_column','Table_column','Column_column','MAX_column','MIN_column','_merge']]
        #print(df_new_mm)
        df_new_min =df_min.merge(df1, how='inner',left_on='MIN_column', right_on='Groupby_Count')
        df_min_context = df_new_min[['Schema_column_x','Table_column_x','Column_column_x','Unique_Item','MIN_column']]
        df_min_context.columns =['Schema_column','Table_column','Columnv','Item_Min','MIN_column']
        list_min.append(df_min)
    
        #df_diff = pd.concat([df_max_context,df_min_context]).drop_duplicates(keep=False)
        #print(df_diff)

        #print(df_min_context)
    mean_df = pd.concat(list_stat_df)
    count_df =pd.concat(list_count_df)
    count_df['Unique_Item'] =count_df['Unique_Item'].apply(str)
    ##quant_df =pd.concat(list_quant_df)
    median_df =pd.concat(list_median_df)
    ##median_df['Unique_Value'] =median_df['Unique_Value'].apply(str)
    median_df['Median_Groupby_Count'] =median_df['Median_Groupby_Count'].apply(str)
    std_df =pd.concat(list_std_df)
    min_df =pd.concat(list_min)
    #min_df['Item_Min'] = min_df['Item_Min'].apply(str)
    max_df =pd.concat(list_max)
    #max_df['Item_Max'] = max_df['Item_Max'].astype('str')
    #print(max_df.dtypes)
    ##max_df.columns=max_df.columns.str.upper()
    #print(max_df)
    #file_s ='/Users/tobiascaouette/Documents/Process_Validation/data_set_files_testing/percents.csv'
    #count_df.to_csv(file_s, sep='\t', encoding='utf-8') # investigate percent
    # need to change dtypes per column

    #cs_id.close()
    print('ALL DFs BUILT')
    print(count_df)
    df_count_mean = pd.merge(count_df,mean_df,left_on=['Schema_column','Table_column','Column_column'], right_on=['Schema_column','Table_column','Column_column'])
    df_count_median = pd.merge(df_count_mean,median_df, left_on=['Schema_column','Table_column','Column_column'], right_on=['Schema_column','Table_column','Column_column'])
    df_count_std = pd.merge(df_count_median,std_df, left_on=['Schema_column','Table_column','Column_column'], right_on=['Schema_column','Table_column','Column_column'])
    df_max_std = pd.merge(df_count_std, max_df, left_on=['Schema_column','Table_column','Column_column'], right_on=['Schema_column','Table_column','Column_column'])
    df_min_std = pd.merge(df_max_std, min_df, left_on=['Schema_column','Table_column','Column_column'], right_on=['Schema_column','Table_column','Column_column'])
    df_min_std.drop(['INDEX_X','INDEX_Y'], axis=1, inplace=True)


    #quant_df is huge leaving out for testing purposes.[mean_df, count_df,  median_df, std_df,  min_df, max_df]  
    return [mean_df, df_min_std,  median_df, std_df,  min_df, max_df]   




#role = role
#database = database
#schema = 'public'
#user = user
#ctx_id_new = snowflake.connector.connect(
#    user = user,
#    account = "om1id",
##    authenticator = 'externalbrowser',
#    role = role,
#    database = database,
#    schema = schema,
#    warehouse = 'LOAD_WH',
#    autocommit = False
#    )

#cs_id_new = ctx_id_new.cursor()

def get_col_types(df):
    
    '''
        Helper function to create/modify Snowflake tables; gets the column and dtype pair for each item in the dataframe

        
        args:
            df: dataframe to evaluate
            
    '''
        
    import numpy as np
        # Get dtypes and convert to df
    
    ct = df.dtypes.reset_index().rename(columns={0:'col'})
    ct = ct.apply(lambda x: x.astype(str).str.upper()) # case matching as snowflake needs it in uppers
        
    # only considers objects at this point
    # only considers objects and ints at this point
    ct['col'] = np.where(ct['col']=='OBJECT', 'NCHAR', ct['col'])#VARCHAR
    ct['col'] = np.where(ct['col'].str.contains('DATE'), 'DATETIME', ct['col'])
    ct['col'] = np.where(ct['col'].str.contains('INT'), 'VARCHAR', ct['col']) #NUMERIC
    ct['col'] = np.where(ct['col'].str.contains('FLOAT'), 'VARCHAR', ct['col']) #FLOAT
    ct['col'] = np.where(ct['col'].str.contains('CATEGORY'), 'VARCHAR', ct['col'])
    # get the column dtype pair
    l = []
    for index, row in ct.iterrows():
        l.append(row['index'] + ' ' + row['col'])
    
    string = ', '.join(l) # convert from list to a string object
    
    string = string.strip()
    
    return string

def create_table(table, action, col_type, df, cur):
    
    '''
        Function to create/replace and append to tables in Snowflake
        
        args:
            table: name of the table to create/modify
            action: whether do the initial create/replace or appending; key to control logic
            col_type: string with column name associated dtype, each pair separated by a comma; comes from get_col_types() func
            df: dataframe to load
            
        dependencies: function get_col_types(); helper function to get the col and dtypes to create a table
    '''
   
    if action=='create_replace':
    
        # set up execute
        sql=""" CREATE OR REPLACE TABLE """ + table.upper() +"""("""+ col_type + """)"""
             
        print(sql)
        cur.execute(sql) 
        time.sleep(5)
        #prep to ensure proper case
        df.columns = [col.upper() for col in df.columns]

        # write df to table
        #write_pandas(ctx_id_new, df, table.upper())
        
    #elif action=='append':
        
        # convert to a string list of tuples
    #    df = str(list(df.itertuples(index=False, name=None)))
        # get rid of the list elements so it is a string tuple list
    #    df = df.replace('[','').replace(']','')
        
        # set up execute
    #    cur.execute(
    #        """ INSERT INTO """ + table + """
    #            VALUES """ + df + """

    #        """)  
# new function call here for just output  
#build_big_df[0] = mean_df, build_big_df[1] = count_df, build_big_df[2]=quant_df, build_big_df[3]=median_df, build_big_df[4]=std_df


def send_df_snow(user,database,role,df_list,schema1,cs_id_new,schema):
    
    engine = create_engine(URL(
    account = 'om1id',
    user = user,
    authenticator = 'externalbrowser',
    database = database,
    schema = 'public',
    role=role,
    warehouse = 'LOAD_WH',
    autocommit = False,
    encoding="utf8"
    ))
    
    if_exists = 'replace'
    columnlist=[]
    table_name_list=[]
    for i in df_list: #this call back is causing the issue... data, frames are aleady built, so it's failing here FIX THIS
        columnlist.append(i.columns.to_list())
    print(columnlist)
    table_name_list = [f'QA_mean_values_{schema1}',f'QA_count_percent_values_{schema1}',f'QA_count_median_values_{schema1}',f'QA_std_values_{schema1}',f'QA_min_values_{schema1}',f'QA_max_values_{schema1}']
    print(table_name_list)

    #add datetime stamp to run -------> this needs to be done
    #this is out of order
    list_dict = {}
    for k, v in zip(table_name_list,df_list):
        list_dict.setdefault(k, []).append(v) #extend
    for k,v in list_dict.items():
        print(v[0].dtypes)
    #print(list_dict)
    with engine.connect() as con:
        for k,v in list_dict.items():
            print(k , v)
            col_type = get_col_types(v[0])
            create_table(k, 'create_replace', col_type, v[0],cs_id_new)
            v[0].to_sql(name=k.lower(), con=con, if_exists=if_exists,index=False,chunksize=16000)
            print(f'{k} Sent to {database}.{schema}')







            # query = f'select * from {database}.{schema}.{table_name}'
            # print(query)
            # max = new_query(query,cs_id_new)
            # #print(max)
            # #ctx_id_new.close()
    print(f'CHECK QA TABLES in {database}.{schema} ')

    
        





#query = f'select * from {database}.{schema}.{table_name}'
#print(query)
##max = new_query(query,cs_id_new)
#print(max)
#ctx_id_new.close()
#print(f'CHECK {table_name} in {database}.{schema} ')




#table_df =pd.concat(list_table)
#print(table_df)
#------comaprative raw and transformed-- run stats---- df_raw minus df_tansformed 
#----- visulaize the counts ------
#------ next suite will be the implausible values ------



#count is the count of objects in the column --- does not include null
#unique is the count of unique items in the column
#top is the Max count in the column
#freq is the number of time the top appears in column


def main():
    #clean the code and add back the original percentage and quantiles... possibly min/max
    #cs_id, ctx_id,schema1,cs_id_new,ctx_id_new,schema,user,database,role
    cs_id, ctx_id,schema1,cs_id_new,ctx_id_new,schema,user,database,role = get_terminal() #new function for terminal input. 
    table_names = tables_schema(schema1,cs_id)
    df_dict, table_col_dict = rename_columns(table_names,cs_id)
    

    #build_big_df(df_dict,table_col_dict)
    df_list = build_big_df(df_dict,table_col_dict,schema1)
    cs_id.close()

    print('DATAFRAMES BUILT')
    # create df
    # now that the table is created, append to it

    #append_table('table_test', 'append', None, df2)

    send_df_snow(user,database,role,df_list,schema1,cs_id_new,schema)
    cs_id_new.close()

    print('CHECK DATABASE---FINISHED PROCESSING')
   
if __name__ == "__main__":
  main()





