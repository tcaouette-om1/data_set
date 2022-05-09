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

dir = r"/Users/tobiascaouette/Documents/Process_Validation/TEST_FILES/"
def find_files(dir):
    list_files=[]
    for file in os.listdir(dir):
        if file.endswith(".csv"):
            list_files.append(os.path.join(dir, file))
            print(os.path.join(dir, file))
    return sorted(list_files)

files= find_files(dir)
print(files)

def create_df(list_of_files):
    df_list=[]
    for file in list_of_files:
        df_list.append(os.path.basename(file))
        df_list.append(pd.read_csv(file, index_col=0, nrows=0).columns.tolist()) 
    return df_list

df_list = create_df(files)

def table__and_columns(df_list):
    list_col=[]
    for i in range(1, len(df_list), 2):
        print(df_list[i], end = '  ')
        list_col.append(df_list[i])

    list_table=[]
    for i in range(0, len(df_list), 2):
        print(df_list[i], end = '  ')
        list_table.append(df_list[i])
    return list_table,list_col

lists=table__and_columns(df_list)
print("THIS IS THE NEW INFORMATION")
print(lists)
    

def create_ddl(role, schema_name,table_name, header_list):    
    """Create table's create DDL. Return string with create DDL"""
    columns = ""
    
    for item in header_list:
        if item != header_list[-1]:
            columns = columns + str(item) + " varchar" + ",\n\t"
        else:
            columns = columns + str(item) + " varchar"

    create_statement = f"""
    create or replace table {role}.{schema_name}.{table_name} (
    {columns}
    )
    ;\n"""
    
    return create_statement