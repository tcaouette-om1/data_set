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

    df_tables = new_query(cs_id,sql_tables)
    df_tables.columns =['created_on','table_name','database_name','schema_name','kind','comment',
    'cluster_by','rows','bytes','owner','retention_time','auto_cluster','change_tracking','search_op',
    'search_op_prog','search_op_bytes','is_external']

    table_names = df_tables['table_name'].to_list()
    return table_names    

# dynamically grab tables in schema, then each table into dataframe, column by column --- focus on diagnosis codes, patient age, other codes - length
# 