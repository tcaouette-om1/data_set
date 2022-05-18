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


role = 'ngr_exact_sciences'
database = 'ngr_exact_sciences'

ctx_id = snowflake.connector.connect(
    user = 'tcaouette',
    account = "om1id",
    authenticator = 'externalbrowser',
    role = role,
    database = database,
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

sql_tables = f'''show tables in {schema} ''' 

df_tables = fetch_pandas_old(cs_id,sql_tables)
df_tables.columns =['created_on','table_name','database_name','schema_name','kind','comment',
'cluster_by','rows','bytes','owner','retention_time','auto_cluster','change_tracking','search_op',
'search_op_prog','search_op_bytes','is_external']

print(df_tables.head())

list_meta =[]
for col in df_tables.columns:
    list_meta.append(col)

print(list_meta)

print(df_tables.columns.to_list())