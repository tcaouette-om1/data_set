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


ctx_deid = snowflake.connector.connect(
    user='tcaouette',
    password='d0Nt8@ckD0WN!',
    account='om1'
    )

cs_deid = ctx_deid.cursor()

sql = f"""
    select * from research.bd_request.test_syndax_01

"""
def fetch_pandas_old(cur, sql):
    cur.execute(sql)
    rows = 0
    while True:
        dat = cur.fetchmany(50000)
        if not dat:
            break
        a = [cs_deid.description[i][0] for i in range(len(cs_deid.description))]
        df = pd.DataFrame(dat, columns=cur.description)
        rows += df.shape[0]
    return df


n=100000000

def fetch_pandas(cur, sql):
    cur.execute(sql)
    rows = 0
    while True:
        dat = cur.fetchmany(n)
        if not dat:
            break
        a = [cs_deid.description[i][0] for i in range(len(cs_deid.description))]
        df = pd.DataFrame(dat, columns=a)
        rows += df.shape[0]
        
    return df




fetch_pandas(cs_deid, sql)

df_all = fetch_pandas(cs_deid, sql)
print(df_all.head(50))

ctx_deid.close()


#ctx_id = snowflake.connector.connect(
#    user = 'tcaouette',
#    account = "om1id",
#    authenticator = 'externalbrowser',
#    role = 'ngr_reach',
#    database = 'ngr_reach',
#    warehouse = 'LOAD_WH',
#    autocommit = False
#    )

#cs_id = ctx_id.cursor()

base_sql = f"""
create or replace table test_syndax_01 (
FACILITY_ID varchar,
PATIENT_ID varchar,
ENCOUNTER_ID varchar,
DIAGNOSIS_ID varchar,
STANDARD_DIAGNOSIS_CODE	varchar,
STANDARD_DIAGNOSIS_CODE_TYPE varchar,
DIAGNOSIS_TYPE varchar,
PROBLEM_LIST_DIAGNOSIS_FLAG varchar,
PL_DIAGNOSIS_ONSET_DATE	varchar,
DIAGNOSIS_DTTM varchar,
PL_DIAGNOSIS_RESOLUTION_DATE varchar,
PL_DIAGNOSIS_STATUS varchar,
DIAGNOSIS_CREATED_DTTM varchar,
DIAGNOSIS_UPDATED_DTTM varchar
)

list @test_files
copy into test_syndax_01 from (
 select 
        t.$1 FACILITY_ID,
        t.$2 PATIENT_ID,
        t.$3 ENCOUNTER_ID,
        t.$4 DIAGNOSIS_ID,
        t.$5 STANDARD_DIAGNOSIS_CODE,
        t.$6 STANDARD_DIAGNOSIS_CODE_TYPE,
        t.$7 DIAGNOSIS_TYPE,
        t.$8 PROBLEM_LIST_DIAGNOSIS_FLAG,
        t.$9 PL_DIAGNOSIS_ONSET_DATE,
        t.$10 DIAGNOSIS_DTTM,
        t.$11 PL_DIAGNOSIS_RESOLUTION_DATE,
        t.$12 PL_DIAGNOSIS_STATUS,
        t.$13 DIAGNOSIS_CREATED_DTTM,
        t.$14 DIAGNOSIS_UPDATED_DTTM
 from @test_files/diagnosis.csv.gz t
 )
ON_ERROR = 'ABORT_STATEMENT'
;

select * from test_syndax_01;

alter table test_syndax_01 add column record_locator varchar;

update test_syndax_01 t1
        set record_locator = t2.file_row
        from (
        SELECT    
                METADATA$FILENAME AS FILE_NAME,
                METADATA$FILE_ROW_NUMBER  AS FILE_ROW_num,
                concat('@test_files/',file_name,'/row_num_',file_row_num) as file_row,
                t.$1, t.$2 patient_id,t.$3 ,t.$4,t.$5,t.$6,t.$7,t.$8,t.$9,t.$10,t.$11,t.$12,t.$13,t.$14,t.$15,t.$16,t.$17,t.$18,t.$19,t.$20
        FROM   @test_files/diagnosis t
        ) t2
        where t1.patient_id = t2.patient_id;
        
select * from test_syndax_01;


"""