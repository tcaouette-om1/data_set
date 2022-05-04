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
date_1 = datetime.today()
end_date = date_1 + timedelta(days=10)

dates = pd.date_range(end = end_date, periods = 20).to_pydatetime().tolist()
print(dates)
def con_dates(num_dates):
    dates = pd.date_range(end = datetime.today(), periods = num_dates).to_pydatetime().tolist()
    lots_of_dates = [date_obj.strftime('%Y-%m-%d %H:%M:%S') for date_obj in dates]
    date_1 = datetime.today()
    end_date = date_1 + timedelta(days=10)
    dates = pd.date_range(end = end_date, periods = num_dates).to_pydatetime().tolist()
    lots_of_dates_add = [date_obj.strftime('%Y-%m-%d %H:%M:%S') for date_obj in dates]
    print(lots_of_dates)
    print(lots_of_dates_add)
    return lots_of_dates,lots_of_dates_add

bunch_of_dates = con_dates(100)
print(bunch_of_dates)

random.choice(string.ascii_uppercase + string.digits)

def randStr(chars = string.ascii_uppercase + string.digits, N=10):
	return ''.join(random.choice(chars) for _ in range(N))

print(randStr())

# default length(=10) random string
print(randStr())
# random string of length 7
print(randStr(N=7)) 
# random string with characters picked from ascii_lowercase
print(randStr(chars=string.ascii_lowercase))
# random string with characters picked from 'abcdef123456'
print(randStr(chars='abcdef123456'))



def randN(N):
	min = pow(10, N-1)
	max = pow(10, N) - 1
	return random.randint(min, max)

def randN_strN(N):
    min = pow(10, N-1)
    max = pow(10, N) - 1
    cake = randStr() +str(random.randint(min, max))
    hash_cake = hashlib.md5(cake.encode())
    return hash_cake.hexdigest()

def randN_strN_(N):
    min = pow(5, N-1)
    max = pow(5, N) - 1
    cake = randStr() +str(random.randint(min, max))
    return cake

def randN_str2(N,M):
    Nmin = pow(10, N-1)
    Nmax = pow(10, N) - 1
    Mmin = pow(10, M-1)
    Mmax = pow(10, M) - 1
    cheese2 = str(random.randint(Nmin, Nmax)) +"-"+ str(random.randint(Mmin, Mmax)) 
    return cheese2

def randN_str3(N,M,O):
    Nmin = pow(10, N-1)
    Nmax = pow(10, N) - 1
    Mmin = pow(10, M-1)
    Mmax = pow(10, M) - 1
    Omin = pow(10, O-1)
    Omax = pow(10, O) - 1
    if O > 0:
        cheese = str(random.randint(Nmin, Nmax)) +"-"+ str(random.randint(Mmin, Mmax)) +"-"+str(random.randint(Omin, Omax))
    else:
        cheese = str(random.randint(Nmin, Nmax)) +"-"+ str(random.randint(Mmin, Mmax))
    return cheese

print(randN_str3(12,6,1))
def list_1(y,z):
    list_1=[]
    for i in range(0,y):
        list_1.append(randN(z))
    return list_1

list_10 = list_1(100,10)
print('Testing list 10')
print(list_10)


list_2=[]
for i in range(0,100):
    list_2.append(randN_str3(12,6,1))
print(list_2)

list_imns=[]
for i in range(0,100):
    list_imns.append(randN_str3(7,3,0))
print(list_imns)

list_22=[]
for i in range(0,100):
    list_22.append(randN_str2(9,6))
print(list_22)

list_3=[]
for i in range(0,100):
    list_3.append(randN_strN(10))
print(list_3)
list_ptsid =list_3

ndigit=[]
for i in range(0,100):
    ndigit.append(randN_strN_(1))
print('PROVIDER ID')
print(ndigit)
list_providerid = ndigit

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
    df_list = [pd.read_csv(file) for file in list_of_files]
    return df_list

df_list = create_df(files)

print(df_list[0])

list_encid = list_1(100,10)
list_facid = list_1(100,10)
list_insurid = list_1(100,12)
list_ptsid = list_3
list_digid = list_2
list_encprovid = list_providerid
list_immunid = list_imns
list_planid = list_1(100,6)

def update_diagnosis(F_ID,PTS_ID,ENC_ID,DIG_ID,DIG_DTTM):
    column_headers =['FACILITY_ID',	'PATIENT_ID','ENCOUNTER_ID',	
    'DIAGNOSIS_ID','STANDARD_DIAGNOSIS_CODE','STANDARD_DIAGNOSIS_CODE_TYPE','DIAGNOSIS_TYPE','PROBLEM_LIST_DIAGNOSIS_FLAG'
    'PL_DIAGNOSIS_ONSET_DATE''DIAGNOSIS_DTTM','PL_DIAGNOSIS_RESOLUTION_DATE''PL_DIAGNOSIS_STATUS','DIAGNOSIS_CREATED_DTTM','DIAGNOSIS_UPDATED_DTTM']
    df_diagnosis =pd.DataFrame(columns=column_headers)
    df_diagnosis['FACILITY_ID']= F_ID
    df_diagnosis['PATIENT_ID'] = PTS_ID
    df_diagnosis['ENCOUNTER_ID'] = ENC_ID
    df_diagnosis['DIAGNOSIS_ID'] = DIG_ID
    df_diagnosis['STANDARD_DIAGNOSIS_CODE']=759
    df_diagnosis['STANDARD_DIAGNOSIS_CODE_TYPE']='Encounter DX'
    df_diagnosis['PROBLEM_LIST_DIAGNOSIS_FLAG']='N'
    df_diagnosis['DIAGNOSIS_DTTM']=DIG_DTTM[0]
    df_diagnosis['DIAGNOSIS_CREATED_DTTM']=DIG_DTTM[0]
    df_diagnosis['DIAGNOSIS_UPDATED_DTTM']=DIG_DTTM[0]
    df_diagnosis =df_diagnosis[column_headers]
    return df_diagnosis
df_dig = update_diagnosis(list_facid,list_ptsid,list_encid,list_digid,bunch_of_dates)
print(df_dig)

def update_encounter(F_ID,PTS_ID,ENC_ID,ENCPROV_ID,ENC_DTTM):
    column_headers =['FACILITY_ID','PATIENT_ID','ENCOUNTER_ID',	'ENCOUNTER_DTTM','ENCOUNTER_RENDERING_PROVIDERID','ENCOUNTER_CLASS','ENCOUNTER_TYPE'
    ,'ENCOUNTER_SERIES','ENCOUNTER_STATUS',	'ADMIT_DATE','DISCHARGE_DATE','ADMISSION_SOURCE','DISCHARGE_DISPOSITION','ENCOUNTER_CREATED_DTTM','ENCOUNTER_UPDATED_DTTM']
    df_encounter =pd.DataFrame(columns=column_headers)
    df_encounter['FACILITY_ID'] = F_ID
    df_encounter['PATIENT_ID'] = PTS_ID
    df_encounter['ENCOUNTER_ID'] = ENC_ID
    df_encounter['ENCOUNTER_DTTM'] = ENC_DTTM[0]
    df_encounter['ENCOUNTER_RENDERING_PROVIDERID'] = ENCPROV_ID
    df_encounter['ENCOUNTER_TYPE'] = 'BPA'
    df_encounter['ENCOUNTER_SERIES'] = 'one-time'
    df_encounter['ENCOUNTER_CREATED_DTTM'] = ENC_DTTM[0]
    df_encounter['ENCOUNTER_UPDATED_DTTM'] = ENC_DTTM[1]
    df_encounter =df_encounter[column_headers]
    return df_encounter
df_enc = update_encounter(list_facid,list_ptsid,list_encid,list_encprovid,bunch_of_dates)
print(df_enc)

def update_facility(F_ID):
    column_headers =['FACILITY_ID',	'FACILITY_NAME','LOC_ID','LOC_NAME']
    df_encounter =pd.DataFrame(columns=column_headers)
    df_encounter['FACILITY_ID'] = F_ID
    df_encounter['FACILITY_NAME'] = 'MTS_OBSERVATORY'
    df_encounter =df_encounter[column_headers]
    return df_encounter
df_fac = update_facility(list_facid)
print(df_fac)
#to do - relabel the data lists to reflect what they actually are. the ID's need to be consistent 
def update_immunization(F_ID,PTS_ID,ENC_ID,IMMUN_ID,ENC_DTTM,ENCPROV_ID):
    column_headers =['FACILITY_ID',	'PATIENT_ID','ENCOUNTER_ID','IMMUNIZATION_ID',	'IMMUNIZATION_TYPE','ADMINISTRATION_DATE','IMMUNIZATION_NAME','IMMUNIZATION_NDC','ADMINISTERING_PROVIDER_ID',
    'IMMUNIZATION_CREATED_DTTM','IMMUNIZATION_UPDATED_DTTM']
    df_encounter =pd.DataFrame(columns=column_headers)
    df_encounter['FACILITY_ID'] = F_ID
    df_encounter['PATIENT_ID'] = PTS_ID
    df_encounter['ENCOUNTER_ID'] = ENC_ID
    df_encounter['IMMUNIZATION_ID'] = IMMUN_ID
    df_encounter['IMMUNIZATION_TYPE'] = 'adm during visit'
    df_encounter['ADMINISTRATION_DATE'] = ENC_DTTM[0]
    df_encounter['IMMUNIZATION_NAME'] = 'PNEUMOCOCCAL CONJUGATE (PCV13) (PREVNAR 13)'
    df_encounter['ADMINISTERING_PROVIDER_ID'] = ENCPROV_ID
    df_encounter['IMMUNIZATION_CREATED_DTTM'] = ENC_DTTM[0]
    df_encounter['IMMUNIZATION_UPDATED_DTTM'] = ENC_DTTM[1]
    df_encounter =df_encounter[column_headers]
    return df_encounter
df_imn = update_immunization(list_facid,list_ptsid,list_encid,list_immunid,bunch_of_dates,list_encprovid)
print(df_imn)

def update_insurance(F_ID,PTS_ID,ENC_ID,INSUR_ID,ENC_DTTM,PLAN_ID):
    column_headers =['FACILITY_ID',	'PATIENT_ID','ENCOUNTER_ID','INSURANCE_ID',	'INSURANCE_COMPANY','INSURANCE_PLAN','FINANCIAL_CLASS',	'BENEFIT_PLAN_ID',	'EFF_DATE',	'TERM_DATE',
    	'INSURANCE_CREATED_DTTM','INSURANCE_UPDATED_DTTM']
    df_encounter =pd.DataFrame(columns=column_headers)
    df_encounter['FACILITY_ID'] = F_ID
    df_encounter['PATIENT_ID'] = PTS_ID
    df_encounter['ENCOUNTER_ID'] = ENC_ID
    df_encounter['INSURANCE_ID'] = INSUR_ID
    df_encounter['INSURANCE_COMPANY'] = 'HEALTHCARE'
    df_encounter['INSURANCE_PLAN'] = 'PPO'
    df_encounter['FINANCIAL_CLASS'] = 'MANAGED CARE'
    df_encounter['BENEFIT_PLAN_ID'] = PLAN_ID
    df_encounter['INSURANCE_UPDATED_DTTM'] = ENC_DTTM[1]
    df_encounter =df_encounter[column_headers]
    return df_encounter
df_insur = update_insurance(list_facid,list_ptsid,list_encid,list_insurid ,bunch_of_dates,list_planid)
print(df_insur)
#insurance