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
'%Y-%m%-d %H:%M:%S'
dates = pd.date_range(end = datetime.today(), periods = 100).to_pydatetime().tolist()
"%d%B%Y"
lots_of_dates = [date_obj.strftime('%Y-%m-%d %H:%M:%S') for date_obj in dates]
print(lots_of_dates)

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


def randN_str(N,M,O):
    Nmin = pow(10, N-1)
    Nmax = pow(10, N) - 1
    Mmin = pow(10, M-1)
    Mmax = pow(10, M) - 1
    Omin = pow(10, O-1)
    Omax = pow(10, O) - 1
    cheese = str(random.randint(Nmin, Nmax)) +"-"+ str(random.randint(Mmin, Mmax)) +"-"+str(random.randint(Omin, Omax))
    return cheese

print(randN_str(12,6,1))

list_1=[]
for i in range(0,100):
    list_1.append(randN(9))
print(list_1)

list_2=[]
for i in range(0,100):
    list_2.append(randN_str(9,6,5))
print(list_2)


list_3=[]
for i in range(0,100):
    list_3.append(randN_strN(10))
print(list_3)

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

def update_diagnosis(df_list):
    df_diagnosis = df_list[0]
    return df_diagnosis
df = update_diagnosis(df_list)
print(df.head())
