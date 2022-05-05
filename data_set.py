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
list_phys_orderid = list_1(100,5)


list_2=[]
for i in range(0,100):
    list_2.append(randN_str3(12,6,1))
print(list_2)
list_med_ndc=[]
for i in range(0,100):
    list_med_ndc.append(randN_str3(4,4,2))
print(list_med_ndc)

list_imns=[]
for i in range(0,100):
    list_imns.append(randN_str3(7,3,0))
print(list_imns)

list_medad=[]
for i in range(0,100):
    list_medad.append(randN_str3(8,1,0))
print(list_medad)

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

list_phys_orderid = list_1(100,5)
list_encid = list_1(100,10)
list_facid = list_1(100,10)
list_insurid = list_1(100,12)
list_ptsid = list_3
list_digid = list_2
list_encprovid = list_providerid
list_immunid = list_imns
list_planid = list_1(100,6)
list_medadmin = list_medad
list_med_ndcid = list_med_ndc
list_procedureorid = list_1(100,10)
list_noteid = list_1(100,5)
list_resultid = list_1(100,9)


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

list_medad_orderid = [x[:-2] for x in list_medadmin]

def update_med_admin(F_ID,PTS_ID,ENC_ID,MEDAD_ID,MEDAD_OR_ID,ENC_DTTM,MED_NDC):
    column_headers =['FACILITY_ID',	'PATIENT_ID','ENCOUNTER_ID','MEDICATION_ADMIN_ID','MED_ADMIN_ORDER_ID','MED_ADMIN_STATUS','MED_DESCRIPTION','MED_ADMIN_NDC',
    	'MEDICATION_ORDER_GPI',	'MEDICATION_ORDER_RXNORM',	'MED_ADMIN_ROUTE_OF_ADMINISTRATION','MED_ADMIN_PROVIDER_ID','MED_ADMIN_START_TIME',	'MED_ADMIN_CREATED_DTTM','MED_ADMIN_UPDATED_DTTM']
    df_encounter =pd.DataFrame(columns=column_headers)
    df_encounter['FACILITY_ID'] = F_ID
    df_encounter['PATIENT_ID'] = PTS_ID
    df_encounter['ENCOUNTER_ID'] = ENC_ID
    df_encounter['MEDICATION_ADMIN_ID'] = MEDAD_ID
    df_encounter['MED_ADMIN_ORDER_ID'] = MEDAD_OR_ID
    df_encounter['MED_ADMIN_STATUS'] = 'NEW BAG'
    df_encounter['MED_DESCRIPTION'] = 'INTRAVENOUS SOLUTION'
    df_encounter['MED_ADMIN_NDC'] = MED_NDC
    df_encounter['MED_ADMIN_ROUTE_OF_ADMINISTRATION'] = 'IV'
    df_encounter['MED_ADMIN_START_TIME'] = ENC_DTTM[0]
    df_encounter['MED_ADMIN_UPDATED_DTTM'] = ENC_DTTM[1]
    df_encounter =df_encounter[column_headers]
    return df_encounter
df_medad = update_med_admin(list_facid,list_ptsid,list_encid,list_medadmin,list_medad_orderid,bunch_of_dates,list_med_ndcid)
print(df_medad)

def update_med_order(F_ID,PTS_ID,ENC_ID,MEDAD_OR_ID,ENC_DTTM,PHYSOR_ID):
    column_headers =['FACILITY_ID',	'PATIENT_ID','ENCOUNTER_ID','MEDICATION_ORDER_ID',	'MEDICATION_ORDER_STATUS',	'MEDICATION_ORDER_NDC',	'MEDICATION_ORDER_GPI'
    'MEDICATION_ORDER_RXNORM',	'MEDICATION_ORDER_INDICATION','MEDICATION_ORDER_SIG','MEDICATION_ORDER_ROUTE_OF_ADMINISTRATION','MEDICATION_NAME',	'STRENGTH',
    'MED_ORDER_UNIT_DOSE',	'HV_DISCRETE_DOSE','HV_DISCR_FREQ_ID',	'MED_ORDER_INFUSION_DOSE',	'MED_ORDER_INFUSION_RATE',
    'MED_ORDER_TOTAL_DOSE',	'MEDICATION_ORDER_PRESCRIBING_PHYSICIAN_ID',	'MEDICATION_ORDER_REFILLS_AUTHORIZED',	'MEDICATION_ORDER_DAW',	'MED_ORDER_EXPIRE_DATE',
    'MED_ORDER_START_DATE','MED_ORDER_END_DATE',	'MED_ORDER_CREATED_DTTM',	'MED_ORDER_UPDATED_DTTM']
    df_encounter =pd.DataFrame(columns=column_headers)
    df_encounter['FACILITY_ID'] = F_ID
    df_encounter['PATIENT_ID'] = PTS_ID
    df_encounter['ENCOUNTER_ID'] = ENC_ID
    df_encounter['MED_ADMIN_ORDER_ID'] = MEDAD_OR_ID
    df_encounter['MEDICATION_ORDER_STATUS'] = 'COMPLETED'
    
    df_encounter['MEDICATION_ORDER_GPI'] = 132045
    df_encounter['MEDICATION_ORDER_ROUTE_OF_ADMINISTRATION'] = 'IV'
    df_encounter['MEDICATION_NAME'] = 'OXYTOCIN 10 UNIT/ML INJECTION SOLUTION'
    df_encounter['STRENGTH'] = '10 unit/mL'
    df_encounter['MED_ORDER_UNIT_DOSE'] = 'UNITS'
    df_encounter['HV_DISCRETE_DOSE'] = '10'
    df_encounter['HV_DISCR_FREQ_ID'] = 'once PRN'
    df_encounter['MED_ORDER_INFUSION_DOSE'] = 5
    df_encounter['MED_ORDER_TOTAL_DOSE'] = 1
    df_encounter['MEDICATION_ORDER_PRESCRIBING_PHYSICIAN_ID'] = PHYSOR_ID 

    df_encounter['MED_ORDER_START_DATE'] = ENC_DTTM[0]
    df_encounter['MED_ORDER_END_DATE'] = ENC_DTTM[0]
    df_encounter['MED_ORDER_CREATED_DTTM'] = ENC_DTTM[0]
    df_encounter['MED_ORDER_UPDATED_DTTM'] = ENC_DTTM[1]
    df_encounter =df_encounter[column_headers]
    return df_encounter
df_medor = update_med_order(list_facid,list_ptsid,list_encid,list_medad_orderid,bunch_of_dates,list_phys_orderid)
print(df_medor)


note_list =[r"Presents for 1st visit with reported h/o GCA with PMR-> follows with her Rheum in NY.Reportedly diagnosed with GCA 1 1/2 yrs ago when she presented with severe shoulder and hip girdle aching and could not get up from a seated position.Also reports that she had unusual scalp sensation to right side of head. Denies any visual changes, jaw claudication.Had TA biopsy-> reports negative.Currently on Prednisone 5mg daily,  and Actemra IV monthly (started approx Aug/Sept 2019; last dose Dec 9, 2019).Never tried sc Actemra. Recently stopped MTX 0.4cc sc weekly with folic acid 1mg dailyFeels some slight aching in her shoulders at present.Incoming records:Dec 2019H/H: 14.5/44, WBC 9.4, MCV 95, Plt 286BUN/creat 19/1.10, GFR 50ESR 2CRP <1June 2019TB quant negHep Bs Ag neg",
r"PRESCRIBE: predniSONE 5 mg oral tablet, Take 1 tab. PO daily, # 30, RF: 2. (Transmitted by XXX-FFFFFF Lumezanu, MD)[CR][LF][CR][LF]1. Temporal arteritis: diagnosed in 6/2016. Had HAs in the temporal area, PMR symptoms, fatigue, fevers. Had high ESR, CRP, +TA biopsy.[CR][LF]Started PND 60 mg PO daily on 6/20/2016 (took some steroids for 1 week before but unsure what dose).[CR][LF]Feels much better: no HAs, no PMR symptoms, no visual changes from baseline, no jaw claudication.[CR][LF]Complains of fatigue.[CR][LF]Labs from 7/9/2016: normal ESR of 1, CRP on 0.07.[CR][LF]Labs from 8/5/2016: normal ESR, CRP increased to 0.62 (normal is <0.5).[CR][LF]Labs from 9/20/2016: normal ESR, CRP.[CR][LF]Labs from 10/24/2016: normal ESR: 7, CRP minimally elevated to 0.58 (normal is <0.5).[CR][LF]Labs (11/28/2016): normal ESR, CRP at 5 and 0.23.[CR][LF]Decrease PND from 30 mg to 25 mg daily until next visit.[CR][LF]Continue ASA 81 mg PO daily.[CR][LF]Continue Ca and vitamin D 1 tab. PO BID.[CR][LF]Continue Fosamax 70 mg PO Q weekly, started it in 7/2016.[CR][LF]Consider MTX in the future if needed.[CR][LF][CR][LF]2. Headaches: caused by TA. Resolved with steroids.[CR][LF]S/P brain surgery (for meningioma in 2008).[CR][LF][CR][LF]3. Long term steroid use: continue Ca and vitamin D 1 tab. PO BID.[CR][LF]Continue Fosamax Q weekly.[CR][LF][CR][LF]F/U in 5 weeks. Get labs few days before next appt. I reviewed labs from 11/28/2016 with patient and his wife, copy provided to them.[CR][LF]Encouraged patient to start exercising.[CR][LF][CR][LF]PCP: Dr. XXX-FFFFFF XXX-FFFFFF[CR][LF]Referring physician: Dr. XXX-FFFFFF XXX-FFFFFF[CR][LF][CR][LF]ORDERED/ADVISED:  - C reactive protein,ESR  ICD Codes (Z51.81, R51, M31.6)[CR][LF][CR][LF]PROVIDED: Patient Education (12/1/2016)[CR][LF]**********[CR][LF]The following text was added by E. Lumezanu, MD on  Thursday December 1, 2016  10:35 PM:[CR][LF][CR][LF][CR][LF]Just before he left patient developed redness of the Rt eye, no eye pain.[CR][LF]Likely a small blood vessel ruptured.[CR][LF]I recommended to see an eye doctor if not better by tomorrow.[CR][LF][CR][LF]XXX-FFFFFF Lumezanu MD",
r"Headache is better but she is having a lot of side effects from the prednisone.  She is feeling weak.  She has fullness in the head.  She feels edematous.  Easy bruising.  No blurring in vision.Was seen by Dr. XXX-LLLLLL and XXX-LLLLLL recently.  She has dry eyes.  Treated with eyedrops by Dr. XXX-LLLLLL.  No new visual defect from her GCA noticed.She is gradually tapering the prednisone.  She is not on any antibiotics for prophylaxis.  Denies any fever or infection.  Actemra will be started tomorrow.Patient has sulfa allergies.  We will not start Bactrim.  Not interested in pentamidine inhaler.(New onset severe headaches this summer, never felt them before, associated feeling ill, in bed and felt horrible, headaches was persistent  and constant, over the top and sides of her head. In October, PCP suspected GCA. Associated jaw and jaw swelling. Puffy gums. No double vision or vision loss, no PMR symptoms, r temporal artery was engorged>lt, biopsy was consistent with GCA.Prednisone 60 mg daily in October which has helped, gradually tapered down to 30mg daily. Inflammation  markers dropped but now they are getting higher with prednisone. Prednisone causes agitation and nervousness and muscle weakens. Headache resolved. No strokes.)",
r"Patient originally developed and was diagnosed with PMR in 2010, when he presented with proximal myalgias/ stiffness, malaise/ fatigue, and elevated CRP/ESR. He then developed giant cell arteritis in 2012. While on a course of tapering steroids (was on Medrol 2 mg qd at the time), he developed severe headaches, worsening PMR symptoms, and ultimately, diplopia. He was hospitalized in August 2012 when he developed new-onset visual changes and given IV steroids. No visual loss occurred, and he has subsequently done well, although he remains steroid-dependent, unable to lower Medrol below 4 mg daily without flare of disease.",
r"He underwent a left temporal artery biopsy just over a week ago, which showed giant cell arteritis.  For 2 or 3 weeks prior to that, he had a prodrome of headache, which is unusual for him, visual disturbance, and neck pain.  He had lab work that showed elevated acute phase reactants.  He was admitted to the hospital on September 30 and had pulsed steroids for 3 days.  He was placed on prednisone 20 mg t.i.d. at discharge.  Because of significant steroid-related side effects, I spoke with his PCP last week, and suggested that he consolidate to 60 mg daily, which she has done.  Unfortunately, he continues to struggle with significant steroid-related side effects, including insomnia, irritability, peripheral edema, high blood pressure.  All his GCA symptoms have resolved.",
r"Diagnosed in April with GCA and on high dose prednisone. Presented with jaw pain and eventually visual disturbances. Pt also carries a diagnosis of PMR. Had a flare of jaw pain last week.",
r"PCP: Dr. XXX-FFFFFF XXX-LLLLLL.Self-payHx polymalgia rheumatica with giant cell arteritis (~2/2013: steroids concluded ~2/2014; resumed 8/2015): she reports a temporal artery biopsy was never performed and she was empirically treated as GCA for an additional year.  Represented 6/8/16 for concern of PMR recurrence: discomfort in the hips/lower back.+ANA (1:160 homo & speckled), +ds-DNA.4/20/16.  CRP 0.9 (<0.8),+ds-DNA 17(<4).Negative: ( ESR 15, TSH, RF, XXX-LLLLLL, SM/RNP, SSA, SSB, CCP). 6/8/16.  CRP 1 (<0.5), +ds-DNA18(<4), +ANA 1:160, homogenous and speckled.Negative: (Hepatitis B core antibody, hepatitis B surface antibody, hepatitis B surface antigen, CK, hepatitis C antibody, ESR 17, HLA-B27, C3, C4, RF, CCP). 8/31/16.  Negative: ( XXX-LLLLLL, SM/RNP).  CRP 1.1 (<0.5), ESR 28.  Past Rx: Celebrex (can`t take with steroids due to gastric ulcers), lumbar paraspinal trigger point CSI (helpful but she did not like procedure), methotrexate (initiated 9/21/16-12/7/16; recurring infections).Current Rx: Prednisone 8 mg daily (decreased from 10-9 mg daily 3/13/17; 8 mg 4/2016; 7 mg on 9/27/17; plan for 6 mg by 10/27/17), Advil (with Prilosec bid and Pepcid ac daily). Actemra weekly (initiated ~7/27/17).  -------Today's HPI:last visit was 9/27/17, was supposed to f/uShe was on actemra for 8 weeks.She had severe abdominal pain, rectal bleeding, went to XXX-LLLLLL ER. Had colonoscopy, dx w polyps.    Had colitis for a few weeks afterwards.  Was recommended pred 20 mg dairy but instead did 10 mg daily for a few months, then 9 mg daily without issue.  But started to get arthralgias at teh 8 mg daily dose.  no wback to 9 mg prednisone daily.small patch over the anterior r thigh, some overlying scales.  does not itch. unclear if psoraisis. she reports it tends to flare when the crp elevates. Then 3 months ago (~6/2018), started to develop pain in the r thumb pain, hands swelling, had been having swelling in the knees.  Difficulty holding pen in the r hand.  In feb had elevated of blood pressure, required 4 days of hospitalization to control.",
r"72-year-old female with diagnosis of polymyalgia rheumatica with giant cell arteritis diagnosed in January 2016.  This is a clinical diagnosis and patient has never had a tissue diagnosis.  My plan would be to start tapering her prednisone and is clinical symptoms or inflammatory lab parameters worsen then would consider a biopsy at that time.1) possible giant cell arteritis - clinical diagnosis- Continue with prednisone 5 mg daily- Additional laboratories today to include sedimentation rate, C-reactive protein, CBCWould recommend TA biopsy at this time considering her active symptomsMay also need CT angiography of arch2) Lumbar OA- following with Orthopedics3) Osteoporosis with vertebral fracture- Schedule DXA- Alendronate 70mg po qweekFollowup visit in 2 months",
r"She was diagnosed with GCA by a neurologist about 3-4 years ago - developed excrutiating HA's out of the blue  - severe pain and diagnosed with GCA - she had MRI and biopsy - biopsy was negative but she had been on prednisone for several months.  but reports that her HA's went completely away on the prednisone.  So over the last few years she has been up and down on the prednisone.  the lowest dose she has been on is 10mg daily.  So she switched PCP to Jencare about 2 years ago and sees PCP and Neuro there.  [CR][LF]So now she is having issues.[CR][LF]She is having a lot of hand shakes and feels shaky.  Legs are weak and shaky.  And wants to know why shaking.  [CR][LF]She was called a couple of weeks ago and told that her cardiac studies were poor and she needed to go down on the prednisone and get off of it.  [CR][LF]She was on 20mg and then now 10mg 3 months ago.  [CR][LF]",
r"the patient presents for her regularly scheduled visit. She has received the 6th of 6 intravenous Cytoxan infusions last week for her class IV glomerulonephritis secondary to systemic lupus erythematosus. the patient feels better in regards to the dehydration and hypotension she was experiencing at her last visit. She received a prescription for Imuran 500 mg 3 times a day instead of CellCept because she was told that she was allergic to CellCept in XXX-LLLLLL. She now complains of a two-week history of left knee pain and swelling. It is causing her difficulty ambulating and she cannot stand on her legs for more thn a few minutes at a time. She was going to buy a brace for her left knee but decided against it."
]
note_list10 =note_list*10

def update_note(PTS_ID,ENC_ID,PROCOR_ID,ENC_DTTM,NOTE_ID,RESULT_ID,NOTE_TEXT):
    column_headers =['PATIENT_ID','ENCOUNTER_ID','PROCEDURE_ORDER_ID',	'NOTE_ID',	'RESULT_LAB_ID',	'FILING_DATE',	'NOTE_DATE',	'ACTIVITY_DATE',	'DESCRIPTION',	'NOTE_TEXT']
    df_encounter =pd.DataFrame(columns=column_headers)
    df_encounter['PATIENT_ID'] = PTS_ID
    df_encounter['ENCOUNTER_ID'] = ENC_ID
    df_encounter['PROCEDURE_ORDER_ID'] = PROCOR_ID
    df_encounter['NOTE_ID'] = NOTE_ID
    df_encounter['RESULT_LAB_ID'] = RESULT_ID
    df_encounter['NOTE_DATE'] = ENC_DTTM[0]
    df_encounter['ACTIVITY_DATE'] = ENC_DTTM[1]
    df_encounter['NOTE_TEXT'] = NOTE_TEXT
    df_encounter =df_encounter[column_headers]
    return df_encounter
df_note = update_note(list_ptsid,list_encid,list_procedureorid ,bunch_of_dates,list_noteid,list_resultid,note_list10)
print(df_note)

def update_observations(F_ID,PTS_ID,ENC_ID,MEDAD_OR_ID,ENC_DTTM):
    column_headers =['FACILITY_ID',	'PATIENT_ID','ENCOUNTER_ID',	'OBSERVATION_ID',	'OBSERVATION_NOTE_FLOWSHEET_NAME',	'OBSERVATION_ATTRIBUTE_LOCAL_NAME',
    'STANDARD_ATTRIBUTE_CODE',	'OBSERVATION_ATTRIBUTE_VALUE',	'OBSERVATION_ATTRIBUTE_VALUE_UNIT',	'OBSERVATION_DTTM',	'OBSERVATION_PERFORMED_PROVIDER_ID',
    'OBSERVATION_CATEGORY',	'OBSERVATION_CREATED_DTTM',	'OBSERVATION_UPDATE_DTTM']
    df_encounter =pd.DataFrame(columns=column_headers)
    df_encounter['FACILITY_ID'] = F_ID
    df_encounter['PATIENT_ID'] = PTS_ID
    df_encounter['ENCOUNTER_ID'] = ENC_ID
    df_encounter['OBSERVATION_ID'] = MEDAD_OR_ID
    df_encounter['OBSERVATION_NOTE_FLOWSHEET_NAME'] = 'Encounter Vitals'
    
    df_encounter['OBSERVATION_ATTRIBUTE_LOCAL_NAME'] = 'Weight'
    df_encounter['STANDARD_ATTRIBUTE_CODE'] = 48867
    df_encounter['OBSERVATION_ATTRIBUTE_VALUE'] = 2100
    
    df_encounter['OBSERVATION_CATEGORY'] ='Enc Vitals'
    df_encounter['OBSERVATION_DTTM'] = ENC_DTTM[0]
    df_encounter['OBSERVATION_CREATED_DTTM'] = ENC_DTTM[0]
    df_encounter =df_encounter[column_headers]
    return df_encounter
df_observ = update_observations(list_facid,list_ptsid,list_encid,list_medad_orderid,bunch_of_dates)
print(df_observ)