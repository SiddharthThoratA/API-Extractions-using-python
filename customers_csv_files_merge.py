#import libaries
import sys, getopt
import datetime
import csv
import gzip
import os
import psycopg2
import shutil
import requests
import json
import time
import pandas as pd
import numpy as np
import hashlib
from datetime import datetime, timedelta
import glob
from pytz import timezone

#run_date = datetime.datetime.now().strftime("%Y%m%d")
run_date = ''

"""
how to call this script :
python3 csv_files_merge.py  -d <run_dt> -c <config_file>
example :
python3 csv_files_merge.py  -d 20210517 -c mssc_config_params.txt
"""

def main(argv):

    # now = datetime.datetime.now()
    # print('current time :', datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    try:
        opts, args = getopt.getopt(argv, "hd:c:", ["date=", "config="])
    except getopt.GetoptError:
        print('csv_files_merge.py -c <config_file> -d <rundate>')
    for opt, arg in opts:
        if opt == '-h':
            print('csv_files_merge.py -c <config_file> -d <rundate>')
            sys.exit(2)
        elif opt in ('-d','date='):
            run_dt = arg
            print('rundate here :',run_dt)
        elif opt in ('-c', '--config'):
            config_file = arg
            if not config_file:
                print('Missing config file name --> syntax is : csv_files_merge.py -c <config_file> -d <rundate>')
                sys.exit()

    print('config file is :', config_file)
    print('run date is :', run_dt)
    
    #change date formate from 20230730 to 2023-07-30 and store it to the previous day variable
    date_object = datetime.strptime(run_dt, "%Y%m%d")
    prev_dt = date_object.strftime("%Y-%m-%d")
    
    print('prev_dt is: ',prev_dt)
    
    listOfGlobals = globals()
    listOfGlobals['run_date'] = run_dt
    listOfGlobals['prev_dt'] = prev_dt
    
    
    #print("Done!")
    read_config(config_file)

def read_config(config_file):
    conf_file = config_file
    fileobj = open(conf_file)
    params = {}
    for line in fileobj:
        line = line.strip()
        if not line.startswith('#'):
            conf_value = line.split('~')
            print('conf_value before: ',conf_value)
            if len(conf_value) == 2:
                params[conf_value[0].strip()] = conf_value[1].strip()
            print('conf_value after: ',conf_value)
    fileobj.close()

    #params.update(S3PATH = list_file)
    print(params)
    
    if params['OUT_FILE_LOC']:
        ofilepath = params['OUT_FILE_LOC']
        output_file_path = ofilepath.replace('RUN_DATE',run_date)
    
    print('Output file path is :',output_file_path)

    #ex_sql_file(params)
    
    #call this function to combine customers csv files
    combined_customers_files(output_file_path)
    
    #call this function to get details of today's created and modified orders
    check_customers_count(output_file_path)

    print('state : complete')

def combined_customers_files(output_file_path):

    #run_date = '20210407'
    #order_file_path = f'{output_file_path}/order/'
    
    customers_file_path = output_file_path
    
    print("customers file's path: ",customers_file_path)
    
    #order_csv_files = glob.glob(r'C:\Users\Siddharth\Downloads\mssc_orders\2023-07-08\*.{}'.format('csv'))
   
    customers_csv_files = glob.glob(f'{customers_file_path}/*.csv')
    #print(customers_csv_files)
    
    combined_customers_csv = pd.DataFrame()

    # append the CSV files
    for file in customers_csv_files:
    # Now, the 'customer_no' column will be treated as a string, preserving the leading zeros
        df = pd.read_csv(file,sep='|',dtype={'customer_no': str, 'phone_home' : str})
        combined_customers_csv = combined_customers_csv.append(df, ignore_index=True)

    customer_columns = ['customer_id','birthday','company_name','creation_date','creation_date_pst','customer_no','email','first_name','gender','last_login_time','last_login_time_pst','last_modified','last_modified_pst','last_name','last_visit_time','last_visit_time_pst','phone_business','phone_home','phone_mobile','previous_login_time','previous_login_time_pst','previous_visit_time','previous_visit_time_pst','c_cordialSubscribed','c_cordialList','c_cordialCID','c_subscribed','c_isLoyaltyMember','c_vipStatus','c_isLoyaltyProgramInitialized','c_aListStatus','c_freeEconomyShipping','c_pointsBalance','c_vipExpireStatus','c_vipExtensionStatus','c_vipPointsNeeded','c_aListMembershipTier','file_name']
    
    #convert creation_date into pst timezone
    combined_customers_csv['creation_date_pst']=pd.to_datetime(combined_customers_csv['creation_date'])
    combined_customers_csv['creation_date_pst']=combined_customers_csv['creation_date_pst'].dt.tz_convert(timezone('US/Pacific'))
    
    #convert last_login_time into pst timezone
    combined_customers_csv['last_login_time_pst']=pd.to_datetime(combined_customers_csv['last_login_time'])
    combined_customers_csv['last_login_time_pst']=combined_customers_csv['last_login_time_pst'].dt.tz_convert(timezone('US/Pacific'))
    
    #convert last_modified into pst timezone
    combined_customers_csv['last_modified_pst']=pd.to_datetime(combined_customers_csv['last_modified'])
    combined_customers_csv['last_modified_pst']=combined_customers_csv['last_modified_pst'].dt.tz_convert(timezone('US/Pacific'))
    
    #convert last_visit_time into pst timezone
    combined_customers_csv['last_visit_time_pst']=pd.to_datetime(combined_customers_csv['last_visit_time'])
    combined_customers_csv['last_visit_time_pst']=combined_customers_csv['last_visit_time_pst'].dt.tz_convert(timezone('US/Pacific'))
    
    #convert previous_login_time into pst timezone
    combined_customers_csv['previous_login_time_pst']=pd.to_datetime(combined_customers_csv['previous_login_time'])
    combined_customers_csv['previous_login_time_pst']=combined_customers_csv['previous_login_time_pst'].dt.tz_convert(timezone('US/Pacific'))
    
    #convert previous_visit_time into pst timezone
    combined_customers_csv['previous_visit_time_pst']=pd.to_datetime(combined_customers_csv['previous_visit_time'])
    combined_customers_csv['previous_visit_time_pst']=combined_customers_csv['previous_visit_time_pst'].dt.tz_convert(timezone('US/Pacific'))
    
    #combined_customers_csv['gender'].replace(-1, None, inplace=True)
    #combined_customers_csv['c_aListMembershipTier'].replace(-1, None, inplace=True)
    
    combined_customers_csv['file_name'] = f'customers_data_{run_date}.csv'
    #combined_customers_csv['file_name'] = f'customers_data_2024_04.csv'

    combined_customers_csv = combined_customers_csv.reindex(columns=customer_columns)

    combined_customers_csv = combined_customers_csv.drop_duplicates(subset=customer_columns,keep='first')
    combined_customers_csv.reset_index(drop=True, inplace=True)
    #df_csv_append.drop_duplicates()
    combined_customers_csv.to_csv(f'{customers_file_path}/mssc_customers_data_{run_date}.csv',sep='|',index=False)


def check_customers_count(output_file_path):
    
    customers_file = f'{output_file_path}/mssc_customers_data_{run_date}.csv'
    print('customers file path is :',customers_file)
    
    
    customers_df =  pd.read_csv(f'{customers_file}',sep='|')
    print(customers_df.head(5))
    customers_counts = customers_df['customer_no'].count()
    
    #Here taking total count of customers which are created and modified today
    print('Total_customers_today :',customers_counts)
    
    #converting date columns into proper date formate yyyy-mm-dd
    customers_df["creation_date"] = pd.to_datetime(customers_df["creation_date"])
    customers_df["last_modified"] = pd.to_datetime(customers_df["last_modified"])
    
    #Filtering only those orders which are created today.
    created_customers = customers_df[customers_df["creation_date"].dt.date == pd.to_datetime(prev_dt).date()]
    print('customers_created_today :',len(created_customers))
    
    #Filtering only those orders which are modified today.
    modified_customers = customers_df[(customers_df["creation_date"].dt.date != pd.to_datetime(prev_dt).date()) & (customers_df["last_modified"].dt.date == pd.to_datetime(prev_dt).date())]
    print('customers_modified_today :',len(modified_customers))


if __name__ == "__main__":
    main(sys.argv[1:])
