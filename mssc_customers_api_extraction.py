#importing required libaries
import sys, getopt
#import datetime
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
from decimal import Decimal

#run_date = datetime.datetime.now().strftime("%Y%m%d")
run_date = ''

"""
how to call this script :
python3 mssc_customers_api_extraction.py -d <rundate> -c <paramerters.txt>
example :
python3 mssc_customers_api_extraction.py -d 20230730 -c parameters.txt
"""

def main(argv):
    try:
        opts, args = getopt.getopt(argv, "hd:c:", ["date=", "config="])
    except getopt.GetoptError:
        print('mssc_customers_api_extraction.py -c <config_file> -d <rundate>')
    for opt, arg in opts:
        if opt == '-h':
            print('mssc_customers_api_extraction.py -c <config_file> -d <rundate>')
            sys.exit(2)
        elif opt in ('-d','date='):
            run_dt = arg
            print('rundate here :',run_dt)
        elif opt in ('-c', '--config'):
            config_file = arg
            if not config_file:
                print('Missing config file name --> syntax is : mssc_customers_api_extraction.py -c <config_file> -d <rundate>')
                sys.exit()

    print('config file is :', config_file)
    print('run date is :', run_dt)
    
    #change date formate from 20230730 to 2023-07-30 and store it to the previous day variable
    date_object = datetime.strptime(run_dt, "%Y%m%d")
    prev_dt = date_object.strftime("%Y-%m-%d")
    
    print('prev_dt is: ',prev_dt)

    #Make variable as global so it can be used any where in the script
    listOfGlobals = globals()
    listOfGlobals['run_date'] = run_dt

    listOfGlobals['prev_dt'] = prev_dt

    #print("Done!")
    
    #call function to extract values from the files 
    read_config(config_file)

def read_config(config_file):
    conf_file = config_file
    fileobj = open(conf_file)
    #Create a dictionary for parameters
    params = {}
    for line in fileobj:
        line = line.strip()
        if not line.startswith('#'):
            conf_value = line.split('~')
            if len(conf_value) == 2:
                params[conf_value[0].strip()] = conf_value[1].strip()
    fileobj.close()

    #params.update(AUTH_PARAMETERS = parameters_file)
    print('Authentication paramerters :',params)

    listOfGlobals = globals()
    listOfGlobals['params'] = params
    
    #call this function to use values from the created dictionary
    ex_param_file(params)

def ex_param_file(params):
    #print('inside ex_sql_file()')
        
    if params['OUT_FILE_LOC']:
        ofilepath = params['OUT_FILE_LOC'].strip()
        ofilepath = ofilepath.replace('RUN_DATE',run_date)
        print("ofilepath is - ",ofilepath)
        
        #create a date folder in given path
        try:
            os.mkdir(ofilepath)
        except OSError:
            print ("Creation of the directory %s failed" % ofilepath)
        else:
            print ("Successfully created the directory %s " % ofilepath)
        
    else:
        print('Missing output location path --- exiting')
        sys.exit()
            
    #call this function to check total count on the given date  
    check_total_count(ofilepath)
 
    print('state : complete')

#This function will return the access token
def token_generate():
    
    token_url = params['token_url'].strip()
    #print('token_url :', token_url)
    content_type = params['content-type'].strip()
    #print(content_type)
    auth = params['authorization'].strip()
    cookie = params['cookie'].strip()
    
    url = f'{token_url}'
    #url = "https://account.demandware.com/dw/oauth2/access_token?grant_type=client_credentials"
    
    payload = {}
    headers = {
    #'Content-Type': 'application/x-www-form-urlencoded',
    'Content-Type': f'{content_type}',
    #'Authorization': 'Basic MDc2NmZkM2QtODVhZS00YzVkLWFhYWEtZWEwMDgwMjZmZmZlOk9jYXBpQEp1bHkyMDIz',
    'Authorization': f'Basic {auth}',
    #'Cookie': 'X-Contour-Session-Affinity="ab8b4ea386bd2f0b"'
    'Cookie': f'{cookie}'
    }
    
    response = requests.request("POST", url, headers=headers, data=payload)
    access_token=response.json()['access_token']
    return access_token

#This function fetches data(on 'creation_date & last_modified') for a given date and time range(i.e 15 or 5 or 1 mins)  
def get_json_data(bearer_token,field,dates,s_time,e_time,start_num=0):
    #url = "https://staging-na01-spi.demandware.net/s/allheart/dw/shop/v20_10/order_search"
    if params['data_url']:
        api = params['data_url']
    
    #url = f'{api}'
    url = "https://www.allheart.com/s/-/dw/data/v21_6/customer_lists/allheart/customer_search?client_id=0766fd3d-85ae-4c5d-aaaa-ea008026fffe"

    headers = {
      'Content-Type': 'application/json',
      'Authorization': f'Bearer {bearer_token}'
    }

    all_responses = []
    payload = json.dumps({
      "query": {
        "filtered_query": {
          "filter": {
            "range_filter": {
              #"field": "creation_date",
              "field": f"{field}",
              "from": f"{dates}T{s_time}.000Z",
              #"from": f"2022-01-28T{s_time}.000Z",
              "to": f"{dates}T{e_time}.000Z"
            }
          },
          "query": {
            "match_all_query": {}
          }
        }
      },
      "select": "(**)",
      "sorts": [
        {
          "field": f"{field}",
          #"field": "creation_date",
          "sort_order": "asc"
        }
      ],
      "start": start_num,
      "count": 200
    })
      
    response = requests.request("POST", url, headers=headers, data=payload)
    all_responses.append(response.json())

    return all_responses

#This function generate customers csv file pagewise
def generate_customers_file(json_data, Ex_data_on, dates, s_time, e_time, file_paths, page_no=0):
    # List of desired columns
    desired_string_cols = ['customer_id', 'birthday', 'company_name', 'creation_date', 'customer_no', 'email', 'first_name', 'last_login_time', 'last_modified', 'last_name', 'last_visit_time', 'phone_business', 'phone_home', 'phone_mobile', 'previous_login_time', 'previous_visit_time', 'c_cordialSubscribed', 'c_cordialList', 'c_cordialCID', 'c_subscribed', 'c_isLoyaltyMember', 'c_vipStatus', 'c_aListStatus', 'c_freeEconomyShipping', 'c_vipExpireStatus', 'c_vipExtensionStatus','c_isLoyaltyProgramInitialized']
    
    gender_col = ['gender']
    
    desired_integers_cols = ['c_pointsBalance', 'c_vipPointsNeeded', 'c_aListMembershipTier']

    # Filter and transform the JSON data
    filtered_data = []

    for record in json_data[0]['hits']:
        filtered_record = {}
        
        # Handle string columns
        for column in desired_string_cols:
            filtered_record[column] = record['data'].get(column, '')
        
        # Handle Gender columns value
        for genders in gender_col:
            filtered_record[genders] = record['data'].get(genders, None)
            
        # Handle integer columns
        for column in desired_integers_cols:
            filtered_record[column] = record['data'].get(column,np.nan)
            
        #for column in desired_integers_cols:
        #    value = record['data'].get(column, '')
        #    filtered_record[column] = int(value) if value != '' else np.nan
        
        filtered_data.append(filtered_record)

    customers_df = pd.DataFrame(filtered_data)
    
    #customers_df['gender'] = customers_df['gender'].fillna(-1).astype(int)
    
    customer_columns = ['customer_id','birthday','company_name','creation_date','customer_no','email','first_name','gender','last_login_time','last_modified','last_name','last_visit_time','phone_business','phone_home','phone_mobile','previous_login_time','previous_visit_time','c_cordialSubscribed','c_cordialList','c_cordialCID','c_subscribed','c_isLoyaltyMember','c_vipStatus','c_isLoyaltyProgramInitialized','c_aListStatus','c_freeEconomyShipping','c_pointsBalance','c_vipExpireStatus','c_vipExtensionStatus','c_vipPointsNeeded','c_aListMembershipTier']
    
    customers_df = customers_df.reindex(columns=customer_columns)
    
    s_time = s_time.replace(':', '_')
    e_time = e_time.replace(':', '_')

    # Save the combined DataFrame to a CSV file
    customers_df.to_csv(f'{file_paths}/mssc_customers_on_{Ex_data_on}_{dates}_{s_time}_to_{e_time}_P{page_no}.csv', sep='|', index=False)

    return customers_df

#This function will generate list of counts in increments of 200,starting from 0 and counting until the provided number is reached.
def calculate_total_pages(number):
    increments = []
    current_increment = 0

    while current_increment < number:
        increments.append(current_increment)
        current_increment += 200

    return increments

#This function checks Total counts of given date,if count is > 0 then it will generate all the files.
def check_total_count(file_paths):
    print("checking for today's total orders")
    
    file_paths = file_paths
    
    curr_date = prev_dt
        
    print("***************************************************************")
    print('Date : ',curr_date)
    
    s_time = '00:00:00'
    e_time = '23:59:59'
    
    fetch_data = ['creation_date','last_modified']
    #fetch_data = ['creation_date']
    total_records = []
    for i in range(len(fetch_data)):
        Ex_data_on = fetch_data[i]
        #print(Ex_data)
        token=token_generate()
        time.sleep(2)
        json_data=get_json_data(token,Ex_data_on,curr_date,s_time,e_time)
        time.sleep(1)
    
        if json_data[0]['total'] != 0:
            Total_customers_count = json_data[0]['total']
            total_records.append(Total_customers_count)
            print(f'Total_records_on_{fetch_data[i]} : ',Total_customers_count)
            print("***************************************************************")
        else:
            Total_customers_count = json_data[0]['total']
            total_records.append(Total_customers_count)
            print(f'Total_records_on_{fetch_data[i]} : ',Total_customers_count)
            print("***************************************************************")
            print("There is no record today")
    
    total_counts = sum(total_records)
    print("Total_Number_of_records :", sum(total_records))
    
    if total_counts != 0:
        print('calling gereate all files function')
        
        #This function will be called if count is greater than 0.
        customer_json_data_ext(file_paths)
    else:
        print("No records today!")

#This function calls all other functions        
def customer_json_data_ext(file_path):
    
    start_time = ['00:00:00','01:00:00','02:00:00','03:00:00','04:00:00','05:00:00','06:00:00','07:00:00','08:00:00','09:00:00','10:00:00','11:00:00','12:00:00','13:00:00','14:00:00','15:00:00','16:00:00','17:00:00','18:00:00','19:00:00','20:00:00','21:00:00','22:00:00','23:00:00' ]
    #start_time = ['20:00:00','21:00:00','22:00:00','23:00:00']
    
    end_time = ['01:00:00','02:00:00','03:00:00','04:00:00','05:00:00','06:00:00','07:00:00','08:00:00','09:00:00','10:00:00','11:00:00','12:00:00','13:00:00','14:00:00','15:00:00','16:00:00','17:00:00','18:00:00','19:00:00','20:00:00','21:00:00','22:00:00','23:00:00','23:59:59']
    #end_time = ['21:00:00','22:00:00','23:00:00','23:59:59']
    
    file_paths = file_path
    
    for i in range(len(start_time)):
        curr_date = prev_dt
        #curr_date = '2023-11-30'
        #curr_date = '2021-04-07'
        s_time = start_time[i]
        e_time = end_time[i]
        #print(f'start_time {s_time} and end_time {e_time}')

        extraction_fields = ['creation_date','last_modified']
        #extraction_fields = ['creation_date']
        for i in range(len(extraction_fields)):
            Ex_data_on = extraction_fields[i]

            #Call token_generation function to generate token
            token=token_generate()
            time.sleep(1)

            #Call get_json_data function to get data based on extraction_fields,date and time intervals
            json_data=get_json_data(token,Ex_data_on,curr_date,s_time,e_time)
            time.sleep(2)

            total_records = json_data[0]['count']

            #checking total records for 1-hour frame
            if total_records != 0:
                if total_records >= 200:
                    #Convert to 30-minute time frames if total_records are greater than 200
                    print(f"1 hr Freq : More than 200 records on {Ex_data_on} {curr_date} between {s_time} to {e_time} and total count: {json_data[0]['total']}")
                    start_datetime = datetime.strptime(f"{curr_date} {s_time}", "%Y-%m-%d %H:%M:%S")
                    end_datetime = datetime.strptime(f"{curr_date} {e_time}", "%Y-%m-%d %H:%M:%S")
                    time_diff = (end_datetime - start_datetime).total_seconds()
                    print(time_diff)
                    intervals = int(time_diff / 1800)

                    for h in range(intervals):
                        new_s_time_30m = (start_datetime + timedelta(seconds=h * 1800)).strftime("%H:%M:%S")
                        new_e_time_30m = (start_datetime + timedelta(seconds=(h + 1) * 1800)).strftime("%H:%M:%S")

                        token = token_generate()
                        time.sleep(1)
                        json_data = get_json_data(token, Ex_data_on, curr_date, new_s_time_30m, new_e_time_30m)
                        time.sleep(1)
                        total_records = json_data[0]['count']

                        #checking total records for 30min frame
                        if total_records != 0:
                            if total_records >= 200:
                                print(f"30 min Freq : More than 200 records on {Ex_data_on} {curr_date} between {new_s_time_30m} to {new_e_time_30m} and total count: {json_data[0]['total']}")
                                new_s_time_15m = datetime.strptime(f"{curr_date} {new_s_time_30m}", "%Y-%m-%d %H:%M:%S")
                                new_e_time_15m = datetime.strptime(f"{curr_date} {new_e_time_30m}", "%Y-%m-%d %H:%M:%S")
                                time_diff_15m = (new_e_time_15m - new_s_time_15m).total_seconds()
                                print("time_diff_15m : ",time_diff_15m)
                                intervals_15m = int(time_diff_15m / 900)
                                print(intervals_15m)

                                for f in range(intervals_15m):
                                    new_s_time_15min = (new_s_time_15m + timedelta(seconds=f * 900)).strftime("%H:%M:%S")
                                    new_e_time_15min = (new_s_time_15m + timedelta(seconds=(f + 1) * 900)).strftime("%H:%M:%S")

                                    token = token_generate()
                                    time.sleep(1)
                                    json_data = get_json_data(token, Ex_data_on, curr_date, new_s_time_15min, new_e_time_15min)
                                    time.sleep(1)
                                    total_records = json_data[0]['count']

                                    #checking total records for 15min frame
                                    if total_records != 0:
                                        if total_records >= 200:
                                            print(f"15 min Freq : More than 200 records on {Ex_data_on} {curr_date} between {new_s_time_15min} to {new_e_time_15min} and total count: {json_data[0]['total']}")

                                            new_s_time_5m = datetime.strptime(f"{curr_date} {new_s_time_15min}", "%Y-%m-%d %H:%M:%S")
                                            new_e_time_5m = datetime.strptime(f"{curr_date} {new_e_time_15min}", "%Y-%m-%d %H:%M:%S")
                                            time_diff_5m = (new_e_time_5m - new_s_time_5m).total_seconds()
                                            print("time_diff_15m : ",time_diff_5m)
                                            intervals_5m = int(time_diff_5m / 300)
                                            print(intervals_5m)

                                            for j in range(intervals_5m):
                                                new_s_time_5min = (new_s_time_5m + timedelta(seconds=j * 300)).strftime("%H:%M:%S")
                                                new_e_time_5min = (new_s_time_5m + timedelta(seconds=(j + 1) * 300)).strftime("%H:%M:%S")

                                                token = token_generate()
                                                time.sleep(1)
                                                json_data = get_json_data(token, Ex_data_on, curr_date, new_s_time_5min, new_e_time_5min)
                                                time.sleep(1)
                                                total_records = json_data[0]['count']

                                                #checking total records for 5min frame
                                                if total_records != 0:
                                                    if total_records >= 200:
                                                        print(f"5 min Freq : More than 200 records on {Ex_data_on} {curr_date} between {new_s_time_5min} to {new_e_time_5min} and total count: {json_data[0]['total']}")

                                                        new_s_time_1m = datetime.strptime(f"{curr_date} {new_s_time_5min}", "%Y-%m-%d %H:%M:%S")
                                                        new_e_time_1m = datetime.strptime(f"{curr_date} {new_e_time_5min}", "%Y-%m-%d %H:%M:%S")
                                                        time_diff_1m = (new_e_time_1m - new_s_time_1m).total_seconds()
                                                        print("time_diff_1m : ",time_diff_1m)
                                                        intervals_1m = int(time_diff_1m / 60)
                                                        print(intervals_1m)

                                                        for k in range(intervals_1m):
                                                            new_s_time_1min = (new_s_time_1m + timedelta(seconds=k * 60)).strftime("%H:%M:%S")
                                                            new_e_time_1min = (new_s_time_1m + timedelta(seconds=(k + 1) * 60)).strftime("%H:%M:%S")

                                                            token = token_generate()
                                                            time.sleep(1)
                                                            json_data = get_json_data(token, Ex_data_on, curr_date, new_s_time_1min, new_e_time_1min)
                                                            time.sleep(1)
                                                            total_records = json_data[0]['count']

                                                            #checking total records for 1min frame
                                                            if total_records != 0:
                                                                if total_records >= 200:
                                                                    print(f"1 min Freq : More than 200 records on {Ex_data_on} {curr_date} between {new_s_time_1min} to {new_e_time_1min} and total count: {json_data[0]['total']}")

                                                                    new_s_time_1s = datetime.strptime(f"{curr_date} {new_s_time_1min}", "%Y-%m-%d %H:%M:%S")
                                                                    new_e_time_1s = datetime.strptime(f"{curr_date} {new_e_time_1min}", "%Y-%m-%d %H:%M:%S")
                                                                    time_diff_1s = (new_e_time_1s - new_s_time_1s).total_seconds()
                                                                    print("time_diff_1s : ",time_diff_1s)
                                                                    intervals_1s = int(time_diff_1s)
                                                                    print(intervals_1s)

                                                                    for l in range(intervals_1s):
                                                                        new_s_time_1sec = (new_s_time_1s + timedelta(seconds=l)).strftime("%H:%M:%S")
                                                                        new_e_time_1sec = (new_s_time_1s + timedelta(seconds=(l + 1))).strftime("%H:%M:%S")

                                                                        token = token_generate()
                                                                        time.sleep(1)
                                                                        json_data = get_json_data(token, Ex_data_on, curr_date, new_s_time_1sec, new_e_time_1sec)
                                                                        time.sleep(1)
                                                                        total_records = json_data[0]['count']

                                                                        #checking total records for 1sec frame
                                                                        if total_records != 0:
                                                                            if json_data[0]['total'] > 200:
                                                                                print(f"1 sec Freq : More than 200 records on {Ex_data_on} between {new_s_time_1sec} and {new_e_time_1sec}")
                                                                                print(f"Data available on {Ex_data_on} {curr_date} between {new_s_time_1sec} to {new_e_time_1sec} and total count: {json_data[0]['total']}")

                                                                                total_records_1sec = json_data[0]['total']

                                                                                sequence_numbers = calculate_total_pages(total_records_1sec)
                                                                                print(sequence_numbers)

                                                                                for n in range(len(sequence_numbers)):
                                                                                    print("start_number is :",sequence_numbers[n])

                                                                                    token = token_generate()
                                                                                    time.sleep(1)

                                                                                    json_data = get_json_data(token, Ex_data_on, curr_date, new_s_time_1sec, new_e_time_1sec, sequence_numbers[n])
                                                                                    time.sleep(1)

                                                                                    print(f"page_{n}: Data available on {Ex_data_on} {curr_date} between {new_s_time_1sec} to {new_e_time_1sec} and total count: {json_data[0]['count']}")
                                                                                    
                                                                                    page_number = n
                                                                                    
                                                                                    generate_customers_file(json_data, Ex_data_on, curr_date, new_s_time_1sec, new_e_time_1sec, file_paths,page_number)


                                                                            else:#for 1sec
                                                                                print(f"Data available on {Ex_data_on} {curr_date} between {new_s_time_1sec} to {new_e_time_1sec} and total Orders: {json_data[0]['count']}")
                                                                                generate_customers_file(json_data, Ex_data_on, curr_date, new_s_time_1sec, new_e_time_1sec, file_paths)
                                                                        else:#for 1sec
                                                                            print(f"No data available on {Ex_data_on} {curr_date} between {new_s_time_1sec} to {new_e_time_1sec} and total Orders: {json_data[0]['count']}")


                                                                else:#for 1min
                                                                    print(f"Data available on {Ex_data_on} {curr_date} between {new_s_time_1min} to {new_e_time_1min} and total Orders: {total_records}")
                                                                    generate_customers_file(json_data, Ex_data_on, curr_date, new_s_time_1min, new_e_time_1min, file_paths)

                                                            else:#for 1min
                                                                #total_records = json_data[0]['count']
                                                                print(f"No data available on {Ex_data_on} {curr_date} between {new_s_time_1min} to {new_e_time_1min} and total Orders: {total_records}")    



                                                    else:#for 5min
                                                        print(f"Data available on {Ex_data_on} {curr_date} between {new_s_time_5min} to {new_e_time_5min} and total Orders: {total_records}")
                                                        generate_customers_file(json_data, Ex_data_on, curr_date, new_s_time_5min, new_e_time_5min, file_paths)

                                                else:#for 5min
                                                    #total_records = json_data[0]['count']
                                                    print(f"No data available on {Ex_data_on} {curr_date} between {new_s_time_5min} to {new_e_time_5min} and total Orders: {total_records}")    



                                        else:#for 15min
                                            print(f"Data available on {Ex_data_on} {curr_date} between {new_s_time_15min} to {new_e_time_15min} and total Orders: {total_records}")
                                            generate_customers_file(json_data, Ex_data_on, curr_date, new_s_time_15min, new_e_time_15min, file_paths)

                                    else:#for 15min
                                        #total_records = json_data[0]['count']
                                        print(f"No data available on {Ex_data_on} {curr_date} between {new_s_time_15min} to {new_e_time_15min} and total Orders: {total_records}")    


                            else:#for 30min
                                print(f"Data available on {Ex_data_on} {curr_date} between {new_s_time_30m} to {new_e_time_30m} and total Orders: {total_records}")
                                generate_customers_file(json_data, Ex_data_on, curr_date, new_s_time_30m, new_e_time_30m, file_paths)

                        else:#for 30min
                            #total_records = json_data[0]['count']
                            print(f"No data available on {Ex_data_on} {curr_date} between {new_s_time_30m} to {new_e_time_30m} and total Orders: {total_records}")

                else:#for 1_hour
                    print(f"Data available on {Ex_data_on} {curr_date} between {s_time} to {e_time} and total Orders: {total_records}")
                    generate_customers_file(json_data, Ex_data_on, curr_date, s_time, e_time, file_paths)


            else:#for 1_hour
                #total_records = json_data[0]['count']
                print(f"No data available on {Ex_data_on} {curr_date} between {s_time} to {e_time} and total Orders: {total_records}")
    

if __name__ == "__main__":
    main(sys.argv[1:])
   
