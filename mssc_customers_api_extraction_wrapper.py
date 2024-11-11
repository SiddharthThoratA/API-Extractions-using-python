########################################################################################################
#Purpose of the script : Main script to extract MSSC-Customers data by calling API from SFCC 		   #
# in JSON fromat and convert it into csv.							                	   			   #
#how to call this script :                                                     		  	   			   #
#sh mssc_customers_api_extraction_wrapper.sh > ${log_dir}/mssc_customers_api_extraction_wrapper.log    #
#  																						   			   #
#Created date: 2024-05-02 : Siddharth Thorat   				             	 		       			   #
#Updated on: 								                            	 		       			   #
########################################################################################################

import os
import time
from datetime import datetime, timedelta
import subprocess
import sys, getopt
import cx_Oracle
import csv
import gzip
import shutil
import re
import time
import glob
import boto3



# variables define
script_path = 'C:\\Users\\Siddharth\\Desoto\\my_task\\sprint_112\\mssc_python\\mssc_customers'
log_dir = f'{script_path}\\logs'
daily_files_path = f'{script_path}\\daily_files'
trigger_dir = f"{script_path}\\trigger_files"
#s3path = 's3://desototech/MSSC/daily_data'
s3path = 's3://desototech/DWH_team/sid/mssc/MSSC_PROD'
s3_bucket = 'desototech'
#trigger_s3 = 's3://desototech/MSSC/daily_script_triggers/'
trigger_s3 = 's3://desototech/DWH_team/sid/mssc/MSSC_PROD/daily_script_triggers/'



# Take previous date and current datetime 
prev_dt = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
#prev_dt = '20240630'
starttime = datetime.now().strftime('%F %T')

print('------------------------------------------------------------')
print('Previous date is :',prev_dt)
print('Start Date Time now is :',starttime)


log_file_email = f"{log_dir}\\email_notication_{prev_dt}.log"

#Go to script path and send start email
os.chdir(script_path)

email_start_subject = f"MSSC Customers(API) data extraction and load job started for {prev_dt}"
#print(email_start_subject)
email_start_body = f"""
Job mssc_customers_api_extraction_wrapper.sh for daily MSSC Customers(API) data extraction and load has started for the day - {prev_dt}

Start time : {starttime}
"""
#print(email_start_body)
config_file = "email_config.config"

# call python start email send script
with open(log_file_email, "a") as f:
    subprocess.run(["python",f"{script_path}\\email_notification.py", "--s",email_start_subject ,"--b",email_start_body, "--c", config_file], stdout=f)

#Call MSSC API extraction script
subprocess.run(f"python mssc_customers_api_extraction.py -d {prev_dt} -c mssc_customers_config_params.txt > {log_dir}\\mssc_customers_api_extraction_{prev_dt}.log", shell=True)

# Wait for completion
while True:
    with open(f"{log_dir}\\mssc_customers_api_extraction_{prev_dt}.log") as cust_api_ext:
        if 'complete' in cust_api_ext.read():
            print('------------------------------------------------------------')
            print("Extraction completed Successfully")
            print('Completion time :', datetime.now().strftime('%F %T'))
            break
    time.sleep(10)

# Get order counts from log
with open(f"{log_dir}\\mssc_customers_api_extraction_{prev_dt}.log") as log_file:
    log_data = log_file.read()

api_total_cust = int(log_data.split('Total_Number_of_records ')[1].split()[1])
api_created_cust = int(log_data.split('Total_records_on_creation_date ')[1].split()[1])
api_modified_cust = int(log_data.split('Total_records_on_last_modified ')[1].split()[1])

print('*******************************************************************')
print(f"Number of order created today: {api_created_cust}")
print(f"Number of order modified today: {api_modified_cust}")
print(f"Total Number of orders today: {api_total_cust}")


#check if there is any oreder available on the given date or not
if api_total_cust != 0:
    print("Yes!! customers data are there, let's proceed with further steps")
    subprocess.run(f"python customers_csv_files_merge.py -d {prev_dt} -c mssc_customers_config_params.txt > {log_dir}\\customers_csv_files_merge_{prev_dt}.log", shell=True)
    
    while True:
        with open(f"{log_dir}\\customers_csv_files_merge_{prev_dt}.log") as log_file:
            if 'complete' in log_file.read():
                print('------------------------------------------------------------')
                print("Combine csv files process completed Successfully")
                print('Completion time :', datetime.now().strftime('%F %T'))
                break
        time.sleep(10)
    
    with open(f"{log_dir}\\customers_csv_files_merge_{prev_dt}.log") as log_file:
        log_data = log_file.read()

    file_total_cust = int(log_data.split('Total_customers_today ')[1].split()[1])
    file_created_cust = int(log_data.split('customers_created_today ')[1].split()[1])
    file_modified_cust = int(log_data.split('customers_modified_today ')[1].split()[1])
   
    print('*******************************************************************')
    print(f"Total number of customers: {file_total_cust}")
    print(f"Total created customers: {file_created_cust}")
    print(f"Total modified customers: {file_modified_cust}")
    print('*******************************************************************')
    
    # Check if total created and modified counts are matched or not,if not matched than send notification
    if api_created_cust != file_created_cust:
        print("Sending Mismatched email")
        
        #send mismatch email if counts are not matched
        mismatched_log_file = f"{log_dir}\\mismatched_email_notication_{prev_dt}.log"

        email_start_subject = f"Alert! Mismatched found in MSSC Customers(API) data extraction - mssc_customers_api_extraction_wrapper.py for {prev_dt}"
        #print(email_start_subject)
        email_start_body = f"""
Mismatched found in MSSC Customers(API) data extraction for the day - {prev_dt}

Total Customers of API : {api_created_cust}
Total Customers of File : {file_created_cust}

Start time : {starttime}
completion time : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        """
        #print(email_start_body)
        with open(mismatched_log_file, "a") as f:
            subprocess.run(["python",f"{script_path}\\email_notification.py", "--s",email_start_subject ,"--b",email_start_body, "--c", config_file], stdout=f)
        
        # Exit the script after sending the email
        print("exit")
        exit(1)        

    else:
        # Creating list file
        with open(f"{script_path}\\csv_list_daily.lst", "w") as list_file:
            for file in glob.glob(f"{daily_files_path}\\{prev_dt}\\mssc_customers_data_*.csv"):
                list_file.write(f"{file}\n")
                        
        
        # Copy file to s3 and log the output
        with open(f"{script_path}\\csv_list_daily.lst", "r") as list_file:
            for line in list_file:
                line = line.strip()
                print(line)
                filename = os.path.basename(line)
                print(filename)
                foldername = os.path.basename(os.path.dirname(line))
                print(foldername)

                with open(os.path.join(log_dir, f"s3upload_daily_{prev_dt}.log"), "a") as log_file:
                    print(f"S3 file path: {s3path}/customers/{prev_dt}/")
                    print('\n')
                    subprocess.run(["aws", "s3", "cp", line, f"{s3path}/customers/{prev_dt}/"], stdout=log_file, stderr=subprocess.STDOUT, shell=True)
                    print('*******************************************************************')

        print("Data files copy to S3 completed!!")
        print('==================================================================================')
        
        
        email_start_subject = f"MSSC Customers(API) data extraction and load Job completed - mssc_customers_api_extraction_wrapper.sh for {prev_dt}"
        #print(email_start_subject)
        email_start_body = f"""
Job mssc_customers_api_extraction_wrapper.sh for daily MSSC Customers(API) data extraction and load has completed for the day - {prev_dt}

Customers created today : {file_created_cust}
Customers modified today : {file_modified_cust}
Total Customers today : {file_total_cust}


Start time : {starttime}
completion time : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        """
        # call python completion email script
        with open(log_file_email, "a") as f:
            subprocess.run(["python",f"{script_path}\\email_notification.py", "--s",email_start_subject ,"--b",email_start_body, "--c", config_file], stdout=f)
    

        with open(f"{log_dir}\\mssc_customer_data_extraction_completed_{prev_dt}.txt", "w") as completion_file:
            completion_file.write("Completion email sent")
        
else:
    print("No orders for: ",prev_dt)
    
    
    email_start_subject = f"MSSC Customers(API) data extraction and load Job completed - mssc_customers_api_extraction_wrapper.sh for {prev_dt}"
        #print(email_start_subject)
    email_start_body = f"""
Job mssc_customers_api_extraction_wrapper.sh for daily MSSC Customers(API) data extraction and load has completed for the day - {prev_dt}

Total number of orders are received today : {api_total_cust}

Start time : {starttime}
completion time : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        """
    # call python completion email script if no order is given date
    with open(log_file_email, "a") as f:
        subprocess.run(["python",f"{script_path}\\email_notification.py", "--s",email_start_subject ,"--b",email_start_body, "--c", config_file], stdout=f)

        
    with open(f"{log_dir}\\mssc_customer_data_extraction_completed_{prev_dt}.txt", "w") as completion_file:
        completion_file.write("Completion email sent")
        print('trigger file created and customers data is 0')
        

# Create trigger file and upload it on S3
trigger_file_path = f"C:\\Users\\Siddharth\\Desoto\\my_task\\sprint_112\\mssc_python\\mssc_customers\\trigger_files\\mssc_customer_trigger_{prev_dt}.txt"
#trigger_file_path = f"{trigger_dir}\mssc_etl_trigger_{prev_dt}.txt"
#completion_file_path = os.path.join(log_dir, f'mssc_customer_data_extraction_completed_{prev_dt}.txt')
completion_file_path = f"{log_dir}\\mssc_customer_data_extraction_completed_{prev_dt}.txt"

with open(completion_file_path, 'r') as completion_file:
    if "Completion email sent" in completion_file:
        with open(trigger_file_path, 'w') as trigger_file:
            trigger_file.write(str(api_total_cust))

        time.sleep(1)

        subprocess.run(["aws", "s3", "cp", trigger_file_path, f"{trigger_s3}"],shell=True)
        print(f'Trigger file is created on s3 : {trigger_s3}')

        #s3.upload_file(trigger_file_path, trigger_s3, os.path.basename(trigger_file_path))

        print("Script completed")
        print("==================================================================================")      


    
    
    

    
    
    
