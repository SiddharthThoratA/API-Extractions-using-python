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
    
    #call this function to combine orders csv files
    combined_order_files(output_file_path)
    
    #call this function to combine product csv files
    combined_product_files(output_file_path)
    
    #call this function to combine option items csv files
    combined_option_items_files(output_file_path)
    
    #call this function to get details of today's created and modified orders
    check_orders_count(output_file_path)

    print('state : complete')
    
def combined_order_files(output_file_path):

    #order_file_path = f'{output_file_path}/{run_date}/order/'
    order_file_path = f'{output_file_path}/order/'
    
    print("orders file's path: ",order_file_path)
    
    #order_csv_files = glob.glob(r'C:\Users\Siddharth\Downloads\mssc_orders\2023-07-08\*.{}'.format('csv'))
   
    order_csv_files = glob.glob(f'{order_file_path}*.csv')
    #print(order_csv_files)
    
    combined_orders_csv = pd.DataFrame()

    # append the CSV files
    for file in order_csv_files:
    # Now, the 'customer_no' column will be treated as a string, preserving the leading zeros
        df = pd.read_csv(file,sep='|',dtype={'order_customer_info.customer_no': str})
        combined_orders_csv = combined_orders_csv.append(df, ignore_index=True)

    orders_cols = ['order__type','order_adjusted_merchandize_total_tax','order_adjusted_shipping_total_tax','order_confirmation_status','order_created_by','order_creation_date','order_creation_pst_date','order_currency','order_export_status','order_last_modified','order_last_modified_pst_date','order_merchandize_total_tax','order_order_no','order_order_total','order_payment_status','order_product_sub_total','order_product_total','order_shipping_status','order_shipping_total','order_shipping_total_tax','order_site_id','order_status','order_taxation','order_tax_total','order_c_ATCustomsDuty','order_c_ATTax','order_c_mcOrderConfirmationStatusFailed','order_c_IPAddress','order_customer_info._type','order_customer_info.customer_id','order_customer_info.customer_no','customer_emails','order_notes._type','order_notes.link','file_name']
    
    dups_check_cols = ['order__type','order_adjusted_merchandize_total_tax','order_adjusted_shipping_total_tax','order_confirmation_status','order_created_by','order_currency','order_export_status','order_merchandize_total_tax','order_order_no','order_order_total','order_payment_status','order_product_sub_total','order_product_total','order_shipping_status','order_shipping_total','order_shipping_total_tax','order_site_id','order_status','order_taxation','order_tax_total','order_c_ATCustomsDuty','order_c_ATTax','order_c_mcOrderConfirmationStatusFailed','order_c_IPAddress','order_customer_info._type','order_customer_info.customer_id','order_customer_info.customer_no','customer_emails','order_notes._type','order_notes.link','file_name']

    combined_orders_csv['order_creation_pst_date']=pd.to_datetime(combined_orders_csv['order_creation_date'])
    combined_orders_csv['order_creation_pst_date']=combined_orders_csv['order_creation_pst_date'].dt.tz_convert(timezone('US/Pacific'))
    
    combined_orders_csv['order_last_modified_pst_date']=pd.to_datetime(combined_orders_csv['order_last_modified'])
    combined_orders_csv['order_last_modified_pst_date']=combined_orders_csv['order_last_modified_pst_date'].dt.tz_convert(timezone('US/Pacific'))
    
    combined_orders_csv['file_name'] = f'orders_data_{run_date}.csv'

    combined_orders_csv = combined_orders_csv.reindex(columns=orders_cols)

    combined_orders_csv = combined_orders_csv.drop_duplicates(subset=dups_check_cols,keep='first')
    combined_orders_csv.reset_index(drop=True, inplace=True)
    #df_csv_append.drop_duplicates()
    combined_orders_csv.to_csv(f'{order_file_path}/mssc_orders_data_{run_date}.csv',sep='|',index=False)


#Function helps us to combine all the csv file to one csv file for the product data
def combined_product_files(output_file_path):
   
    #product_file_path = f'{output_file_path}/{run_date}/product/'
    product_file_path = f'{output_file_path}/product/'
    print("products file's path: ",product_file_path)
    
    product_csv_files = glob.glob(f'{product_file_path}*.csv')
    
    #print(product_csv_files)
    
    combined_products_csv = pd.DataFrame()

    # append the CSV files
    for file in product_csv_files:
        df = pd.read_csv(file,sep='|')
        combined_products_csv = combined_products_csv.append(df, ignore_index=True)

    product_cols = ['order_order_no','product_item__type','product_item_adjusted_tax','product_item_base_price','product_item_bonus_product_line_item','product_item_gift','product_item_item_id','product_item_item_text','product_item_price','product_item_price_after_item_discount','product_item_price_after_order_discount','product_item_product_id','product_item_product_name','product_item_quantity','product_item_shipment_id','product_item_tax','product_item_tax_basis','product_item_tax_class_id','product_item_tax_rate','product_item_c_OptionType','product_item_c_Type','product_item_c_inStockDate','product_item_c_isBackorder','product_item_c_isRRP','product_item_c_msscBrand','product_item_c_personalizationJson','product_item_option_items','product_item_c_personalizationEligible','file_name']
    
    #Added below two rows just for temporary on 20231104.
    combined_products_csv = combined_products_csv.drop(columns=['product_item_c_personalizationJson'])
    combined_products_csv['product_item_c_personalizationJson'] = 'NAN'
    
    #Add file_name as new column and assinged value
    combined_products_csv['file_name'] = f'products_data_{run_date}.csv'
    
    #Arrange columns as per your table structure
    combined_products_csv = combined_products_csv.reindex(columns=product_cols)
   
    #remove record from the combined file if found blank value for product_item_product_id column
    combined_products_csv = combined_products_csv.dropna(subset=['product_item_product_id'])

    #Drop duplicate columns from the merged file and keep it first row only
    combined_products_csv = combined_products_csv.drop_duplicates(subset=product_cols,keep='first')
    
    combined_products_csv.reset_index(drop=True, inplace=True)
    
    #Create '|' delimited CSV file on particular path
    combined_products_csv.to_csv(f'{product_file_path}/mssc_products_data_{run_date}.csv',sep='|',index=False)


def combined_option_items_files(output_file_path):
    
    #option_items_file_path = f'{output_file_path}/{run_date}/option_items/'
    option_items_file_path = f'{output_file_path}/option_item/'
    
    print("option items file's path: ",option_items_file_path)
    
    option_item_csv_files = glob.glob(f'{option_items_file_path}*.csv')
    
    #option_item_csv_files = glob.glob(r'C:\Users\Siddharth\Downloads\mssc_option_items\2023-07-08\*.{}'.format('csv'))
    
    combined_option_item_csv = pd.DataFrame()

    # append the CSV files
    for file in option_item_csv_files:
        df = pd.read_csv(file,sep='|')
        combined_option_item_csv = combined_option_item_csv.append(df, ignore_index=True)

    option_items_cols = ['order_no','product_items_product_id','product_item_item_id','option_items__type', 'option_items_adjusted_tax','option_items_base_price', 'option_items_bonus_product_line_item','option_items_gift', 'option_items_item_id', 'option_items_item_text','option_items_option_id', 'option_items_option_value_id','option_items_price', 'option_items_price_after_item_discount','option_items_price_after_order_discount', 'option_items_product_id','option_items_product_name', 'option_items_quantity','option_items_shipment_id', 'option_items_tax',      'option_items_tax_basis', 'option_items_tax_class_id','option_items_tax_rate', 'option_items_c_OptionType','option_items_c_ProductReference', 'option_items_c_Type','file_name']
    
    combined_option_item_csv['file_name'] = f'option_items_data_{run_date}.csv'
    
    combined_option_item_csv = combined_option_item_csv.reindex(columns=option_items_cols)


    combined_option_item_csv = combined_option_item_csv.drop_duplicates(subset=option_items_cols,keep='first')
    combined_option_item_csv.reset_index(drop=True, inplace=True)
    #df_csv_append.drop_duplicates()
    combined_option_item_csv.to_csv(f'{option_items_file_path}/mssc_option_items_data_{run_date}.csv',sep='|',index=False)


def check_orders_count(output_file_path):
    
    orders_file = f'{output_file_path}/order/mssc_orders_data_{run_date}.csv'
    print('orders file path is :',orders_file)
    
    
    orders_df =  pd.read_csv(f'{orders_file}',sep='|')
    orders_counts = orders_df['order_order_no'].count()
    
    #Here taking total count of orders which are created and modified today
    print('total_orders_today :',orders_counts)
    
    #converting date columns into proper date formate yyyy-mm-dd
    orders_df["order_creation_date"] = pd.to_datetime(orders_df["order_creation_date"])
    orders_df["order_last_modified"] = pd.to_datetime(orders_df["order_last_modified"])
    
    #Filtering only those orders which are created today.
    created_orders = orders_df[orders_df["order_creation_date"].dt.date == pd.to_datetime(prev_dt).date()]
    print('orders_created_today :',len(created_orders))
    
    #Filtering only those orders which are modified today.
    modified_orders = orders_df[(orders_df["order_creation_date"].dt.date != pd.to_datetime(prev_dt).date()) & (orders_df["order_last_modified"].dt.date == pd.to_datetime(prev_dt).date())]
    print('orders_modified_today :',len(modified_orders))
    
    #Filtering total orders which are modified today.
    total_modified_orders = orders_df[(orders_df["order_creation_date"].dt.date <= pd.to_datetime(prev_dt).date()) & (orders_df["order_last_modified"].dt.date == pd.to_datetime(prev_dt).date())]
    print('total_modified :',len(total_modified_orders))
    

if __name__ == "__main__":
    main(sys.argv[1:])
