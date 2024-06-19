#importing required libaries
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
#import datetime
from datetime import datetime, timedelta
from decimal import Decimal

#run_date = datetime.datetime.now().strftime("%Y%m%d")
run_date = ''

"""
how to call this script :
python3 mssc_api_extraction.py -d <rundate> -c <paramerters.txt>
example :
python3 mssc_api_extraction.py -d 20230730 -c parameters.txt
"""

def main(argv):
    try:
        opts, args = getopt.getopt(argv, "hd:c:", ["date=", "config="])
    except getopt.GetoptError:
        print('mssc_api_extraction.py -c <config_file> -d <rundate>')
    for opt, arg in opts:
        if opt == '-h':
            print('mssc_api_extraction.py -c <config_file> -d <rundate>')
            sys.exit(2)
        elif opt in ('-d','date='):
            run_dt = arg
            print('rundate here :',run_dt)
        elif opt in ('-c', '--config'):
            config_file = arg
            if not config_file:
                print('Missing config file name --> syntax is : mssc_api_extraction.py -c <config_file> -d <rundate>')
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
        
         #Create a below mentioned folders inside the created date folder
        folder_names = ['order','product','option_item','payment_details']
        file_paths = []
        for i in range(len(folder_names)):
            folder_path = os.path.join(ofilepath,folder_names[i])
            
            file_paths.append(folder_path)
     
            try:
                os.mkdir(folder_path)
            except OSError:
                print ("Creation of the directory %s failed" % folder_path)
            else:
                print ("Successfully created the directory %s " % folder_path)
        
    else:
        print('Missing output location path --- exiting')
        sys.exit()
        
    print("File paths: ", file_paths)
    
    #call this function to check total count on the given date  
    check_total_count(file_paths)
    
    #dates_generation(file_paths)
    
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
    
    payload = {}
    headers = {
    'Content-Type': f'{content_type}',
    'Authorization': f'Basic {auth}',
    'Cookie': f'{cookie}'
    }
    
    response = requests.request("POST", url, headers=headers, data=payload)
    access_token=response.json()['access_token']
    return access_token

#This function fetches data(on 'creation_date & last_modified') for a given date and time range(i.e 15 or 5 or 1 mins)  
def get_json_data(bearer_token,field,dates,s_time,e_time):
    if params['data_url']:
        api = params['data_url']
    
    url = f'{api}'
    #print('URL is :',url)

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
              "field": f"{field}",
              "from": f"{dates}T{s_time}.000Z",
              "to": f"{dates}T{e_time}.000Z"
            }
          },
          "query": {
            "match_all_query": {}
          }
        }
      },
      "count":200,
      "select": "(**)",
      "sorts": [
        {
          "field": "creation_date",
          "sort_order": "asc"
        }
      ]
    })
      
    response = requests.request("POST", url, headers=headers, data=payload)
    all_responses.append(response.json())

    return all_responses
    
    
#This function generates csv file of order and product related data 
def order_product_details(json_data,Ex_data_on,dates,s_time,e_time,file_paths):
    
    result1 = pd.DataFrame()

    for j in range(len(json_data[0]['hits'])):
        data1 = json.dumps(json_data[0]['hits'][j])
        data = json.loads(data1)

        df1 = pd.json_normalize(data['data'])
        df1 =  df1.add_prefix('order_')
        df1['key'] = 1

        df2 = pd.json_normalize(data['data']['payment_instruments'])
        df2 = df2.add_prefix('payment_inst_')
        df2['key'] = 1

        df3 = pd.json_normalize(data['data']['shipments'])
        df3 = df3.add_prefix('shipments_')  # Add prefix to columns
        df3['key'] = 1

        df4 = pd.json_normalize(data['data']['shipping_items'])
        df4 = df4.add_prefix('shipping_item_')  # Add prefix to columns
        df4['key'] = 1

        if 'price_adjustments' in data['data']['shipping_items'][0]:
            #print('yes price data there')
            df5 = pd.json_normalize(data['data']['shipping_items'][0]['price_adjustments'])
            df5 = df5.add_prefix('price_adjustments_')
        else:
            #print('NO data there')
            df5 = pd.DataFrame()
            #cols_list = ['price_adjustments__type','price_adjustments_creation_date','price_adjustments_custom','price_adjustments_item_text','price_adjustments_last_modified','price_adjustments_manual','price_adjustments_price','price_adjustments_price_adjustment_id','price_adjustments_promotion_id','price_adjustments_promotion_link','price_adjustments_applied_discount._type','price_adjustments_applied_discount.amount','price_adjustments_applied_discount.type']
            df5[['price_adjustments__type','price_adjustments_creation_date','price_adjustments_custom','price_adjustments_item_text','price_adjustments_last_modified','price_adjustments_manual','price_adjustments_price','price_adjustments_price_adjustment_id','price_adjustments_promotion_id','price_adjustments_promotion_link','price_adjustments_applied_discount._type','price_adjustments_applied_discount.amount','price_adjustments_applied_discount.type']] = pd.DataFrame([[np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan]])

        df5['key'] = 1

        #result_df6 = pd.DataFrame()
        if 'product_items' in (data['data']):
            for i in range(len(data['data']['product_items'])):
                df6 = pd.json_normalize(data['data']['product_items'][i])
                df6 = df6.add_prefix('product_item_')
                df6['key'] = 1

             #result_df6 = result_df6.append(df6)
            #result_df6.reset_index(drop=True, inplace=True)

                result = pd.merge(pd.merge(pd.merge(pd.merge(pd.merge(df1,df2,on='key'),df3 , on='key'),df4 , on='key'),df5, on='key'),df6, on='key')
                result1 = result1.append(result)
        else:
            print('No product_items data available')
            df6 = pd.DataFrame()
            df6[['product_item__type','product_item_adjusted_tax','product_item_base_price','product_item_bonus_product_line_item','product_item_gift', 'product_item_item_id', 'product_item_item_text','product_item_price', 'product_item_price_after_item_discount','product_item_price_after_order_discount', 'product_item_product_id', 'product_item_product_name', 'product_item_quantity','product_item_shipment_id', 'product_item_shipping_item_id','product_item_tax', 'product_item_tax_basis', 'product_item_tax_class_id', 'product_item_tax_rate','product_item_c_inStockDate', 'product_item_c_isBackorder','product_item_c_personalizationJson']] =  pd.DataFrame([[np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan]])
            df6['key'] = 1

            result = pd.merge(pd.merge(pd.merge(pd.merge(pd.merge(df1,df2,on='key'),df3 , on='key'),df4 , on='key'),df5, on='key'),df6, on='key')
            result1 = result1.append(result)
    
    s_time = s_time.replace(':','_')
    e_time = e_time.replace(':','_')
    
    order_file_path = file_paths[0]
    product_file_path = file_paths[1]
    
    #print('order file path_is :',order_file_path)
    #print('product file path is :',product_file_path)

    drop_cols = ['order_customer_name','order_order_token','order_billing_address.address1','order_billing_address.first_name','order_billing_address.full_name','order_billing_address.id','order_billing_address.last_name','order_billing_address.phone','order_customer_info.customer_name','order_customer_info.email','payment_inst_payment_instrument_id','payment_inst_c_customerPaymentProfileId','payment_inst_c_customerProfileId','payment_inst_payment_card._type','payment_inst_payment_card.card_type','payment_inst_payment_card.credit_card_expired','payment_inst_payment_card.credit_card_token','payment_inst_payment_card.expiration_month','payment_inst_payment_card.expiration_year','payment_inst_payment_card.holder','payment_inst_payment_card.masked_number','payment_inst_payment_card.number_last_digits','shipments_shipping_address.address1','shipments_shipping_address.first_name','shipments_shipping_address.full_name','shipments_shipping_address.id','shipments_shipping_address.last_name','shipments_shipping_address.phone']
    result1 = result1.drop(['order_payment_instruments','order_shipments','order_shipping_items','shipping_item_price_adjustments','order_product_items','order_c_ATTaxDetail','key'],axis=1,errors="ignore")
    result1['customer_emails'] = result1['order_customer_info.email'].apply(lambda x : hashlib.sha256(str(x).encode()).hexdigest())
    result1 = result1.drop(drop_cols,axis=1,errors="ignore")
    result1.reset_index(drop=True, inplace=True)
    
    #########################################################################################################################
    # Orders and prodcut related columns only
    result1 = result1.reindex(columns=['order__type','order_adjusted_merchandize_total_tax','order_adjusted_shipping_total_tax','order_confirmation_status','order_created_by','order_creation_date','order_currency','order_export_status','order_last_modified','order_merchandize_total_tax','order_order_no','order_order_total','order_payment_status','order_product_sub_total','order_product_total','order_shipping_status','order_shipping_total','order_shipping_total_tax','order_site_id','order_status','order_taxation','order_tax_total','order_c_ATCustomsDuty','order_c_ATTax','order_c_mcOrderConfirmationStatusFailed','order_c_IPAddress','order_customer_info._type','order_customer_info.customer_id','order_customer_info.customer_no','customer_emails','order_notes._type','order_notes.link','product_item__type','product_item_adjusted_tax','product_item_base_price','product_item_bonus_product_line_item','product_item_gift','product_item_item_id','product_item_item_text','product_item_price','product_item_price_after_item_discount','product_item_price_after_order_discount','product_item_product_id','product_item_product_name','product_item_quantity','product_item_shipment_id','product_item_tax','product_item_tax_basis','product_item_tax_class_id','product_item_tax_rate','product_item_c_OptionType','product_item_c_Type','product_item_c_inStockDate','product_item_c_isBackorder','product_item_c_isRRP','product_item_c_msscBrand','product_item_c_personalizationJson','product_item_option_items','product_item_c_personalizationEligible'])
    #result1.to_csv(f'C:\\Users\\Siddharth\\Downloads\\mssc_orders_products_details_{dates}.csv',sep='|',index = False)
    
    #########################################################################################################################
    # Orders related columns only
    orders_cols = ['order__type','order_adjusted_merchandize_total_tax','order_adjusted_shipping_total_tax','order_confirmation_status','order_created_by','order_creation_date','order_currency','order_export_status','order_last_modified','order_merchandize_total_tax','order_order_no','order_order_total','order_payment_status','order_product_sub_total','order_product_total','order_shipping_status','order_shipping_total','order_shipping_total_tax','order_site_id','order_status','order_taxation','order_tax_total','order_c_ATCustomsDuty','order_c_ATTax','order_c_mcOrderConfirmationStatusFailed','order_c_IPAddress','order_customer_info._type','order_customer_info.customer_id','order_customer_info.customer_no','customer_emails','order_notes._type','order_notes.link']
    
    #New changed done on '2023-07-29'  
    orders_df = result1.reindex(columns=orders_cols)
    orders_df['order_customer_info.customer_no'] = orders_df['order_customer_info.customer_no'].astype(str)
    orders_df = orders_df.drop_duplicates(subset=orders_cols,keep='first')
    
    #Generate Orders CSV file on given path
    orders_df.to_csv(f'{order_file_path}/mssc_orders_on_{Ex_data_on}_{dates}_{s_time}_to_{e_time}.csv',sep='|',index = False)  
    
    #########################################################################################################################
    # Products related columns only
    products_cols = ['order_order_no','product_item__type','product_item_adjusted_tax','product_item_base_price','product_item_bonus_product_line_item','product_item_gift','product_item_item_id','product_item_item_text','product_item_price','product_item_price_after_item_discount','product_item_price_after_order_discount','product_item_product_id','product_item_product_name','product_item_quantity','product_item_shipment_id','product_item_tax','product_item_tax_basis','product_item_tax_class_id','product_item_tax_rate','product_item_c_OptionType','product_item_c_Type','product_item_c_inStockDate','product_item_c_isBackorder','product_item_c_isRRP','product_item_c_msscBrand','product_item_c_personalizationJson','product_item_option_items','product_item_c_personalizationEligible']
    products_df = result1.reindex(columns=products_cols)
    
    #Generate Products CSV file on given path
    products_df.to_csv(f'{product_file_path}/mssc_products_on_{Ex_data_on}_{dates}_{s_time}_to_{e_time}.csv',sep='|',index = False)
    
#This function generates csv file for product option items related data    
def product_option_items_details(json_data,Ex_data_on,dates,s_time,e_time,file_paths):
    
    option_items_df = pd.DataFrame()

    for j in range(len(json_data[0]['hits'])):
        data1 = json.dumps(json_data[0]['hits'][j])
        data = json.loads(data1)

        if 'product_items' in (data['data']):
            for item in data['data']['product_items']:
                if 'option_items' in item:
                    order_no = data['data']['order_no']
                    product_item_item_id = item['item_id']
                    product_items_product_id = item['product_id']
                    item_df = pd.json_normalize(item,record_path='option_items')
                    item_df = item_df.add_prefix('option_items_')
                    item_df['product_item_item_id'] = product_item_item_id
                    item_df['order_no'] = order_no
                    item_df['product_items_product_id'] = product_items_product_id
                    #New code is added 2023-07-30
                    item_df['option_items_tax_basis'] = item_df['option_items_tax_basis'].astype(str)

                    option_items_df = option_items_df.append(item_df)
                    #print(product_item_item_id)

                    #print(order_no)

                    #print(product_items_product_id)
                    #print("----------------------------------------------------")
        else:
            print('No Product_items available')

    s_time = s_time.replace(':','_')
    e_time = e_time.replace(':','_')
    
    option_item_file_path= file_paths[2]
    #print('option item file path_is :',option_item_file_path)
    
    #option_items_df = option_items_df.rename(columns={'_type':'option_items_type'},errors="ignore")
    option_items_df = option_items_df.reindex(columns=['order_no','product_items_product_id','product_item_item_id','option_items__type', 'option_items_adjusted_tax','option_items_base_price', 'option_items_bonus_product_line_item','option_items_gift', 'option_items_item_id', 'option_items_item_text','option_items_option_id', 'option_items_option_value_id','option_items_price', 'option_items_price_after_item_discount','option_items_price_after_order_discount', 'option_items_product_id','option_items_product_name', 'option_items_quantity','option_items_shipment_id', 'option_items_tax','option_items_tax_basis', 'option_items_tax_class_id','option_items_tax_rate', 'option_items_c_OptionType','option_items_c_ProductReference', 'option_items_c_Type'])

    option_items_df.reset_index(drop=True, inplace=True)
    
    #Generate Option_items CSV file on given path
    option_items_df.to_csv(f'{option_item_file_path}/mssc_option_items_on_{Ex_data_on}_{dates}_{s_time}_to_{e_time}.csv',sep='|',index = False)
    

def get_payment_inst_data(json_data,Ex_data_on,dates,s_time,e_time,file_paths):
    
    payment_data = pd.DataFrame()

    for j in range(len(json_data[0]['hits'])):
        data1 = json.dumps(json_data[0]['hits'][j])
        data = json.loads(data1)

        df1 = pd.json_normalize(data['data'])
        df1 =  df1.add_prefix('order_')
        df1['key'] = 1

        df2 = pd.json_normalize(data['data']['payment_instruments'])
        df2 = df2.add_prefix('payment_inst_')
        df2['key'] = 1
        
        out_put = pd.merge(df1,df2,on='key')
        payment_data = payment_data.append(out_put)
        
           
    s_time = s_time.replace(':','_')
    e_time = e_time.replace(':','_')
    
    
    payment_details_file_path= file_paths[3]
    #all_payment_columns = ['order_order_no', 'payment_inst_amount','payment_inst_payment_instrument_id','payment_inst_payment_method_id','payment_inst_c_customerPaymentProfileId','payment_inst_c_customerProfileId', 'payment_inst_payment_card._type','payment_inst_payment_card.card_type','payment_inst_payment_card.credit_card_expired','payment_inst_payment_card.credit_card_token','payment_inst_payment_card.expiration_month','payment_inst_payment_card.expiration_year','payment_inst_payment_card.holder','payment_inst_payment_card.masked_number','payment_inst_payment_card.number_last_digits','payment_inst_masked_gift_certificate_code']
    
    payment_columns =  ['order_order_no', 'payment_inst_payment_instrument_id','payment_inst_amount', 'payment_inst_payment_method_id', 'payment_inst_c_customerProfileId', 'payment_inst_payment_card.card_type','payment_inst_payment_card.masked_number', 'payment_inst_masked_gift_certificate_code', 'file_name']
    
    drop_cols = ['order__type', 'order_adjusted_merchandize_total_tax', 'order_adjusted_shipping_total_tax', 'order_channel_type', 'order_confirmation_status', 'order_created_by', 'order_creation_date', 'order_currency', 'order_customer_name', 'order_export_status', 'order_last_modified', 'order_merchandize_total_tax', 'order_order_token', 'order_order_total', 'order_payment_instruments', 'order_payment_status', 'order_product_items', 'order_product_sub_total', 'order_product_total', 'order_shipments', 'order_shipping_items', 'order_shipping_status', 'order_shipping_total', 'order_shipping_total_tax', 'order_site_id', 'order_status', 'order_taxation', 'order_tax_total', 'order_c_ATCustomsDuty', 'order_c_ATTax', 'order_c_ATTaxDetail', 'order_c_Sift_OrderSent', 'order_c_mcOrderConfirmationStatusFailed', 'order_c_timeDownloaded', 'order_c_IPAddress', 'order_billing_address._type', 'order_billing_address.address1', 'order_billing_address.city', 'order_billing_address.country_code', 'order_billing_address.first_name', 'order_billing_address.full_name', 'order_billing_address.id', 'order_billing_address.last_name', 'order_billing_address.phone', 'order_billing_address.postal_code', 'order_billing_address.state_code', 'order_customer_info._type', 'order_customer_info.customer_id', 'order_customer_info.customer_name', 'order_customer_info.customer_no', 'order_customer_info.email', 'order_notes._type', 'order_notes.link','payment_inst__type','key']
    payment_data = payment_data.drop(drop_cols,axis=1,errors="ignore")
    
    payment_data = payment_data.reindex(columns=payment_columns)
    payment_data = payment_data.drop_duplicates(subset=payment_columns,keep='first')
    
    #Generate Option_items CSV file on given path
    payment_data.to_csv(f'{payment_details_file_path}/mssc_payment_details_on_{Ex_data_on}_{dates}_{s_time}_to_{e_time}.csv',sep='|',index = False)



#This function checks Total counts of given date,if count is > 0 then it will generate all the files.
#def check_total_count(file_paths):
def check_total_count(file_paths):
    print("checking for today's total orders")
    
    file_paths = file_paths
    
    curr_date = prev_dt
    #curr_date = dates
        
    print("***************************************************************")
    print('Date : ',curr_date)
    
    s_time = '00:00:00'
    e_time = '23:59:59'
    
    fetch_data = ['creation_date','last_modified']
    #fetch_data = ['creation_date']
    total_records = []
    for i in range(len(fetch_data)):
        #creation_date = fetch_data[i]
        #last_modified = fetch_data[i]
        #print(f'Extract data on {fetch_data[i]} start_time {s_time} and end_time {e_time}')
        Ex_data_on = fetch_data[i]
        #print(Ex_data)
        token=token_generate()
        time.sleep(2)
        json_data=get_json_data(token,Ex_data_on,curr_date,s_time,e_time)
        time.sleep(1)
    
        if json_data[0]['total'] != 0:
            Total_orders_count = json_data[0]['total']
            total_records.append(Total_orders_count)
            print(f'Total_records_on_{fetch_data[i]} : ',Total_orders_count)
            print("***************************************************************")
        else:
            Total_orders_count = json_data[0]['total']
            total_records.append(Total_orders_count)
            print(f'Total_records_on_{fetch_data[i]} : ',Total_orders_count)
            print("***************************************************************")
            print("There is no record today")
    
    total_counts = sum(total_records)
    print("Total_Number_of_records :", sum(total_records))
    
    if total_counts != 0:
        print('calling gereate all files function')
        #This function will be called if count is greater than 0.
        generate_all_files(file_paths)
    else:
        print("No records today!")

#This function calls all other functions        
def generate_all_files(file_paths):
    start_time = ['00:00:00', '00:15:00', '00:30:00', '00:45:00', '01:00:00', '01:15:00', '01:30:00', '01:45:00', '02:00:00', '02:15:00', '02:30:00', '02:45:00', '03:00:00', '03:15:00', '03:30:00', '03:45:00', '04:00:00', '04:15:00', '04:30:00', '04:45:00', '05:00:00', '05:15:00', '05:30:00', '05:45:00', '06:00:00', '06:15:00', '06:30:00', '06:45:00', '07:00:00', '07:15:00', '07:30:00', '07:45:00', '08:00:00', '08:15:00', '08:30:00', '08:45:00', '09:00:00', '09:15:00', '09:30:00', '09:45:00', '10:00:00', '10:15:00', '10:30:00', '10:45:00', '11:00:00', '11:15:00', '11:30:00', '11:45:00', '12:00:00', '12:15:00', '12:30:00', '12:45:00', '13:00:00', '13:15:00', '13:30:00', '13:45:00', '14:00:00', '14:15:00', '14:30:00', '14:45:00', '15:00:00', '15:15:00', '15:30:00', '15:45:00', '16:00:00', '16:15:00', '16:30:00', '16:45:00', '17:00:00', '17:15:00', '17:30:00', '17:45:00', '18:00:00', '18:15:00', '18:30:00', '18:45:00', '19:00:00', '19:15:00', '19:30:00', '19:45:00', '20:00:00', '20:15:00', '20:30:00', '20:45:00', '21:00:00', '21:15:00', '21:30:00', '21:45:00', '22:00:00', '22:15:00', '22:30:00', '22:45:00', '23:00:00', '23:15:00', '23:30:00', '23:45:00']
    
    #start_time = ['23:45:00']
    #end_time = ['23:59:59']
    
    end_time = ['00:15:00', '00:30:00', '00:45:00', '01:00:00', '01:15:00', '01:30:00', '01:45:00', '02:00:00', '02:15:00', '02:30:00', '02:45:00', '03:00:00', '03:15:00', '03:30:00', '03:45:00', '04:00:00', '04:15:00', '04:30:00', '04:45:00', '05:00:00', '05:15:00', '05:30:00', '05:45:00', '06:00:00', '06:15:00', '06:30:00', '06:45:00', '07:00:00', '07:15:00', '07:30:00', '07:45:00', '08:00:00', '08:15:00', '08:30:00', '08:45:00', '09:00:00', '09:15:00', '09:30:00', '09:45:00', '10:00:00', '10:15:00', '10:30:00', '10:45:00', '11:00:00', '11:15:00', '11:30:00', '11:45:00', '12:00:00', '12:15:00', '12:30:00', '12:45:00', '13:00:00', '13:15:00', '13:30:00', '13:45:00', '14:00:00', '14:15:00', '14:30:00', '14:45:00', '15:00:00', '15:15:00', '15:30:00', '15:45:00', '16:00:00', '16:15:00', '16:30:00', '16:45:00', '17:00:00', '17:15:00', '17:30:00', '17:45:00', '18:00:00', '18:15:00', '18:30:00', '18:45:00', '19:00:00', '19:15:00', '19:30:00', '19:45:00', '20:00:00', '20:15:00', '20:30:00', '20:45:00', '21:00:00', '21:15:00', '21:30:00', '21:45:00', '22:00:00', '22:15:00', '22:30:00', '22:45:00', '23:00:00', '23:15:00', '23:30:00', '23:45:00', '23:59:59']
    
    file_paths = file_paths 
    for i in range(len(start_time)):
        curr_date = prev_dt
        #curr_date = '2024-05-22'
        #curr_date = curr_date
        s_time = start_time[i]
        e_time = end_time[i]
        # print(f'start_time {s_time} and end_time {e_time}')
        
        #Extract data based on creation and modified date
        extraction_fields = ['creation_date','last_modified']
        #extraction_fields = ['creation_date']
        for i in range(len(extraction_fields)):
            Ex_data_on = extraction_fields[i]
            #Call token_generation function to generate token
            token = token_generate()
            time.sleep(1)
            #Call get_json_data function to get data based on extraction_fields,date and time intervals
            json_data = get_json_data(token, Ex_data_on, curr_date, s_time, e_time)
            time.sleep(1)
            total_records = json_data[0]['count']
            if total_records != 0:
                if total_records >= 200:
                    # Convert to 5-minute time frames if total_records are greater than 200
                    print(f"15 min Freq : More than 200 orders found on {Ex_data_on} between {s_time} and {e_time}")
                    
                    start_datetime = datetime.strptime(f"{curr_date} {s_time}", "%Y-%m-%d %H:%M:%S")
                    end_datetime = datetime.strptime(f"{curr_date} {e_time}", "%Y-%m-%d %H:%M:%S")
                    
                    #print('start_datetime :',start_datetime)
                    #print('end_datetime :',end_datetime)
                    
                    time_diff = (end_datetime - start_datetime).total_seconds()
                    #print('time_diff :',time_diff)
                    intervals = int(time_diff / 300)
                    
                    #print('intervals :',intervals)
                    
                    for j in range(intervals):
                        new_s_time = (start_datetime + timedelta(seconds=j * 300)).strftime("%H:%M:%S")
                        new_e_time = (start_datetime + timedelta(seconds=(j + 1) * 300)).strftime("%H:%M:%S")
                        token = token_generate()
                        time.sleep(1)
                        json_data = get_json_data(token, Ex_data_on, curr_date, new_s_time, new_e_time)
                        time.sleep(1)
                        total_records = json_data[0]['count']
                        if total_records != 0:
                            if total_records >= 200:
                                # Convert to 1-minute time frames
                                print(f"5 min Freq : More than 200 orders found on {Ex_data_on} between {new_s_time} and {new_e_time}")
                                
                                start_datetime_1min = datetime.strptime(f"{curr_date} {new_s_time}", "%Y-%m-%d %H:%M:%S")
                                end_datetime_1min = datetime.strptime(f"{curr_date} {new_e_time}", "%Y-%m-%d %H:%M:%S")
                                
                                time_diff_1min = (end_datetime_1min - start_datetime_1min).total_seconds()
                                intervals_1min = int(time_diff_1min / 60)
        
                                for k in range(intervals_1min):
                                    final_s_time = (start_datetime_1min + timedelta(seconds=k * 60)).strftime("%H:%M:%S")
                                    final_e_time = (start_datetime_1min + timedelta(seconds=(k + 1) * 60)).strftime("%H:%M:%S")
                                    token = token_generate()
                                    time.sleep(1)
                                    json_data = get_json_data(token, Ex_data_on, curr_date, final_s_time, final_e_time)
                                    time.sleep(1)

                                    total_records = json_data[0]['count']
                                    
                                    if total_records != 0:                                
                                        if total_records >= 200:
                                            # Convert to 1-second time frames if total_records are still greater than 200
                                            print(f"1 min Freq : More than 200 orders found on {Ex_data_on} between {final_s_time} and {final_e_time}")
                                            
                                            start_datetime_1sec = datetime.strptime(f"{curr_date} {final_s_time}", "%Y-%m-%d %H:%M:%S")
                                            end_datetime_1sec = datetime.strptime(f"{curr_date} {final_e_time}", "%Y-%m-%d %H:%M:%S")
                                            time_diff_1sec = (end_datetime_1sec - start_datetime_1sec).total_seconds()
                                            intervals_1sec = int(time_diff_1sec)

                                            for l in range(intervals_1sec):
                                                final_s_time_1sec = (start_datetime_1sec + timedelta(seconds=l)).strftime("%H:%M:%S")
                                                final_e_time_1sec = (start_datetime_1sec + timedelta(seconds=(l + 1))).strftime("%H:%M:%S")
                                                token = token_generate()
                                                time.sleep(1)
                                                json_data = get_json_data(token, Ex_data_on, curr_date, final_s_time_1sec, final_e_time_1sec)
                                                time.sleep(1)

                                                if json_data[0]['count'] != 0:
                                                    if json_data[0]['total'] > 200:
                                                        print(f"1 sec Freq : More than 200 orders found on {Ex_data_on} between {final_s_time_1sec} and {final_e_time_1sec}")
                                                        print(f"Data available on {Ex_data_on} {curr_date} between {final_s_time_1sec} to {final_e_time_1sec} and total Orders: {json_data[0]['total']}")
                                                        order_product_details(json_data, Ex_data_on, curr_date, final_s_time_1sec, final_e_time_1sec, file_paths)
                                                        time.sleep(1)
                                                        product_option_items_details(json_data, Ex_data_on, curr_date, final_s_time_1sec, final_e_time_1sec ,file_paths)
                                                        time.sleep(1)
                                                        get_payment_inst_data(json_data,Ex_data_on,curr_date,s_time,e_time,file_paths)
                                                        time.sleep(1)   
                                                    else:
                                                        print(f"Data available on {Ex_data_on} {curr_date} between {final_s_time_1sec} to {final_e_time_1sec} and total Orders: {json_data[0]['count']}")
                                                        order_product_details(json_data, Ex_data_on, curr_date, final_s_time_1sec, final_e_time_1sec, file_paths)
                                                        time.sleep(1)
                                                        product_option_items_details(json_data, Ex_data_on, curr_date, final_s_time_1sec, final_e_time_1sec ,file_paths)
                                                        time.sleep(1)
                                                        get_payment_inst_data(json_data,Ex_data_on,curr_date,s_time,e_time,file_paths)
                                                        time.sleep(1)                                                       
                                                        
                                                else:
                                                    (f"No data available on {Ex_data_on} {curr_date} between {final_s_time_1sec} to {final_e_time_1sec} and total Orders: {json_data[0]['count']}")

                                        else:
                                            print(f"Data available on {Ex_data_on} {curr_date} between {final_s_time} to {final_e_time} and total Orders: {total_records}")
                                            order_product_details(json_data, Ex_data_on, curr_date, final_s_time, final_e_time, file_paths)
                                            time.sleep(1)
                                            product_option_items_details(json_data, Ex_data_on, curr_date, final_s_time, final_e_time, file_paths)
                                            time.sleep(1)
                                            get_payment_inst_data(json_data,Ex_data_on,curr_date,s_time,e_time,file_paths)
                                            time.sleep(1)      
                                    else:
                                        print(f"No data available on {Ex_data_on} {curr_date} between {final_s_time} to {final_e_time} and total Orders: {total_records}")
                                            
                                    
                            else:
                                print(f"Data available on {Ex_data_on} {curr_date} between {new_s_time} to {new_e_time} and total Orders: {total_records}")
                                order_product_details(json_data, Ex_data_on, curr_date, new_s_time, new_e_time, file_paths)
                                time.sleep(1)
                                product_option_items_details(json_data, Ex_data_on, curr_date, new_s_time, new_e_time, file_paths)
                                time.sleep(1)
                                get_payment_inst_data(json_data,Ex_data_on,curr_date,s_time,e_time,file_paths)
                                time.sleep(1)
                        else:
                            print(f"No data available on {Ex_data_on} {curr_date} between {new_s_time} to {new_e_time} and total Orders: {total_records}")
                else:
                    print(f"Data available on {Ex_data_on} {curr_date} between {s_time} to {e_time} and total Orders: {total_records}")
                    order_product_details(json_data, Ex_data_on, curr_date, s_time, e_time, file_paths)
                    time.sleep(1)
                    product_option_items_details(json_data, Ex_data_on, curr_date, s_time, e_time, file_paths)
                    time.sleep(1)
                    get_payment_inst_data(json_data,Ex_data_on,curr_date,s_time,e_time,file_paths)
                    time.sleep(1)
            else:
                print(f"No data available on {Ex_data_on} {curr_date} between {s_time} to {e_time} and total Orders: {total_records}")

if __name__ == "__main__":
    main(sys.argv[1:])
   
