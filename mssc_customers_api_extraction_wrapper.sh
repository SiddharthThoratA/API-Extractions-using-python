
########################################################################################################
#Purpose of the script : Main script to extract MSSC-Customers data by calling API from SFCC 		   #
# in JSON fromat and convert it into csv.							                	   			   #
#how to call this script :                                                     		  	   			   #
#sh mssc_customers_api_extraction_wrapper.sh > ${log_dir}/mssc_customers_api_extraction_wrapper.log    #
#  																						   			   #
#Created date: 2024-05-02 : Siddharth Thorat   				             	 		       			   #
#Updated on: 								                            	 		       			   #
########################################################################################################

#!/bin/bash

cd /home/dstdw/mssc/mssc_customers

#variable declaration
script_path=/home/dstdw/mssc/mssc_customers
d=`date +%Y%m%d%H%M%S`
log_dir=/home/dstdw/mssc/mssc_customers/logs
daily_files_path=/home/dstdw/mssc/mssc_customers/daily_files
s3path=s3://desototech/MSSC/daily_data
trigger_dir=/home/dstdw/mssc/mssc_customers/trigger_files
trigger_s3=s3://desototech/MSSC/daily_script_triggers/

#files_list_path=/home/siddharth/MSSC/daily_files

#run_dt=`date +%Y%m%d`
#run_dt='20230701'
prev_dt=$(date -d "-1 day" +%Y%m%d)
#prev_dt='20240401'



#echo "Run Date is : " ${run_dt}
echo "Previous date is : " ${prev_dt}

start_time=`date '+%F %T'`

echo "Start Time : "${start_time}

recipients="mprajapati@desototechnologies.com,dgoswami@desototechnologies.com,msshah@desototechnologies.com,kparate@desototechnologies.com,sthorat@desototechnologies.com,atopre@desototechnologies.com"

#recipients="sthorat@desototechnologies.com"

#send start email notification
/usr/sbin/sendmail ${recipients} <<MAIL_END
To: ${recipients}
Subject: MSSC Customers(API) data extraction and load job started for $prev_dt

Job mssc_customers_api_extraction_wrapper.sh for daily MSSC Customers(API) data extraction and load has started for the day - $prev_dt

Start time : `date '+%F %T'`

MAIL_END


#calling python to extract JSON data by API and convert it into csv file 
python3 mssc_customers_api_extraction.py -d ${prev_dt} -c mssc_customers_config_params.txt > ${log_dir}/mssc_customers_api_extraction_${prev_dt}.log &

#wait till python extraction gets completed
while true
do
s1=`grep complete ${log_dir}/mssc_customers_api_extraction_${prev_dt}.log |  awk '{ print $3 }'`

#echo "waiting for python scripts to get completed"

if [[ ${s1} == 'complete' ]]; then
        echo "extaction completed Successfully"
        echo 'completion time : ' `date '+%F %T'`
        break
else
        sleep 10
fi
done

#Get total number of orders, orders on creation date and modified date count from API  
api_total_cust=`grep Total_Number_of_records ${log_dir}/mssc_customers_api_extraction_${prev_dt}.log | awk '{print $3}'`

api_created_cust=`grep Total_records_on_creation_date ${log_dir}/mssc_customers_api_extraction_${prev_dt}.log | awk '{print $3}'`
api_modified_cust=`grep Total_records_on_last_modified ${log_dir}/mssc_customers_api_extraction_${prev_dt}.log | awk '{print $3}'`

echo "**************************************************************************"
echo "Number of customers created today : " ${api_created_cust}
echo "**************************************************************************"
echo "Number of customers modified today : " ${api_modified_cust}
echo "**************************************************************************"
echo "Total Number of customers today : " ${api_total_cust}
echo "**************************************************************************"

#proceed with further steps if count is greater than 0
if [[ ${api_total_cust} != '0' ]]; then
	echo "Yes!! customers data are there, let's proceed with further steps"
	
	#calling python to combined all csv files of respective date from daily_files folder.
	python3 customers_csv_files_merge.py -d ${prev_dt} -c  mssc_customers_config_params.txt > ${log_dir}/customers_csv_files_merge_${prev_dt}.log &
	
	#wait till python extraction gets completed
	while true
	do
	s2=`grep complete ${log_dir}/customers_csv_files_merge_${prev_dt}.log |  awk '{ print $3 }'`
	
	#echo "waiting for python scripts to get completed"		
	if [[ ${s2} == 'complete' ]]; then
			echo "combined files process completed Successfully"
			echo 'completion time : ' `date '+%F %T'`
			break
	else
			sleep 10
	fi
	done
	
	#Get the created, modified and total customers count from the csv file and store it to the variables	
	file_total_cust=`grep Total_customers_today ${log_dir}/customers_csv_files_merge_${prev_dt}.log | awk '{print $3}'`
	file_created_cust=`grep customers_created_today ${log_dir}/customers_csv_files_merge_${prev_dt}.log | awk '{print $3}'`
	file_modified_cust=`grep customers_modified_today ${log_dir}/customers_csv_files_merge_${prev_dt}.log | awk '{print $3}'`
	
	echo "**************************************************************************"
	echo "Total number of customers : " ${file_total_cust}
	echo "Total created customers : " ${file_created_cust}
	echo "Total modified customers : " ${file_modified_cust}
	
	
	# Check if 'customers_creation_count' and 'created_customers' counts are not matched
	if [ $api_created_cust -ne $file_created_cust ]; then
		echo "sending mismatched email"

#send mismatch email if counts are not matched
/usr/sbin/sendmail ${recipients} <<MAIL_END
To: ${recipients}
Subject: Alert! Mismatched found in MSSC Customers(API) data extraction - mssc_customers_api_extraction_wrapper.sh for $prev_dt

Mismatched found in MSSC Customers(API) data extraction for the day - $prev_dt

Total Customers of API : ${api_created_cust}

Total Customers of File : ${file_created_cust}

Start time : ${start_time}
Completion time : `date '+%F %T'`

MAIL_END
# Exit the script after sending the email
	exit 1
		
	else
		#creating a List file containing the list of csv data file
		echo "=================================================================================="
		echo "Creating a list file with all the data file names along with their respective path"
		ls -d -1 "${daily_files_path}/${prev_dt}/mssc_customers_data_"*.csv > ${script_path}/csv_list_daily.lst
		sleep 1
	
		#Upload CSV data files from Unix to s3
		echo "=================================================================================="
		echo "starting copy to s3...."
		echo "s3 upload start time : " `date '+%F %T'`
		while IFS= read -r line
		do
				echo "Data file with path: "$line
				filename=`echo $line | rev | cut -d'/' -f 1 | rev`
				echo "Data file name: "$filename
				foldername=$(echo "$line" | rev | cut -d'/' -f 2 | rev)
				echo "Folder name: $foldername"
				echo -e "\n"
				/usr/local/bin/aws s3 cp ${line} ${s3path}/customers/${prev_dt}/ >> ${log_dir}/s3upload_daily_${prev_dt}.log
		done < ${script_path}/csv_list_daily.lst
		sleep 1
		echo "Data files copy to s3 completed!!"
		echo "=================================================================================="


#send completion email if data is available
/usr/sbin/sendmail ${recipients} <<MAIL_END
To: ${recipients}
Subject: MSSC Customers(API) data extraction and load Job completed - mssc_api_extraction_wrapper.sh for $prev_dt

Job mssc_customers_api_extraction_wrapper.sh for daily MSSC Customers(API) data extraction and load has completed for the day - $prev_dt

Customers created today : ${file_created_cust}

Customers modified today : ${file_modified_cust}

Total Customers today : ${file_total_cust}

Start time : ${start_time}
Completion time : `date '+%F %T'`

MAIL_END
	fi
echo "Completion email sent" > ${log_dir}/mssc_customers_data_extraction_completed_${prev_dt}.txt

	
else 
	echo "No orders for "${prev_dt}
	echo "Completion email sent" > ${log_dir}/mssc_customers_data_extraction_completed_${prev_dt}.txt


#send completion email with 0 count if data in not available
/usr/sbin/sendmail ${recipients} <<MAIL_END
To: ${recipients}
Subject: MSSC(API) data extraction and load Job completed - mssc_api_extraction_wrapper.sh for $prev_dt

Job mssc_api_extraction_wrapper.sh for daily MSSC(API) data extraction and load has completed for the day - $prev_dt

Total number of orders are received today : ${api_total_cust}

Start time : ${start_time}
Completion time : `date '+%F %T'`

MAIL_END

fi

#Touch trigger file and if all above steps are completed
if grep "Completion email sent" ${log_dir}/mssc_customers_data_extraction_completed_${prev_dt}.txt;then
touch ${trigger_dir}/mssc_customer_trigger_$prev_dt.txt
sleep 1s
#copy total_count to the tigger file and load it on S3
echo "${api_total_cust}" > ${trigger_dir}/mssc_customer_trigger_$prev_dt.txt
sleep 5s
/usr/local/bin/aws s3 cp ${trigger_dir}/mssc_customer_trigger_$prev_dt.txt ${trigger_s3}

echo "script completed"
echo "=================================================================================="
fi




























































#if [ -n "$(ls -A "${daily_files}/${prev_dt}/option_items/"*.csv 2>/dev/null)" ]; then
#    ls -d -1 "${daily_files}/${prev_dt}/option_items/"*.csv >> ${files_list_path}/${prev_dt}/csv_list_daily.lst
#    sleep 1
#else
#	echo "No files exist for option_items"
#fi
#
#if [ -n "$(ls -A "${daily_files}/${prev_dt}/order/"*.csv 2>/dev/null)" ]; then
#    ls -d -1 "${daily_files}/${prev_dt}/order/"*.csv >> ${files_list_path}/${prev_dt}/csv_list_daily.lst
#    sleep 2
#else
#	echo "No files exist for order"
#fi
#
#if [ -n "$(ls -A "${daily_files}/${prev_dt}/product/"*.csv 2>/dev/null)" ]; then
#    ls -d -1 "${daily_files}/${prev_dt}/product/"*.csv >> ${files_list_path}/${prev_dt}/csv_list_daily.lst
#    sleep 2
#else
#	echo "No files exist for product"
#fi
#
#
#
##creating a List file containing the list of zipped data files
#echo "=================================================================================="
#echo "Creating a list file with all the data file names along with their respective path"
#ls -d -1 "${daily_files}/${prev_dt}/option_items/"*.csv >> ${files_list_path}/${prev_dt}/csv_list_daily.lst
#sleep 1
#ls -d -1 "${daily_files}/${prev_dt}/order/"*.csv >> ${files_list_path}/${prev_dt}/csv_list_daily.lst
#sleep 1
#ls -d -1 "${daily_files}/${prev_dt}/product/"*.csv >> ${files_list_path}/${prev_dt}/csv_list_daily.lst
#sleep 2

#
##Upload CSV data files from Unix to s3
#echo "=================================================================================="
#echo "starting copy to s3...."
#echo "s3 upload start time : " `date '+%F %T'`
#while IFS= read -r line
#do
#        echo "Data file with path: "$line
#        filename=`echo $line | rev | cut -d'/' -f 1 | rev`
#        echo "Data file name: "$filename
#		foldername=$(echo "$line" | rev | cut -d'/' -f 2 | rev)
#		echo "Folder name: $foldername"
#        echo -e "\n"
#        /usr/local/bin/aws s3 cp ${line} ${s3path}/${foldername}/${prev_dt}/ >> ${log_dir}/s3upload_daily_${run_dt}.log
#done < ${files_list_path}/${prev_dt}/csv_list_daily.lst
#sleep 1
#echo "Data files copy to s3 completed!!"
#echo "=================================================================================="










