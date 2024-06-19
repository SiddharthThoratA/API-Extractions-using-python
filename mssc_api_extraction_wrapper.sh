
############################################################################################
#Purpose of the script : Main script to extract MSSC data by calling API from SFCC-stage   #
# in JSON fromat and convert it into csv.							                	   #
#how to call this script :                                                     		  	   #
#sh mssc_api_extraction_wrapper.sh > ${log_dir}/mssc_api_extraction_wrapper.log  		   #
#  																						   #
#Created date: 2023-07-15 : Siddharth Thorat   				             	 		       #
#Updated on:																			   #
# - 2023-08-01 : Siddharth Thorat : Files count and API count check logic added            #
# - 2023-08-03 : Siddharth Thorat : Logic implemented to fetch data on 1sec timeframe      #
# - 2023-08-04 : Siddharth Thorat : Logic implemented to verify total modified orders	   #
#																					       #
############################################################################################

#!/bin/bash

cd /home/dstdw/mssc

#variable declaration
script_path=/home/dstdw/mssc
d=`date +%Y%m%d%H%M%S`
#run_dt=`date +%Y%m%d`
#run_dt='20230701'
log_dir=
daily_files_path=

prev_dt=$(date -d "-1 day" +%Y%m%d)
#prev_dt='20230802'
s3path=
trigger_dir=
trigger_s3=

#echo "Run Date is : " ${run_dt}
echo "Previous date is : " ${prev_dt}

start_time=`date '+%F %T'`

echo "Start Time : "${start_time}

recipients=

#recipients=

#send start email notification
/usr/sbin/sendmail ${recipients} <<MAIL_END
To: ${recipients}
Subject: MSSC(API) data extraction and load Job started - mssc_api_extraction_wrapper.sh for $prev_dt

Job mssc_api_extraction_wrapper.sh for daily MSSC(API) data extraction and load has started for the day - $prev_dt

Start time : `date '+%F %T'`

MAIL_END


#calling python to extract JSON data by API and convert it into csv file 
python3 mssc_api_extraction.py -d ${prev_dt} -c mssc_config_params.txt > ${log_dir}/mssc_api_extraction_${prev_dt}.log &

#wait till python extraction gets completed
while true
do
s1=`grep complete ${log_dir}/mssc_api_extraction_${prev_dt}.log |  awk '{ print $3 }'`

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
api_total_orders=`grep Total_Number_of_records ${log_dir}/mssc_api_extraction_${prev_dt}.log | awk '{print $3}'`
api_created_orders=`grep Total_records_on_creation_date ${log_dir}/mssc_api_extraction_${prev_dt}.log | awk '{print $3}'`
api_modified_orders=`grep Total_records_on_last_modified ${log_dir}/mssc_api_extraction_${prev_dt}.log | awk '{print $3}'`

echo "**************************************************************************"
echo "Number of order created today : " ${api_created_orders}
echo "**************************************************************************"
echo "Number of order modified today : " ${api_modified_orders}
echo "**************************************************************************"
echo "Total Number of order today : " ${api_total_orders}
echo "**************************************************************************"

#proceed with further steps if count is greater than 0
if [[ ${api_total_orders} != '0' ]]; then
	echo "Yes!! orderes are there, let's proceed with further steps"
	
	#calling python to combined all csv file from order,product and option_items folders respectively
	python3 csv_files_merge.py -d ${prev_dt} -c mssc_config_params.txt > ${log_dir}/csv_files_merge_${prev_dt}.log &
	
	#wait till python extraction gets completed
	while true
	do
	s2=`grep complete ${log_dir}/csv_files_merge_${prev_dt}.log |  awk '{ print $3 }'`
	
	#echo "waiting for python scripts to get completed"		
	if [[ ${s2} == 'complete' ]]; then
			echo "combined files process completed Successfully"
			echo 'completion time : ' `date '+%F %T'`
			break
	else
			sleep 10
	fi
	done
	
	#Get the created, modified and total orders count from the csv file and store it to the variables	
	file_total_orders=`grep total_orders_today ${log_dir}/csv_files_merge_${prev_dt}.log | awk '{print $3}'`
	file_total_modified=`grep total_modified ${log_dir}/csv_files_merge_${prev_dt}.log | awk '{print $3}'`
	file_created_orders=`grep orders_created_today ${log_dir}/csv_files_merge_${prev_dt}.log | awk '{print $3}'`
	file_modified_orders=`grep orders_modified_today ${log_dir}/csv_files_merge_${prev_dt}.log | awk '{print $3}'`
	
	echo "**************************************************************************"
	echo "Total number of orders : " ${file_total_orders}
	echo "Total number of modified : " ${file_total_modified}
	echo "Today's created orders: " ${file_created_orders}
	echo "Todays's modified orders : " ${file_modified_orders}
	
	
	# Check if total created and modified counts are matched or not,if not matched than send notification
	if ([ $api_created_orders -ne $file_created_orders ] && [ $api_modified_orders -ne $file_total_modified ]); then
		echo "sending mismatched email"

#send mismatch email if counts are not matched
/usr/sbin/sendmail ${recipients} <<MAIL_END
To: ${recipients}
Subject: Alert! Mismatched found in MSSC(API) data extraction - mssc_api_extraction_wrapper.sh for $prev_dt

Mismatched found in MSSC(API) data extraction for the day - $prev_dt

Total created orders of API : ${api_created_orders}
Total created orders of File : ${file_created_orders}

Total modified orders of API : ${api_modified_orders}
Total modified orders of File : ${file_total_modified}

Start time : ${start_time}
Completion time : `date '+%F %T'`

MAIL_END
# Exit the script after sending the email
	exit 1
		
	else
		#creating a List file containing the list of zipped data files
		echo "=================================================================================="
		echo "Creating a list file with all the data file names along with their respective path"
		ls -d -1 "${daily_files_path}/${prev_dt}/option_item/mssc_option_items_data_"*.csv >> ${daily_files_path}/${prev_dt}/csv_list_daily.lst
		sleep 1
		ls -d -1 "${daily_files_path}/${prev_dt}/order/mssc_orders_data_"*.csv >> ${daily_files_path}/${prev_dt}/csv_list_daily.lst
		sleep 1
		ls -d -1 "${daily_files_path}/${prev_dt}/product/mssc_products_data_"*.csv >> ${daily_files_path}/${prev_dt}/csv_list_daily.lst
		sleep 2	
	
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
				/usr/local/bin/aws s3 cp ${line} ${s3path}/${foldername}/${prev_dt}/ >> ${log_dir}/s3upload_daily_${prev_dt}.log
		done < ${daily_files_path}/${prev_dt}/csv_list_daily.lst
		sleep 1
		echo "Data files copy to s3 completed!!"
		echo "=================================================================================="


#send completion email if data is available
/usr/sbin/sendmail ${recipients} <<MAIL_END
To: ${recipients}
Subject: MSSC(API) data extraction and load Job completed - mssc_api_extraction_wrapper.sh for $prev_dt

Job mssc_api_extraction_wrapper.sh for daily MSSC(API) data extraction and load has completed for the day - $prev_dt

Orders created today : ${file_created_orders}

Orders modified today : ${file_modified_orders}

Total orders today : ${file_total_orders}

Start time : ${start_time}
Completion time : `date '+%F %T'`

MAIL_END
	fi
echo "Completion email sent" > ${log_dir}/mssc_data_extraction_completed_${prev_dt}.txt

	
else 
	echo "No orders for "${prev_dt}
	echo "Completion email sent" > ${log_dir}/mssc_data_extraction_completed_${prev_dt}.txt


#send completion email with 0 count if data in not available
/usr/sbin/sendmail ${recipients} <<MAIL_END
To: ${recipients}
Subject: MSSC(API) data extraction and load Job completed - mssc_api_extraction_wrapper.sh for $prev_dt

Job mssc_api_extraction_wrapper.sh for daily MSSC(API) data extraction and load has completed for the day - $prev_dt

Total number of orders are received today : ${api_total_orders}

Start time : ${start_time}
Completion time : `date '+%F %T'`

MAIL_END

fi

#Touch trigger file and if all above steps are completed
if grep "Completion email sent" ${log_dir}/mssc_data_extraction_completed_${prev_dt}.txt;then
touch ${trigger_dir}/mssc_etl_trigger_$prev_dt.txt
sleep 1s
#copy total_count to the tigger file and load it on S3
echo "${api_total_orders}" > ${trigger_dir}/mssc_etl_trigger_$prev_dt.txt
sleep 5s
/usr/local/bin/aws s3 cp ${trigger_dir}/mssc_etl_trigger_$prev_dt.txt ${trigger_s3}

echo "script completed"
echo "=================================================================================="
fi
