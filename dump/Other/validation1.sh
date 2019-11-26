

##########	PATHS	###############
#Manual_Adjustments

SRC_PATH=/data/staging/g00103/ge_power/iqp/Manual_Adjustment/Status
CSV_PATH=/data/staging/g00103/ge_power/iqp/Manual_Adjustment/CSVFiles
TEMP_DIR_PATH=/data/staging/g00103/ge_power/iqp/Manual_Adjustment/TempDir/
SCRIPT_PATH=/data/staging/g00103/ge_power/iqp/Manual_Adjustment

ERROR_FILE_NAME="error_file.txt"
BKP_MAIL_LIST="Chitta.Sahoo@ge.com"

##########	TEMP FILES	###############

TEMP_FILE_CSV=$TEMP_DIR_PATH/csv_file_list.txt
ERROR_FILE=$TEMP_DIR_PATH/error_file.txt

##########	DELETING TEMP FILES	###############

rm -f $ERROR_FILE
rm -f $TEMP_FILE_CSV
rm -f $SRC_PATH/Indirect_BACKLOG.txt
rm -f $SRC_PATH/Indirect_SALES.txt
rm -f $SRC_PATH/Indirect_ORDERS.txt
rm -f $SRC_PATH/Backlog_Validation.txt
rm -f $SRC_PATH/Orders_Validation.txt
rm -f $SRC_PATH/Sales_Validation.txt
rm -f $SRC_PATH/Validation_failed.txt


cd $CSV_PATH
echo "$CSV_PATH"

##	Copying the names of all the files from source path to temp file  ##

ls -1 *.csv > $TEMP_FILE_CSV
chmod 777 $TEMP_FILE_CSV

echo "CSV file lists created"

##	Copying the names of all the files from temp file to backlog ,sales and order temp files  ##

cat $TEMP_FILE_CSV|while read line
do
echo $line
full_file_name=`echo $line`
file_name=`echo $full_file_name|awk -F. '{print $1}'`
#file_type=`cat $full_file_name|head -3|tail -1|awk -F, '{print $3}'`
file_type_backlog=`cat $full_file_name|head -3|tail -1|awk -F, '{print $3}'|tr -d '"'`
file_type_backlognull=`cat $full_file_name|head -3|tail -1|awk -F, '{print $4}'|tr -d '"'`
file_type_sales=`cat $full_file_name|head -3|tail -1|awk -F, '{print $48}'|tr -d '"'`
file_type_salesnull=`cat $full_file_name|head -3|tail -1|awk -F, '{print $49}'|tr -d '"'`
file_type_orders=`cat $full_file_name|head -3|tail -1|awk -F, '{print $2}'|tr -d '"'`
echo $file_type_backlog
echo $file_type_sales
echo $file_type_orders

if [ $file_type_backlog = "Backlog" ] && [ $file_type_backlognull != " " ] ; then
echo  $file_name".csv" >> $SRC_PATH/Backlog_Validation.txt
echo $file_name".xml" >> $SRC_PATH/Backlog_Validation.txt

elif [ $file_type_sales = "Sales" ] && [ $file_type_salesnull != " " ] ; then
echo  $file_name".csv" >>$SRC_PATH/Sales_Validation.txt
echo $file_name".xml" >>$SRC_PATH/Sales_Validation.txt

elif [ $file_type_orders = "Orders" ] ; then
echo  $file_name".csv" >>$SRC_PATH/Orders_Validation.txt
echo $file_name".xml" >>$SRC_PATH/Orders_Validation.txt
else
echo "File not in(Backlog,Sales,Orders)"
echo $file_name >>$SRC_PATH/Validation_failed.txt
fi

done

cat $SRC_PATH/Backlog_Validation.txt |grep csv >$SRC_PATH/Indirect_BACKLOG.txt
cat $SRC_PATH/Sales_Validation.txt |grep csv >$SRC_PATH/Indirect_SALES.txt
cat $SRC_PATH/Orders_Validation.txt |grep csv >$SRC_PATH/Indirect_ORDERS.txt

hdfs dfs -put -f $SRC_PATH/Indirect_BACKLOG.txt  /tmp/manual_adjustments/
hdfs dfs -put -f $SRC_PATH/Indirect_ORDERS.txt  /tmp/manual_adjustments/ 
hdfs dfs -put -f $SRC_PATH/Indirect_SALES.txt  /tmp/manual_adjustments/ 


#echo "FILE VALIDATION STARTS"

#sh $SCRIPT_PATH/validation_backlog.sh
#sh $SCRIPT_PATH/validation_sales.sh
#sh $SCRIPT_PATH/validation_orders.sh
