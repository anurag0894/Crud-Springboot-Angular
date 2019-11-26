

export JAVA_HOME=/usr/java/jdk1.8.0_101/jre
export PATH=$JAVA_HOME/bin:$PATH

echo "Main started at :" $(date)

rm /data/staging/g00103/ge_power/iqp/Manual_Adjustment/Failed_Files.txt
touch /data/staging/g00103/ge_power/iqp/Manual_Adjustment/Failed_Files.txt

excel_dir_path=/data/staging/g00103/ge_power/iqp/Manual_Adjustment/XLSFiles
csv_dir_path=/data/staging/g00103/ge_power/iqp/Manual_Adjustment/CSVFiles
log_dir=/data/staging/g00103/ge_power/iqp/Manual_Adjustment/Log
scrtpath=/data/staging/g00103/ge_power/iqp/Manual_Adjustment
archive_csv=/data/staging/g00103/ge_power/iqp/FlatFiles_archive/Pulse/CSVFiles
archive_xls=/data/staging/g00103/ge_power/iqp/FlatFiles_archive/Pulse/XLSFiles
excel_sheet_names=Template
excel_sheet_names_ord=Orders_Template
Failed=Failed_Files.txt

mv $csv_dir_path/* $archive_csv/  

hdfs dfs -rm  /tmp/manual_adjustments/CSVFiles/*
hdfs dfs -put $scrtpath/filelist.csv /tmp/manual_adjustments/CSVFiles/filelist.csv

##############################
# Reading Variable List
##############################

cat /data/staging/g00103/ge_power/iqp/Manual_Adjustment/Filenames.txt |while read Line1
do
if [  -z "$Line1" ]; then
   break;
fi
excel_file_name=$Line1
excel_file_wo_extn=`echo $excel_file_name | cut -d. -f1`
excel_file_Extn=`echo $excel_file_name | cut -d. -f2`
csv_file_names=`echo $excel_file_wo_extn`
echo "processing  "$csv_file_names".xls"

default_file="$excel_file_wo_extn"".""$excel_file_Extn"
xls_file=$(echo $excel_file_name | cut -d"." -f1)
i_file_extn=$(echo $excel_file_name | cut -d"." -f2)


HOURDATE=$(date '+%Y%m%d%H%M')
STAMP=$(date '+%Y%m%d-%H:%M')
log_file=$log_dir/${xls_file}_$STAMP.log


#echo $log_file

#----Checking Existence of File Started------------------------------------------------------

if [ -s $excel_dir_path/$excel_file_name ]; then
	error_variable="Input File exists"
	echo $error_variable > $log_file
else

	error_variable="Input File does not exists"
	echo $error_variable > $log_file

fi

java  -Xmx512m  -cp poi-3.9.jar:poi-ooxml-3.8-beta3.jar:poi-ooxml-schemas-3.8-beta4.jar:xmlbeans-2.3.0.jar:dom4j-1.6.1.jar:javax.xml.stream-1.0.1.jar:. PulseConversion "$excel_dir_path/$excel_file_name" "$csv_dir_path" "$csv_file_names" "$excel_sheet_names"

java_status=$?

if [ $java_status -ne 0 ]
then
echo -e  $excel_file_name '\n' >> $csv_dir_path/$Failed
echo "File failed"
else
echo "File passed"
fi

echo $excel_dir_path
echo $excel_file_name
echo $csv_dir_path 
echo $csv_file_names


echo "---------------------------------------"

done

mv $excel_dir_path/* $archive_xls/
hdfs dfs -put $csv_dir_path/* /tmp/manual_adjustments/CSVFiles/

if [ -z $java_status ]
then
    echo "Main ended Successfully at :" $(date)
    
elif [ $java_status -eq 0 ]
then 
   echo "Main ended Successfully at :" $(date)
    
else
  echo $java_status
    echo "Main ended with failure at :" $(date)
    
fi
