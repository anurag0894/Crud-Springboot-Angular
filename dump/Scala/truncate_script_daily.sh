case $1 in
      "ALL" );;
    ''|*[!0-9]*)echo "Please enter the group number of external file to be truncated. Ex: sh truncate_script.sh <N or ALL>"
                echo "Where N=group number(1-9) and ALL=all the External tables except the incremental one"
                exit;;
esac

if [ "$1" = "ALL" ]
then
   beeline -u "jdbc:hive2://alphd1dx000.dlx.idc.ge.com:10000/;transportMode=binary;principal=hive/_HOST@DLX.IDC.GE.COM" --outputformat=csv2 -e "select source_tbl_name from g00103.gs_iqp_master where refresh_mode='FULL_LOAD'">truncate_table_temp.txt

count_arc=$( cat truncate_table_temp.txt | wc -l );
arc=2
original_count=`expr $count_arc - $arc`

while [ "$original_count" -ge 1 ]

    do
      rec=` head -$arc truncate_table_temp.txt|tail -1`
       hdfs dfs -rm -r /tmp/iqp/external_files/$rec/
        original_count=`expr $original_count - 1`
        arc=`expr $arc + 1`

   done

   rm truncate_table_temp.txt
   
   beeline -u "jdbc:hive2://alphd1dx000.dlx.idc.ge.com:10000/;transportMode=binary;principal=hive/_HOST@DLX.IDC.GE.COM" -e "insert into g00103.spark_ingestion_audit_arch select * from g00103.spark_ingestion_audit"
   beeline -u "jdbc:hive2://alphd1dx000.dlx.idc.ge.com:10000/;transportMode=binary;principal=hive/_HOST@DLX.IDC.GE.COM" -e "truncate table g00103.spark_ingestion_audit"
   cd /home/rajeebp/soumya_test/Shell/temp
   touch deleted.done

exit

elif [ $1 -gt 0 -a $1 -lt 10 ]
then

beeline -u "jdbc:hive2://alphd1dx000.dlx.idc.ge.com:10000/;transportMode=binary;principal=hive/_HOST@DLX.IDC.GE.COM" -e "select source_tbl_name from g00103.gs_iqp_master where group_number=$1 and refresh_mode='FULL_LOAD'">truncate_table_temp.txt


count_arc=$( cat truncate_table_temp.txt | wc -l );
arc=4
original_count=`expr $count_arc - $arc`

while [ "$original_count" -ge 1 ]

    do
      rec=` head -$arc truncate_table_temp.txt|tail -1|cut -d ' ' -f 2 `
       hdfs dfs -rm -r /tmp/iqp/external_files/$rec/
        original_count=`expr $original_count - 1`
        arc=`expr $arc + 1`

   done

   rm truncate_table_temp.txt
   exit

else
   echo "Wrong group number entered"
   echo "Please enter the group number of external file to be truncated. Ex: sh truncate_script.sh <N or ALL>"
   echo "Where N=group number 1-9 and ALL=all the External tables except the incremental one"

fi
