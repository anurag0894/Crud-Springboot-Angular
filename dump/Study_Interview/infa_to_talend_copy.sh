#!/bin/bash


log=/data/staging/g00103/ge_power/iqp/ShellScripts/Log_INFA_Copy`date +%Y-%m-%d`
files=/data/staging/g00103/ge_power/iqp/FlatFiles
arc=/data/staging/g00103/ge_power/iqp/FlatFiles_archive
groupmap=/data/staging/g00103/ge_power/iqp/ShellScripts/groupmap.psv
scripts=/data/staging/g00103/ge_power/iqp/ShellScripts

rm -f /data/staging/g00103/ge_power/iqp/ShellScripts/Log_INFA_Copy*


### Handling different date formats

date_parameter=`date +%Y%m%d`
date_parameter_sent=`date '+%d%m%Y' -d "$date_parameter-1 days"`
date_parameter_sales=`date '+%d%m%Y' -d "$date_parameter-2 days"`
date_parameter_prolec=`date +%y%m%d`
date_parameter_opp=`date +%m%d%y`
date_parameter_Ids=`date +%d%m%y`



### Taking group number specified

for group in 1 2 3 4 5 6
do

if [ "$group" == 1 ]
then
sourcefiledir=/infa_shared/imatica/IQP_ENNDW_DE/Archive

elif [ "$group" == 2 ]
then
sourcefiledir=/infa_shared/imatica/IQP_ENNDW_DE/Archive/CDW

elif [ "$group" == 3 ]
then 
sourcefiledir=/infa_shared/imatica/IQP_ENNDW_DE/Archive/Ids_Archive

elif [ "$group" == 4 ] 
then
sourcefiledir=/infa_shared/imatica/IQP_ENNDW_DE/Archive/SRVC
   
elif [ "$group" == 5 ] 
then   
sourcefiledir=/infa_shared/imatica/IQP_ENNDW_DE/SrcFiles

else
sourcefiledir=/infa_shared/imatica/IQP_ENNDW_DE/LkpFiles

fi  

echo "Source Directory $sourcefiledir "
echo "SFTP Transfer starts for group $group"
echo "SFTP Transfer starts for group $group " >> $log
echo $'\n'

cd $scripts

 grep -n "^${group}" $groupmap >  new_filegroup
          				IFS="|"
          				while read f1 f2
					do
					inputfile=$f2
					done < new_filegroup


echo $inputfile
echo $'\n'

### Iterating individual files					
IFS=" "
					
for file in $inputfile
do
echo "loop start for $file" >> $log
echo "loop start for $file"


if [ "$file" = "ARCH_sales" -o "$file" = "ARCH_backlog" -o "$file" = "ARCH_orders" -o "$file" = "EU_TIME_KEEPING_DATA" ]
then
temp_var=${file}_${date_parameter_sent}*

elif [ "$file" = "sales" ]
then
temp_var=${file}_${date_parameter_sales}*

elif [ "$file" = "ARCH_Prolec_orders" ]
then
temp_var=${file}.txt.${date_parameter_prolec}*


elif [ ${group} = "2" -o  ${group} = "4" ]
then
  if  [ "$file" = "RMA_EMEA" ]
   then 
     temp_var=${file}${date_parameter_opp}*.txt
  else 
     temp_var=${file}${date_parameter_opp}*.csv
   fi


elif [ ${group} == "5" ]
then 
   if [ "$file" = "Date_mas" ]
   then 
     temp_var=${file}.dat
   elif [ "$file" = "BUSINESS_RULE1" ]
   then 
     temp_var=${file}.CSV
   else 
     temp_var=${file}.csv
   fi	 
       
elif [ ${group} = "3"  ]
then 
temp_var=${file}_${date_parameter_Ids}*

elif [ ${group} = "6"  ]
then
temp_var=${file}.csv

else 
temp_var=${file}*${date_parameter}*
   
fi

cd $files

sftp -b - dedwinf@pwnlp0945p01.corporate.ge.com <<END_SCRIPT
	cd  $sourcefiledir
	get $temp_var
	
END_SCRIPT

if [ $? -eq 0 ] ;
	then
	        echo "SFTP Transfer success $temp_var"
	        echo "SFTP Transfer success $temp_var" >> $log
                echo $'\n'
               
	       
	else
	  	echo "SFTP Transfer Error $temp_var"
	  	echo "SFTP Transfer Error $temp_var" >> $log
                echo $'\n'
                
	  	
		  
	fi

done


echo "SFTP Transfer completed for group $group "
echo $'\n'
echo "SFTP Transfer completed for group $group " >> $log


## SAP has : in name creating problem in copying from Talend to HDFS

if [ ${group} == "1" ]
then 
if [ `ls -l SAP_Orders.csv*|wc -l` == 1  ]
then 
SAP_var=$(ls  SAP_Orders.csv* |cut -d " " -f2|cut -d ":" -f1)
mv SAP_Orders* $SAP_var
fi
fi

done


cd $files


gunzip *.gz

##echo "calling delete file scrirpt"


#defining directory variables : change values based on environment/server

##bash /data/staging/g00103/ge_power/iqp/ShellScripts/del_dup_files.sh

