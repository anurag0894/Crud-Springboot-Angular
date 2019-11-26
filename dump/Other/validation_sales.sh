#!/bin/ksh


########## 	PATHS	###############

SRC_PATH=/data/staging/g00103/ge_power/iqp/Manual_Adjustment/CSVFiles
SRC_PATH_XLS=/data/staging/g00103/ge_power/iqp/Manual_Adjustment/XLSFiles
SOURCE_FILES=/data/staging/g00103/ge_power/iqp/Manual_Adjustment 
ARCHIVE_PATH=/data/staging/g00103/ge_power/iqp/FlatFiles_archive/Pulse
TEMP_DIR_PATH=/data/staging/g00103/ge_power/iqp/Manual_Adjustment/TempDir
SCRIPT_PATH=/data/staging/g00103/ge_power/iqp/Manual_Adjustment
STATUS=/data/staging/g00103/ge_power/iqp/Manual_Adjustment/Status

ERROR_FILE_NAME="error_file.txt"
BKP_MAIL_ID="chitta.sahoo@ge.com"
#BKP_MAIL_ID="sasmita1.behera@ge.com"

##########	TEMP FILES	###############

ERROR_FILE=$TEMP_DIR_PATH"Sales_error_file.txt"




#######################  Sales Check  ###########################
FIELD_48_CHECK()
{
ln=1

cat $SRC_PATH/$FILE|while read line
do
if [ $ln -eq 3 ]; then
value=`echo $line|awk -F, '{print $48}'|tr -d '"'`
	if [ "$value" != "Sales" ] ; then
		err=1
       	echo " \n File $FILE has its 48th field incorrect in line "$ln >>$ERROR_FILE
        fi       
ln=` expr $ln + 1 `

else
ln=` expr $ln + 1 `
fi
done
}


#######################  Week Check  ###########################
FIELD_49_CHECK()
{
ln=1

cat $SRC_PATH/$FILE|while read line
do
if [ $ln -le 2 ]; then 
echo "header"
else
week=`echo $line|awk -F, '{ print $49}'`
echo $week

#########    week checking   ##########

	if  [ "$week" = "" ] 
	then
  		break
	else
		if [ $week -ge 00 ] && [ $week -le 52 ] ;
 		then 
       		echo "correct week"        
		else
			err=1
       		echo " \n File $FILE has incorrect week value " $week " in line "$ln >>$ERROR_FILE      
		fi 
	fi
fi
 ln=` expr $ln + 1 `
done                
}


#######################  Year Check  ###########################
FIELD_50_CHECK()
{
ln=1

cat $SRC_PATH/$FILE|while read line
do
if [ $ln -le 2 ]; then 
echo "header"
else
YEAR=`echo $line|awk -F, '{ print $50}'`
if  [ "$YEAR" = "" ] 
	then
  		break
else
YEAR_len=`echo $YEAR|tr -d " "|wc -c`
YEAR_type=`echo $YEAR|tr -d [:digit:]`
if  [ $YEAR_len -ne 5 ] ;
  then 
		err=1
              echo " \n File $FILE has incorrect year value " $YEAR " in line "$ln >>$ERROR_FILE

  else
                if [ "$YEAR_type" != "" ];
                  then 
              	err=1                                  
              	echo " \n File $FILE has non numeric year value " $YEAR " in line "$ln >>$ERROR_FILE
                 fi
fi 
fi
fi
ln=` expr $ln + 1 `
done                
}

#######################  Field 52 Check  ###########################
FIELD_51_CHECK()
{
ln=1

cat $SRC_PATH/$FILE|while read line
do
if [ $ln -le 2 ]; then 
echo "header"
else
Region=`echo $line|awk -F, '{ print $51}'|tr -d " "`
if  [ "$Region" = "" ] 
	then
  		break
else
	Region_type=`echo $Region|grep [a-zA-z]`
	if  [ "$Region_type" = "" ] ;
  	then 
		err=1
              echo " \n File $FILE has numeric value " $Region " in Customer Name field at line "$ln >>$ERROR_FILE

	fi 
fi
fi
ln=` expr $ln + 1 `
done                
}

#######################  Field 59 Check  ###########################
FIELD_58_CHECK()
{
ln=1

cat $SRC_PATH/$FILE|while read line
do
if [ $ln -le 2 ]; then 
echo "header"
else
Sub_region=`echo $line|awk -F, '{ print $58}'|tr -d " "`
if  [ "$Sub_region" = "" ] 
	then
  		break
else
	Sub_region_type=`echo $Sub_region|grep [a-zA-z]`
	if  [ "$Sub_region_type" = "" ] ;
  	then 
		err=1
              echo " \n File $FILE has numeric value " $Sub_region " in sub_region field at line "$ln >>$ERROR_FILE

	fi 
fi
fi
ln=` expr $ln + 1 `
done                
}

#######################  Field 60 Check  ###########################
FIELD_59_CHECK()
{
ln=1

cat $SRC_PATH/$FILE|while read line
do
if [ $ln -le 2 ]; then 
echo "header"
else
Total=`echo $line|awk -F, '{ print $59}'|tr -d "-"|tr -d "."`
if  [ "$Total" = "" ] 
	then
  		break
else
	Total_type=`echo $Total|tr -d [:digit:]`
	if  [ "$Total_type" = "" ] ;
  	then 
		echo "correct total"
       else
		err=1
              echo " \n File $FILE has non numeric value " $Total " in US Sales Value  field at line "$ln >>$ERROR_FILE
	fi 
fi
fi
ln=` expr $ln + 1 `
done                
}

#######################  Field 69 Check  ###########################
FIELD_68_CHECK()
{
ln=1

cat $SRC_PATH/$FILE|while read line
do
if [ $ln -le 2 ]; then 
echo "header"
else
Month1=`echo $line|awk -F, '{ print $68}'|tr -d "-"|tr -d "."`
if  [ "$Month1" = "" ] 
	then
  		break
else
	Month1_type=`echo $Month1|tr -d [:digit:]`
	if  [ "$Month1_type" = "" ] ;
  	then 
		echo "correct Month1"
	else
		err=1
              echo " \n File $FILE has non numeric value " $Month1 " in Local Order Value field at line "$ln >>$ERROR_FILE

	fi 
fi
fi
ln=` expr $ln + 1 `
done                
}

#######################  Field 71 Check  ###########################
FIELD_70_CHECK()
{
ln=1

cat $SRC_PATH/$FILE|while read line
do
if [ $ln -le 2 ]; then 
echo "header"
else
Month18=`echo $line|awk -F, '{ print $70}'|tr -d "-"|tr -d "."`
if  [ "$Month18" = "" ] 
	then
  		break
else
	Month18_type=`echo $Month18|tr -d [:digit:]`
	if  [ "$Month18_type" = "" ] ;
  	then 
		echo "correct Month18"
	else
		err=1
              echo " \n File $FILE has non numeric value " $Month18 " in Exchange Rate field at line "$ln >>$ERROR_FILE

	fi 
fi
fi
ln=` expr $ln + 1 `
done                
}



##################################################	VALIDATION STARTS	#################################################


VALIDATION()
{
		
FIELD_48_CHECK
FIELD_49_CHECK
FIELD_50_CHECK
FIELD_51_CHECK
FIELD_58_CHECK
FIELD_59_CHECK
FIELD_68_CHECK
FIELD_70_CHECK
}
####################################################	MAIN FUNCTION STARTS	 ####################################################

cd $SOURCE_FILES

File_count=`cat $STATUS/Sales_Validation.txt|wc -l`
if [ $File_count -eq 0 ];
	then
	echo " No Sales file uploaded for  date `date` " 
	# echo " No Sales file uploaded for  date `date` " | mailx -s "No Files uploaded" "$BKP_MAIL_LIST"
else
	cat $STATUS/Sales_Validation.txt |grep csv >TEMP_SALES_CSV.txt
	for i in `cat TEMP_SALES_CSV.txt`
	do
	echo "File $i is going to be validated."
	FILE=$i
	echo "VALIDATING $FILE"
	echo "VALIDATION STARTS "
	err=0
##########	DELETING TEMP FILES	###############

rm -f $ERROR_FILE

	VALIDATION
	echo "VALIDATION ENDS.....Error value is = $err"
	if [ $err -eq 0 ] ; then
		echo "NO ERRORS FOUND"
		echo  $FILE >> $SOURCE_FILES/Indirect_SALES.txt
	else
		echo "ERRORS EXIST IN THE FILE"
		FILE_NAME=`echo $FILE|awk -F. '{print $1}'`
		cd $SOURCE_FILES/XLSFiles
		FILE_PRESENT=`ls *.xml |grep $FILE_NAME` 
		if [ "$FILE_PRESENT" = "" ] ; then
			echo "xml file not found"
		#	cat $ERROR_FILE | mailx -s "File $FILE Got Rejected" "$BKP_MAIL_ID"
			mv $SRC_PATH/"$FILE_NAME".csv $ARCHIVE_PATH/CSVFiles
		else
					MAIL_ID1=`cat "$FILE_NAME".xml|grep "RequestorEmail"|cut -d">" -f11|cut -d"<" -f1`
					echo $MAIL_ID1
		#	MAIL_ID="chitta.sahoo@ge.com"
			# MAIL_ID="sasmita1.behera@ge.com"
			# cat $ERROR_FILE | mailx -s "File $FILE Got Rejected" "$MAIL_ID"
				#(echo "Please find attached the file containing the reasons for rejections" ; uuencode $ERROR_FILE $ERROR_FILE_NAME;)| mailx -s "File $FILE_TYPE Got Rejected" "$MAIL_ID"	
			mv $SRC_PATH/"$FILE_NAME".csv $ARCHIVE_PATH/CSVFiles
			cd $SOURCE_FILES/XLSFiles
			mv $SRC_PATH/"$FILE_NAME".xml $ARCHIVE_PATH/XLSFiles
		fi
									
	fi	
echo "VALIDATED" $FILE
done
fi

chmod 777  $SOURCE_FILES/Indirect_SALES.txt
