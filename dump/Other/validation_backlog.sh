#!/bin/ksh


##########	PATHS	###############

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

ERROR_FILE=$TEMP_DIR_PATH"Backlog_error_file.txt"




#######################  Backlog Check  ###########################
FIELD_3_CHECK()
{
ln=1

cat $SRC_PATH/$FILE|while read line
do
if [ $ln -eq 3 ]; then
value=`echo $line|awk -F, '{print $3}'|tr -d '"'`
	if [ "$value" != "Backlog" ] ; then
		err=1
       	echo " \n File $FILE has its 3rd field incorrect in line "$ln >>$ERROR_FILE
        fi       
ln=` expr $ln + 1 `

else
ln=` expr $ln + 1 `
fi
done
}


#######################  Week Check  ###########################
FIELD_4_CHECK()
{
ln=1

cat $SRC_PATH/$FILE|while read line
do
if [ $ln -le 2 ]; then 
echo "header"
else
week=`echo $line|awk -F, '{ print $4}'`
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

#######################  Month Check  ###########################

FIELD_5_CHECK()
{
ln=1

cat $SRC_PATH/$FILE|while read line
do
if [ $ln -le 2 ]; then 
echo "header"
else
MONTH=`echo $line|awk -F, '{ print $5}'|tr -d '"'`
echo $MONTH
	if  [ "$MONTH" = "" ] 
	then
  		break
	else
		if  [ $MONTH = "Jan" ] || [ $MONTH = "Feb" ] || [ $MONTH = "Mar" ] || [ $MONTH = "Apr" ] || [ $MONTH = "May" ] || [ $MONTH = "Jun" ] || [ $MONTH = "Jul" ] || [ $MONTH = "Aug" ] || [ $MONTH = "Sep" ] || [ $MONTH = "Oct" ] || [ $MONTH = "Nov" ] || [ $MONTH = "Dec" ];
  		then 
           		echo "correct month"    
  		else
			err=1
      			echo " \n File $FILE has incorrect month value " $MONTH " in line "$ln >>$ERROR_FILE      
		fi 
	fi
fi
ln=` expr $ln + 1 `
done                
}

#######################  Year Check  ###########################
FIELD_6_CHECK()
{
ln=1

cat $SRC_PATH/$FILE|while read line
do
if [ $ln -le 2 ]; then 
echo "header"
else
YEAR=`echo $line|awk -F, '{ print $6}'`
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

#######################  Field 16 Check  ###########################
FIELD_16_CHECK()
{
ln=1

cat $SRC_PATH/$FILE|while read line
do
if [ $ln -le 2 ]; then 
echo "header"
else
Region=`echo $line|awk -F, '{ print $16}'|tr -d " "`
if  [ "$Region" = "" ] 
	then
  		break
else
	Region_type=`echo $Region|grep [a-zA-z]`
	if  [ "$Region_type" = "" ] ;
  	then 
		err=1
              echo " \n File $FILE has numeric value " $Region " in region field at line "$ln >>$ERROR_FILE

	fi 
fi
fi
ln=` expr $ln + 1 `
done                
}

#######################  Field 17 Check  ###########################
FIELD_17_CHECK()
{
ln=1

cat $SRC_PATH/$FILE|while read line
do
if [ $ln -le 2 ]; then 
echo "header"
else
Sub_region=`echo $line|awk -F, '{ print $17}'|tr -d " "`
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

#######################  Field 18 Check  ###########################
FIELD_18_CHECK()
{
ln=1

cat $SRC_PATH/$FILE|while read line
do
if [ $ln -le 2 ]; then 
echo "header"
else
Total=`echo $line|awk -F, '{ print $18}'|tr -d "-"|tr -d "."`
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
              echo " \n File $FILE has non numeric value " $Total " in Total field at line "$ln >>$ERROR_FILE
	fi 
fi
fi
ln=` expr $ln + 1 `
done                
}

#######################  Field 19 Check  ###########################
FIELD_19_CHECK()
{
ln=1

cat $SRC_PATH/$FILE|while read line
do
if [ $ln -le 2 ]; then 
echo "header"
else
Month1=`echo $line|awk -F, '{ print $19}'|tr -d "-"|tr -d "."`
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
              echo " \n File $FILE has non numeric value " $Month1 " in Month1 field at line "$ln >>$ERROR_FILE

	fi 
fi
fi
ln=` expr $ln + 1 `
done                
}

#######################  Field 35 Check  ###########################
FIELD_35_CHECK()
{
ln=1

cat $SRC_PATH/$FILE|while read line
do
if [ $ln -le 2 ]; then 
echo "header"
else
Month18=`echo $line|awk -F, '{ print $35}'|tr -d "-"|tr -d "."`
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
              echo " \n File $FILE has non numeric value " $Month18 " in Month18 field at line "$ln >>$ERROR_FILE

	fi 
fi
fi
ln=` expr $ln + 1 `
done                
}



##################################################	VALIDATION STARTS	#################################################


VALIDATION()
{
		
FIELD_3_CHECK
FIELD_4_CHECK
FIELD_5_CHECK
FIELD_6_CHECK
FIELD_16_CHECK
FIELD_17_CHECK
FIELD_18_CHECK
FIELD_19_CHECK
FIELD_35_CHECK
}
####################################################	MAIN FUNCTION STARTS	 ####################################################

cd $SOURCE_FILES
File_count=`cat $STATUS/Backlog_Validation.txt|wc -l`
if [ $File_count -eq 0 ];
	then
	echo " No Backlog file uploaded for  date `date` " | mailx -s "No Files uploaded" "$BKP_MAIL_LIST"
else
	cat $STATUS/Backlog_Validation.txt |grep csv >TEMP_BACKLOG_CSV.txt
	for i in `cat TEMP_BACKLOG_CSV.txt`
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
		echo  $FILE >> $SOURCE_FILES/Indirect_BACKLOG.txt
	else
		echo "ERRORS EXIST IN THE FILE"
		FILE_NAME=`echo $FILE|awk -F. '{print $1}'`
		cd $SRC_PATH_XLS
		FILE_PRESENT=`ls *.xml |grep $FILE_NAME` 
		if [ "$FILE_PRESENT" = "" ] ; then
			echo "xml file not found"
		#	cat $ERROR_FILE | mailx -s "File $FILE Got Rejected" "$BKP_MAIL_ID"
			mv $SRC_PATH/"$FILE_NAME".csv $ARCHIVE_PATH/CSVFiles
		else
					MAIL_ID1=`cat "$FILE_NAME".xml|grep "RequestorEmail"|cut -d">" -f11|cut -d"<" -f1`
					echo $MAIL_ID1
			MAIL_ID="chitta.sahoo@ge.com"
			#MAIL_ID="sasmita1.behera@ge.com"
		#	cat $ERROR_FILE | mailx -s "File $FILE Got Rejected" "$MAIL_ID"
				#(echo "Please find attached the file containing the reasons for rejections" ; uuencode $ERROR_FILE $ERROR_FILE_NAME;)| mailx -s "File $FILE_TYPE Got Rejected" "$MAIL_ID"	
			mv $SRC_PATH/"$FILE_NAME".csv $ARCHIVE_PATH/CSV
			cd $SRC_PATH_XLS
			mv $SRC_PATH/"$FILE_NAME".xml $ARCHIVE_PATH/XLSFiles
		fi
									
	fi	
echo "VALIDATED" $FILE
done
fi

chmod 777 $SOURCE_FILES/Indirect_BACKLOG.txt
