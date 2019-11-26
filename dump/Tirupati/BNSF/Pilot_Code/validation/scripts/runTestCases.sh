#!/bin/bash

debug()
{
     echo "[`date "+%Y-%m-%d %H:%M:%S"`] - ${*}"
}

debug 'Table to Test the Script is '$1
debug 'Load Type if it is Initial or incremental Test the Script is '$2
debug 'Source Type if it is Teradata or DB2 Test the Script is '$3
debug 'Start date for initial | Incremental Date to Test the Script is '$4
debug 'End date for initial | Target Date to Test the Incremental Script is '$5
debug 'check coloumn is '$6

if [ -z "$HD_APP_HOME" ]
then
	echo "HD_APP_HOME not set .. setting it to /bnsf/hd"
	HD_APP_HOME=/bnsf/hd
fi


#Define DATA_VALIDATION_HOME
if [ -z "$DATA_VALIDATION_HOME" ]
then
	echo "DATA_VALIDATION_HOME not set .. setting it to /bnsf/hd/data-validation"
	DATA_VALIDATION_HOME=$HD_APP_HOME/data-validation
fi

echo "Running command java -cp $DATA_VALIDATION_HOME -Dcfg.prop=$DATA_VALIDATION_HOME/config/data-validation.properties -jar $DATA_VALIDATION_HOME/lib/data-validation.jar $1 $2 $3 $4 $5 $6 "
java -cp $DATA_VALIDATION_HOME -Dcfg.prop=$DATA_VALIDATION_HOME/config/data-validation.properties -jar $DATA_VALIDATION_HOME/lib/data-validation.jar $1 $2 $3 "$4" "$5" "$6"
#java -cp $DATA_VALIDATION_HOME/lib/data-validation.jar:$DATA_VALIDATION_HOME/lib/* com.bnsf.ingestion.validation.ValidationBase $1 $2 $3 "$4" "$5" "$6"
