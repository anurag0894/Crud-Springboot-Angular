#!/bin/bash

debug() 
{
    #echo "[${USER}][`date "+%F_%T"`] - ${*}" >> ${LOG_FILE}
    echo "[DBG `date "+%Y%m%d_%H%M%S"`] - ${*}" 
}

CURR_DIR=$(dirname $0)/..
echo "Running from $CURR_DIR"

#Check for LOG FILE [e.g /bnsf/hd/logs/mechanical/raw/ioc/data-streaming.log] from start script and retrieve appln id
LOG_FILE=$(cat ${CURR_DIR}/config/wild.properties | grep receiver.log.file.name | cut -d'=' -f2 | tr -d '\r')

if [ -f $LOG_FILE ]
then
  #appId=`grep "INFO YarnClientImpl: Submitted application" $LOG_FILE | tr -s '  ' ' ' | cut -d' ' -f7`

#Look for below line in log
#INFO impl.YarnClientImpl: Submitted application application_1490965549938_0179
  #appId=`grep "INFO impl.YarnClientImpl: Submitted application" $LOG_FILE | tr -s '  ' ' ' | cut -d' ' -f5`
  appId=`grep "INFO .*YarnClientImpl: Submitted application" $LOG_FILE | grep -oE '[^ ]+$' `

  debug "Found $LOG_FILE file, found running application Id : $appId"
else
  debug "Unable to find the log file $LOG_FILE "
fi

if [ -e $LOG_FILE ]
then
  newFile=$LOG_FILE.`date "+%Y%m%d_%H%M%S"`
  mv $LOG_FILE $newFile
  debug "Renamed $LOG_FILE to $newFile"
fi

debug "Stopping the Streaming Appln.. "
debug "Creating the receiver stop file to signal Receiver to STOP.. "
touch /tmp/receiver.stop

hdfs dfs -put /tmp/receiver.stop /bnsf/hd/mechanical/prep/ioc/detector/stop/receiver.stop
debug "Created the receiver.stop file ..awaiting Receiver to stop.. Please check the Streaming TAB on YARN RM or in GEMS..to see if it has stopped receiving messages.."

debug "Sleeping for next 2 minutes.."
sleep 120

#Wait for receiver to stop, so better to sleep before running the kill command
debug "In case there is no activity on Streaming TAB, JOBs/Stages TABs on YARN RM and in GEMS, please use the 'yarn application -kill $appId ' to stop the JOB"

