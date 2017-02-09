#!/bin/sh

LOGFILE=flocking_run.log
endtime=`date +"%F %T"`

function set_dates {
        case $1 in
                "daily") starttime=`date --date='1 day ago' +"%F %T"`;;
                "weekly") starttime=`date --date='1 week ago' +"%F %T"`;;
                "bimonthly") starttime=`date --date='2 month ago' +"%F %T"`;;
                "monthly") starttime=`date --date='1 month ago' +"%F %T"`;;
                "yearly") starttime=`date --date='1 year ago' +"%F %T"`;;
                *) echo "Error: unknown period $period. Use weekly, monthly or yearly"
                         exit 1;;
        esac
        echo $starttime
}

set_dates $1

echo "START" `date` >> $LOGFILE

python OSGFlockingReporter.py -s "$starttime" -e "$endtime" -c osg.config -T template_flocking.html -d 


if [ $? -ne 0 ]
then
	echo "Error sending report. Please investigate" >> $LOGFILE
else
	echo "Sent report" >> $LOGFILE
fi
 
echo "END" `date` >> $LOGFILE
