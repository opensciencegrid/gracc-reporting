#!/bin/sh

LOGFILE=osgpersite_run.log
TOPDIR=/home/sbhat/gracc-reporting
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

cd $TOPDIR
source gracc_venv/bin/activate
PYTHON=`which python`

cd OSG_Reports

echo "START" `date` >> $LOGFILE

$PYTHON OSGPerSiteReporter.py -s "$starttime" -c osg.config -T template_siteusage.html


if [ $? -ne 0 ]
then
	echo "Error sending report. Please investigate" >> $LOGFILE
else
	echo "Sent report" >> $LOGFILE
fi
 
echo "END" `date` >> $LOGFILE
