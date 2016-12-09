#!/bin/sh

STARTDATE=`date --date='2 day ago' +"%F %T"`

# cd /home/gratia/gracc_email_reports/OSG_Reports
cd /cloud/login/sbhat/EmailReports/OSG_Reports

#echo "START" `date` >> probereport_run.log

python ProbeReport.py -c probereport.config -d 

if [ $? -ne 0 ]
then
    echo "Error running report.  Please try running the report manually" >> probereport_run.log
else
    echo "Ran report script" >> probereport_run.log
fi


echo "END" `date` >> probereport_run.log
