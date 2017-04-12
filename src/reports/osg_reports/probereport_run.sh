#!/bin/sh

STARTDATE=`date --date='2 day ago' +"%F %T"`
TOPDIR=/home/sbhat/gracc-reporting

# cd /home/gratia/gracc_email_reports/OSG_Reports

#echo "START" `date` >> probereport_run.log

cd $TOPDIR
source gracc_venv/bin/activate
PYTHON=`which python`

cd OSG_Reports

$PYTHON ProbeReport.py -c osg.config

if [ $? -ne 0 ]
then
    echo "Error running report.  Please try running the report manually" >> probereport_run.log
else
    echo "Ran report script" >> probereport_run.log
fi

echo "END" `date` >> probereport_run.log