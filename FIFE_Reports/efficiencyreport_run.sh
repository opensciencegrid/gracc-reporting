#!/bin/sh

# VOS="NOvA SeaQuest MINERvA MINOS gm2 Mu2e UBooNe DarkSide DUNE CDMS MARS CDF" 
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

vo=$2
set_dates $1

TOPDIR=/home/sbhat/gracc-reporting

cd $TOPDIR
source gracc_venv/bin/activate
PYTHON=`which python`

cd FIFE_Reports

echo "START" `date` >> efficiencyreport_run.log

$PYTHON EfficiencyReporterPerVO.py -F GPGrid -c efficiency.config -E $vo -s "$starttime" -e "$endtime" -T template_efficiency.html

if [ $? -ne 0 ]
then
	echo "Error sending report for $vo . Please investigate" >> efficiencyreport_run.log
else
	echo "Sent report for $vo" >> efficiencyreport_run.log
fi
 
echo "END" `date` >> efficiencyreport_run.log
