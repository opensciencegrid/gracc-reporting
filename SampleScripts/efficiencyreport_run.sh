#!/bin/sh

# Wrapper script to run the Efficiency report
# Example:  ./efficiencyreport_run.sh daily UBooNe

# Valid VOS="NOvA SeaQuest MINERvA MINOS gm2 Mu2e UBooNe DarkSide DUNE CDMS MARS CDF"
# This assumes you're running the reports from a virtualenv


TOPDIR=/home/sbhat/gracc-reporting
LOGFILE=/var/tmp/gracc-reporting/efficiencyreport_run.log     # Ideally should be in /var/log/gracc-reporting
VENVDIR=gracc_venv

function usage {
    echo "Usage:    ./efficiencyreport_run.sh <time period> <VO>"
    echo ""
    echo "Time periods are: daily, weekly, bimonthly, monthly, yearly"
    exit
}

function set_dates {
        case $1 in
                "daily") starttime=`date --date='1 day ago' +"%F %T"`;;
                "weekly") starttime=`date --date='1 week ago' +"%F %T"`;;
                "bimonthly") starttime=`date --date='2 month ago' +"%F %T"`;;
                "monthly") starttime=`date --date='1 month ago' +"%F %T"`;;
                "yearly") starttime=`date --date='1 year ago' +"%F %T"`;;
                *) echo "Error: unknown period $1. Use weekly, monthly or yearly"
                         exit 1;;
        esac
        echo $starttime
}

# Initialize everything
# Check arguments
if [[ $# -ne 2 ]] || [[ $1 == "-h" ]] || [[ $1 == "--help" ]] ;
then
    usage
fi

endtime=`date +"%F %T"`

vo=$2
set_dates $1


# Activate the virtualenv
cd $TOPDIR
source $VENVDIR/bin/activate


# Run the report
echo "START" `date` >> $LOGFILE

efficiencyreport -F GPGrid -E $vo -s "$starttime" -e "$endtime"

# Error handling
if [ $? -ne 0 ]
then
	echo "Error sending report for $vo . Please investigate" >> $LOGFILE
else
	echo "Sent report for $vo" >> $LOGFILE
fi
 
echo "END" `date` >> $LOGFILE
