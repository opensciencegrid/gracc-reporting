#!/bin/sh

# Wrapper script to run the OSG Probe report
# Example:  ./probereport_run.sh

# This assumes you're running the reports from a virtualenv

LOGFILE=/var/tmp/gracc-reporting/probereport_run.log # Ideally should be in /var/log/gracc-reporting
VENVDIR=gracc_venv
TOPDIR=/home/sbhat/gracc-reporting

function usage {
    echo "Usage:    ./probereport_run.sh"
    echo ""
    exit
}

# Initialize everything
# Check arguments
if [[ $# -ne 0 ]] || [[ $1 == "-h" ]] || [[ $1 == "--help" ]] ;
then
    usage
fi

STARTDATE=`date --date='2 day ago' +"%F %T"`

# Activate the virtualenv
cd $TOPDIR
source $VENVDIR/bin/activate

# Run the Report
echo "START" `date` >> $LOGFILE

osgprobereport

# Error Handling
if [ $? -ne 0 ]
then
    echo "Error running report.  Please try running the report manually" >> $LOGFILE
else
    echo "Ran report script" >> $LOGFILE
fi

echo "END" `date` >> $LOGFILE