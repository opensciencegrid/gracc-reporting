#!/bin/sh

# Wrapper script to run the Minerva report
# Example:  ./minerva_report_hourly.sh

# This assumes you're running the reports from a virtualenv

TOPDIR=/home/sbhat/gracc-reporting
LOGFILE=/var/log/gracc-reporting/minervareport_run.log     # Ideally should be in /var/log/gracc-reporting
CONFIGFILE=/etc/gracc-reporting/config/minerva.config
VENVDIR=gracc_venv

function usage {
    echo "Usage:    ./minerva_report_hourly.sh "
    echo ""
    exit
}

# Initialize everything
# Check arguments
if [[ $# -ne 0 ]] || [[ $1 == "-h" ]] || [[ $1 == "--help" ]] ;
then
    usage
fi

# Activate the virtualenv
cd $TOPDIR
source $VENVDIR/bin/activate


# Run the report
echo "START HOURLY RUN" `date` >> $LOGFILE

minervareport -c $CONFIGFILE -d -a

# Error handling
if [ $? -ne 0 ]
then
    echo "Error running minerva report.  Please try running the report manually" >> $LOGFILE
else
    echo "Sent minerva report" >> $LOGFILE
fi

echo "END" `date` >> $LOGFILE



