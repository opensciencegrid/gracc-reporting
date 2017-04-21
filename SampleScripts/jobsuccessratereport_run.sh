#!/bin/sh

# Wrapper script to run the Job Success Rate report for all VOs
# Example:  ./jobsuccessratereport_run.sh

# This assumes you're running the reports from a virtualenv

TOPDIR=/home/sbhat/gracc-reporting
LOGFILE=/var/log/gracc-reporting/jobsuccessratereport_run.log     # Ideally should be in /var/log/gracc-reporting
VENVDIR=gracc_venv

function usage {
    echo "Usage:    ./jobsuccessratereport_run.sh "
    echo ""
    exit
}

# Initialize everything
# Check arguments
if [[ $# -ne 0 ]] || [[ $1 == "-h" ]] || [[ $1 == "--help" ]] ;
then
    usage
fi

# Set script variables

VOS="UBooNE NOvA DUNE Mu2e SeaQuest"
YESTERDAY=`date --date yesterday +"%F %T"`
TODAY=`date +"%F %T"`


# Activate the virtualenv
cd $TOPDIR
source $VENVDIR/bin/activate


# Run the report
echo "START" `date` >> $LOGFILE

for vo in ${VOS}
do
	echo $vo
	jobsuccessratereport -E $vo -s "$YESTERDAY" -e "$TODAY"

    # Error handling
	if [ $? -ne 0 ]
	then 
		echo "Error running report for $vo.  Please try running the report manually" >> $LOGFILE
	else
		echo "Sent report for $vo" >> $LOGFILE

	fi
done
 
echo "END" `date` >> $LOGFILE

