#!/bin/sh

VOS="UBooNE NOvA DUNE Mu2e SeaQuest"
YESTERDAY=`date --date yesterday +"%F %T"`
TODAY=`date +"%F %T"`

cd /home/gratia/gracc_email_reports 

echo "START" `date` >> jobsuccessratereport_run.log

for vo in ${VOS}
do
	echo $vo
	python JobSuccessReport.py -c jobrate.config -E $vo -s "$YESTERDAY" -e "$TODAY" -T template_jobrate.html
	if [ $? -ne 0 ]
	then 
		echo "Error running report for $vo.  Please try running the report manually" >> jobsuccessratereport_run.log
	else
		echo "Sent report for $vo" >> jobsuccessratereport_run.log

	fi
done

 
echo "END" `date` >> jobsuccessratereport_run.log

