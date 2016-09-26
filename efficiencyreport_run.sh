#!/bin/sh

VOS="NOvA SeaQuest MINERvA MINOS gm2 Mu2e UBooNe DarkSide DUNE CDMS MARS CDF" 
YESTERDAY=`date --date yesterday +"%F %T"`
TODAY=`date +"%F %T"`



cd /home/sbhat/EfficiencyReport

echo "START" `date` >> efficiencyreport_run.log

for vo in ${VOS}
do
	echo $vo
	./EfficiencyReporterPerVO.py -F GPGrid -c efficiency.config -E $vo -s "$YESTERDAY" -e "$TODAY" -d
	echo "Sent report for $vo"
done

 
echo "END" `date` >> efficiencyreport_run.log

