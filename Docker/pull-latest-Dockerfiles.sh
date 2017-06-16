#!/bin/sh

FILES=`ls -dl $HOME/GRACC-Reporting_Docker/*/Dockerfile | awk '{print $9}'`  

for FILE in $FILES
do 
	echo $FILE 
	TYPE=`echo $FILE | cut -f 5 -d '/'` 
	cp $FILE ./${TYPE}_Dockerfile
done

