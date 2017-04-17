Example runs:

Project Report:
./OSGReporter -c osg.config -s 2016-12-06 -e 2016-12-13 -r OSG-Connect -T template_project.html -d -v -n

Missing Projects report (now run automatically from Project report):
python MissingProject.py -c osg.config -s 2016-12-06 -e 2017-01-31 -r XD -d -n -v

per site report
python OSGPerSiteReporter.py -s 2016/10/01 -c osg.config -T template_persite.html -d -n -v

Flocking report:
python OSGFlockingReporter.py -s 2016-11-09 -e 2016-11-16 -c osg.config -T template_flocking.html -d -v -n

Probe Report:
python ProbeReport.py -c osg.config -d -n -v

News Report (Top Opportunistic Usage per Facility)
Monthly:
    python TopOppUsageByFacility.py -m 2 -T template_topoppusage.html -N 20 -c osg.config -d -v -n
Absolute dates:
    python TopOppUsageByFacility.py -s "2016-12-01" -e "2017-02-01" -T template_topoppusage.html -N 20 -c osg.config -d -v -n
