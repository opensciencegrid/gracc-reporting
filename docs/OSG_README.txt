Example runs:
For each report, you can specify a non-standard location for the config file with -c, or for a template file with -T.  The defaults are in src/reports/config and src/reports/html_templates.
The -d, -n, and -v flags are, respectively, dryrun (test), no email, and verbose.

Project Report:
osgreport -s 2016-12-06 -e 2016-12-13 -r OSG-Connect -d -v -n   # No missing projects
osgreport -s 2016-12-06 -e 2016-12-13 -r XD -d -v -n   # Missing projects


Missing Projects report (now run automatically from Project report):
python MissingProject.py -s 2016-12-06 -e 2017-01-31 -r XD -d -n -v

Opp Usage per site report
osgpersitereport -s 2016/10/01 -d -v -n

Flocking report:
osgflockingreport -s 2016-11-09 -e 2016-11-16 -d -v -n

Probe Report:
osgprobereport -d -n -v

News Report (Top Opportunistic Usage per Facility)
Monthly:
    osgtopoppusagereport -m 2 -N 20 -d -v -n
Absolute dates:
    osgtopoppusagereport -s "2016-12-01" -e "2017-02-01" -N 20 -d -v -n
