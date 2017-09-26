FIFE Reports
=====================================

For each report, you can specify a non-standard location for the config file with -c, or for a template file with -T.  The defaults are in [src/graccreports/config](https://github.com/shreyb/gracc-reporting/tree/master/src/graccreports/config) and [src/graccreports/html_templates](https://github.com/shreyb/gracc-reporting/tree/master/src/graccreports/html_templates).
The -d, -n, and -v flags are, respectively, dryrun (test), no email, and verbose.

Examples:

**Job Success Rate Report:**
```    
    jobsuccessratereport -E uboone -s '2017/04/03 06:30:01' -e '2017/04/04 06:30:01' -d -v -n
```

**User Efficiency Report:**
```
    efficiencyreport -E uboone -s 2016/07/04 -e 2016/07/05  -F GPGrid  -n -d -v
```

**Top Wasted Hours by VO Report:**
```    
    topwastedhoursvoreport -s "2017/08/01 00:00:00" -e "2017/08/31 00:00:00" -E nova -F GPGrid -d -n -v
```

Change the date/times, and the VO where applicable.  -v makes it verbose.
