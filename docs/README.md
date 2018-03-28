What is this package?
=====================

*gracc-reports* is a set of reports that collect and present data from
the Open Science Grid accounting system GRACC.  It is meant to replace the old 
GRATIA reports.

The reports are as follows (with the corresponding executables following them):

FIFE Reports (Fermilab, in the fife_reports subpackage):
   - Job Success Rate Report (jobsuccessratereport)
   - User Efficiency Report (efficiencyreport)
   - Wasted Hours by User Report (wastedhoursreport)

General OSG Reports (osg_reports subpackage):
   - OSG Flocking Report (osgflockingreport)
   - OSG Project Usage Report (osgreport)
   - OSG Missing Projects from Records report (osgmissingprojects)
   - OSG Usage Per Site Report (osgpersitereport)
   - Top [N] Providers of Opportunistic Hours on the OSG (News Report) (osgtopoppusagereport)
   - Gratia Probes that haven't reported in the past two days (osgprobereport)


Installation
============

To set up gracc-reporting within a virtual environment:

Make sure you have the latest version of [pip.](https://pip.pypa.io/en/stable/installing/#do-i-need-to-install-pip)

Then:
Make sure pip is up to date:
```
   pip install -U pip
```
Install virtualenv if you haven't already:
```
   pip install virtualenv
```
The first time you do this:
```
   virtualenv gracc_venv                # Or whatever other name you want to give your virtualenv instance
   source gracc_venv/bin/activate       # Activate the virtualenv
   pip install -r requirements.txt      # Install the package requirements
   python setup.py install              # Install gracc-reports
```
Then, to access this sandbox later, go to the dir with gracc_venv in it, and:
```
   source gracc_venv/bin/activate
```
and do whatever you need!  If you can't run pip installs on your machine,
then if you have virtualenv, activate it and then upgrade pip and install the 
requirements.


Configuration Files
===================

The configuration files are located within this package by default, keeping
in line with the python setuptools convention.  The reports will automatically
find the correct config files within the package by default.  There are symlinks to those files
in the top-level config/ directory, so they can be changed.

There is also an included executable, copyfiles, that will copy the configuration
and HTML template files to /etc/gracc-reporting.  Running this is optional - the
reports will try to look there and in the package.

You can also specify non-standard (not in /etc/gracc-reporting or within the
package) locations for the config and template files by passing those in with the
-c and -T flags respectively.  For example, you could run:
```
   osgtopoppusagereport -m 2 -N 20 -c /path/to/config/file.config -T /path/to/html/template.html
```

Running Specific Reports
========================

For examples of how to run specific reports, see the FIFE_README.txt or OSG_README.txt
files in the [docs](https://github.com/shreyb/gracc-reporting/tree/master/docs) directory.

Some examples of scripts that run reports automatically within virtualenvs are in the [SampleScripts](https://github.com/shreyb/gracc-reporting/tree/master/SampleScripts) directory.
