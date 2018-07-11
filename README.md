What is this package?
=====================

*gracc-reporting* is a set of libraries that reports can be built on to collect
and present data from the Open Science Grid accounting system GRACC.  It is meant to 
be part of a system that replaces the old GRATIA reports.

No reports are included with this package, but can be built by creating a class that 
is a subclass of the ReportUtils.Reporter class in this package.  For examples, please
see the separate gracc-osg-reports package.

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
   python setup.py install              # Install gracc-reporting
```
Then, to access this sandbox later, go to the dir with gracc_venv in it, and:
```
   source gracc_venv/bin/activate
```
and do whatever you need!  If you can't run pip installs on your machine,
then if you have virtualenv, activate it and then upgrade pip and install the 
requirements.

