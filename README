To set up gracc-reporting within a virtual environment:

Make sure you have the latest version of pip

https://pip.pypa.io/en/stable/installing/#do-i-need-to-install-pip

Then:
Upgrade Pip:
pip install -U pip

Install virtualenv:
pip install virtualenv

The first time you do this:

virtualenv gracc_venv
source gracc_venv/bin/activate
pip install -r requirements.txt


Then, to access this sandbox, go to the dir with gracc_venv in it, and:

source gracc_venv/bin/activate

and do whatever you need!  If you can't run pip installs on your machine,
then if you have virtualenv, activate it and then upgrade pip and install the
requirements.


# Automating virtual env to run reports on cron jobs

If you want to automate this, you can do something like the following:

#!/bin/sh

cd ~/gracc-reporting/
source gracc_venv/bin/activate

PYTHON=`which python`
cd <reports dir>
$PYTHON <report> <params>