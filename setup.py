"""Setup file for gracc-reporting"""
import sys
from setuptools import setup

# Enforce python version
VERSION_TUPLE = (2, 7)
if sys.version_info < VERSION_TUPLE:
    print "Sorry, installing gracc-reporting requires Python {0}.{1} " \
          "or above".format(*VERSION_TUPLE)
    exit(1)

setup(name='gracc-reporting',
      version='2.0.1',
      description='GRACC Email Reports',
      author_email='sbhat@fnal.gov',
      author='Shreyas Bhat',
      url='https://github.com/opensciencegrid/gracc-reporting',
      packages=['gracc_reporting'],
      install_requires=['elasticsearch==5.5.2', 'elasticsearch_dsl==5.4.0',
                        'python-dateutil==2.7.2', 'toml==0.9.4',]
     )
