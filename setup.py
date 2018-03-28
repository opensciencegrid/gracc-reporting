from setuptools import setup
import sys

# Enforce python version
version_tuple = (2, 7)
if sys.version_info < version_tuple:
    print "Sorry, installing gracc-reporting requires Python {0}.{1} " \
          "or above".format(*version_tuple)
    exit(1)

setup(name='gracc-reporting',
      version='1.2',
      description='GRACC Email Reports',
      author_email='sbhat@fnal.gov',
      author='Shreyas Bhat',
      url='https://github.com/opensciencegrid/gracc-reporting',
      packages=['gracc_reporting']
      )
