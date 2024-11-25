"""Setup file for gracc-reporting"""
import sys
from setuptools import setup

setup(name='gracc-reporting',
      version='3.5.0',
      description='GRACC Email Reports',
      author_email='sbhat@fnal.gov',
      author='Shreyas Bhat',
      url='https://github.com/opensciencegrid/gracc-reporting',
      packages=['gracc_reporting'],
      install_requires=['opensearch-py',
                        'python-dateutil', 'toml', 'tabulate',
                        'pandas']
     )
