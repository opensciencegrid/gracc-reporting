from setuptools import setup
import sys

# Enforce python version
version_tuple = (2, 7)
if sys.version_info < version_tuple:
    print "Sorry, installing gracc-reporting requires Python {0}.{1} " \
          "or above".format(*version_tuple)
    exit(1)

setup(name='gracc-reporting',
      version='0.5',
      description='GRACC Email Reports',
      author_email='sbhat@fnal.gov',
      author='Shreyas Bhat',
      url='https://github.com/opensciencegrid/gracc-reporting',
      package_dir={'': 'src'},
      packages=['graccreports', 'graccreports.fife_reports', 'graccreports.osg_reports', 'graccreports.minerva_report'],
      include_package_data=True,
      package_data={'graccreports': ['config/*.config', 'html_templates/*.html']},
      entry_points= {
          'console_scripts': [
              'efficiencyreport = graccreports.fife_reports.EfficiencyReporterPerVO:main',
              'jobsuccessratereport = graccreports.fife_reports.JobSuccessRateReport:main',
              'wastedhoursreport = graccreports.fife_reports.WastedHoursReport:main',
              'osgflockingreport = graccreports.osg_reports.OSGFlockingReporter:main',
              'osgreport = graccreports.osg_reports.OSGReporter:main',
              'osgpersitereport = graccreports.osg_reports.OSGPerSiteReporter:main',
              'osgprobereport = graccreports.osg_reports.ProbeReport:main',
              'osgtopoppusagereport = graccreports.osg_reports.TopOppUsageByFacility:main'
              'osgmissingprojects = graccreports.osg_reports.MissingProject:main',
              'copyfiles = graccreports.copyfiles:main',
              'minervareport = graccreports.minerva_report.MinervaReport:main'
            ]
      }
      )