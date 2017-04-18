from setuptools import setup

setup(name='gracc-reporting',
      version='0.1dev',
      description='GRACC Email Reports',
      author_email='sbhat@fnal.gov',
      author='Shreyas Bhat',
      url='https://github.com/opensciencegrid/gracc-reporting',
      package_dir={'': 'src'},
      packages=['reports', 'reports.fife_reports', 'reports.osg_reports'],
      include_package_data=True,
      package_data={'reports': ['config/*.config', 'html_templates/*.html']},
      entry_points= {
          'console_scripts': [
              'efficiencyreport = reports.fife_reports.EfficiencyReporterPerVO:main',
              'jobsuccessratereport = reports.fife_reports.JobSuccessRateReport:main',
              'wastedhoursreport = reports.fife_reports.WastedHoursReport:main',
              'osgflockingreport = reports.osg_reports.OSGFlockingReporter:main',
              'osgreport = reports.osg_reports.OSGReporter:main',
              'osgpersitereport = reports.osg_reports.OSGPerSiteReporter:main',
              'probereport = reports.osg_reports.ProbeReport:main',
              'topoppusagereport = reports.osg_reports.TopOppUsageByFacility:main'
            ]
      }
      )