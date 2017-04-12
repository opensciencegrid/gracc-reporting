from setuptools import setup

setup(name='gracc-reporting',
        version='0.1dev',
        description='GRACC Email Reports',
        author_email='sbhat@fnal.gov',
        author='Shreyas Bhat',
        url='https://github.com/opensciencegrid/gracc-reporting',
        package_dir={'': 'src'},
        packages=['reports', 'reports.fife_reports', 'reports.osg_reports'],
        entry_points= {
            'console_scripts': [
                'efficiencyreport = reports.fife_reports.EfficiencyReporterPerVO:main'
            ]
        }
      )