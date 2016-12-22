#!/usr/bin/python

import os
import inspect
import traceback
import sys

from elasticsearch_dsl import Search, Q


parentdir = os.path.dirname(
    os.path.dirname(
        os.path.abspath(
            inspect.getfile(
                inspect.currentframe()
            )
        )
    )
)
os.sys.path.insert(0, parentdir)

import Configuration
from Reporter import Reporter, runerror

logfile = 'osgpersitereport.log'
opp_vos = ['glow', 'gluex', 'hcc', 'osg', 'sbgrid']

class VO(object):
    def __init__(self, voname):
        self.name = voname

        # if voname.lower() in opp_vos:
        #     self.opportunistic = True
        # else:
        #     self.opportunistic = False

        self.sites = {}

    def add_site(self, sitename, corehours):
        if sitename in self.sites:
            self.sites[sitename] += corehours
        else:
            self.sites[sitename] = corehours

    def getsitehours(self, sitename):
        return self.sites[sitename]


class OSGPerSiteReporter(Reporter):
    def __init__(self, configuration, start, end, template=False,
                     verbose=False, is_test=False, no_email=False):
        report = 'siteusage'
        Reporter.__init__(self, report, configuration, start, end=end,
                          verbose=verbose, is_test=is_test, no_email=no_email,
                          logfile=logfile, raw=False)
        self.header = ["Site", "Total", "Opportunistic Total",
                       "Percent Opportunistic"]
        try:
            self.client = self.establish_client()
        except Exception as e:
            self.logger.exception(e)
        self.title = 'VOs Usage of OSG Sites: {0} - {1}'.format(
            self.start_time, self.end_time)
        self.vodict = {}
        self.sitelist = []

    def query(self):
        startdate = self.dateparse_to_iso(self.start_time)
        enddate = self.dateparse_to_iso(self.end_time)

        if self.verbose:
            self.logger.info(self.indexpattern)

        s = Search(using=self.client, index=self.indexpattern) \
            .filter("range", EndTime={"gte": startdate, "lt": enddate})\
            .filter('term', ResourceType="Batch")

        s.aggs.bucket('vo_bucket', 'terms', field='VOName', size=2**31-1) \
            .bucket('site_bucket', 'terms', script={"inline": "doc['OIM_Site'].value ?: doc['SiteName'].value", "lang": "painless"}, size=2**31-1) \
            .metric('sum_core_hours', 'sum', field='CoreHours')

        return s

    def generate(self):
        qresults = self.query().execute()
        results = qresults.aggregations

        consumer = self.create_vo_objects()
        consumer.send(None)

        for vo_bucket in results.vo_bucket.buckets:
            vo = vo_bucket['key']
            for site_bucket in vo_bucket.site_bucket.buckets:
                site = site_bucket['key']
                wallhrs = site_bucket['sum_core_hours']['value']
                consumer.send((vo, site, wallhrs))

    def create_vo_objects(self):
        while True:
            vo, site, wallhrs = yield
            # print vo, site, wallhrs
            if vo not in self.vodict:
                V = VO(vo)
                self.vodict[vo] = V
            V.add_site(site, wallhrs)

            if site not in self.sitelist:
                self.sitelist.append(site)


    def format_report(self):
        report = {}
        sitelist = sorted(self.sitelist)
        report["Site"] = [site for site in sitelist]

        for vo, vo_object in sorted(self.vodict.iteritems()):
            self.header.append(vo)
            curvo = self.vodict[vo]
            report[vo] = [curvo.getsitehours(site)
                          if site in curvo.sites else 0 for site in sitelist]

        report["Total"] = [sum((report[col][pos] for col in report
                                if col not in ("Site", "Total")))
                           for pos in range(len(self.sitelist))]

        report["Opportunistic Total"] = [sum((report[col][pos]
                                              for col in report
                                              if col in opp_vos))
                                         for pos in range(len(self.sitelist))]

        report["Percent Opportunistic"] = [
            report["Opportunistic Total"][pos] /
                  report["Total"][pos] * 100
            for pos in range(len(self.sitelist))]

        # Add total line at the bottom of report
        for col, values in report.iteritems():
            if col in ('Site', 'Percent Opportunistic'):
                continue
            values.append(sum(values))
        report["Site"].append("Total")
        report['Percent Opportunistic'].append(
            report['Opportunistic Total'][-1]/report['Total'][-1] * 100)

        return report


    def run_report(self):
        self.generate()
        self.send_report(title=self.title)


def main():
    args = Reporter.parse_opts()

    config = Configuration.Configuration()
    config.configure(args.config)

    try:
        osgreport = OSGPerSiteReporter(config,
                              args.start,
                              args.end,
                              template=args.template,
                              verbose=args.verbose,
                              is_test=args.is_test,
                              no_email=args.no_email)

        osgreport.run_report()
        print 'OSG Per Site Report Execution finished'
    except Exception as e:
        with open(logfile, 'a') as f:
            f.write(traceback.format_exc())
        print >> sys.stderr, traceback.format_exc()
        runerror(config, e, traceback.format_exc())
        sys.exit(1)

    return

if __name__ == '__main__':
    main()
    sys.exit(0)
