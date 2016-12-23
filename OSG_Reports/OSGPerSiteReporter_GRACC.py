#!/usr/bin/python

#Comments:
# do VOs.tolower()


import os
import inspect
import traceback
import sys
import datetime
import json

from elasticsearch_dsl import Search


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


def monthrange(date):
    """ Pass in date list (as returned by TimeUtils.dateparse) or
    datetime.datetime object.

    Returns tuple of datetime.datetime objects that span the month
    (2016-12-05 --> 2016-12-01, 2016-12-31)
    """
    if isinstance(date, list):
        startlist = datetime.datetime(*date)
    start = startlist.replace(day=1)
    nextmonth_first = (startlist.replace(day=28) +
                       datetime.timedelta(days=4)).replace(day=1)
    end = nextmonth_first - datetime.timedelta(days=1)
    return start, end


def prev_month_shift(date):
    """Takes a datetime.datetime object and return two datetime.datetime
    objects that span the range of the _previous_ month
    (2016-12-05 --> 2016-11-01, 2016-11-30)
    """
    sd = date.replace(day=1)-datetime.timedelta(days=1)
    return monthrange(sd)


def perc_change(old, new):
    """Calculates the percentage change between two numbers"""
    try:
        return (float((new - old) / old)) * 100.
    except ZeroDivisionError:
        if new == 0:
            return 0.
        else:
            return 100.


class VO(object):
    def __init__(self, voname):
        self.name = voname
        self.sites = {}
        self.current = True
        self.pos = 0
        self.total = [0,0]
        self.totalcalc = self.running_totalhours()
        self.totalcalc.send(None)

    def add_site(self, sitename, corehours):
        if not self.current: self.pos = 1

        if sitename in self.sites:
            self.sites[sitename][self.pos] += corehours
        else:
            self.sites[sitename] = [0, 0]
            self.sites[sitename][self.pos] = corehours

        self.totalcalc.send(corehours)
        return

    def running_totalhours(self):
        while True:
            newhrs = yield
            self.total[self.pos] += newhrs

    def get_cur_sitehours(self, sitename):
        return self.sites[sitename][0]

    def get_cur_totalhours(self):
        return self.total[0]

    def get_old_sitehours(self, sitename):
        if sitename in self.sites:
            return self.sites[sitename][1]
        else:
            return 0

    def get_old_totalhours(self):
        return self.total[1]


class OSGPerSiteReporter(Reporter):
    def __init__(self, configuration, start, end, template=False,
                     verbose=False, is_test=False, no_email=False):
        report = 'siteusage'
        Reporter.__init__(self, report, configuration, start, end=end,
                          verbose=verbose, is_test=is_test, no_email=no_email,
                          logfile=logfile, raw=False)
        self.header = ["Site", "Total", "Opportunistic Total",
                       "Percent Opportunistic", "Prev. Month Opp. Total",
                       "Percentage Change Month-Month"]

        self.start_time, self.end_time = \
            monthrange(self.dateparse(self.start_time))

        try:
            self.client = self.establish_client()
        except Exception as e:
            self.logger.exception(e)

        self.title = 'VOs Usage of OSG Sites: {0} - {1}'.format(
            self.start_time, self.end_time)
        self.current = True
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
            .bucket('site_bucket', 'terms',
                    script={"inline": "doc['OIM_Site'].value ?: doc['SiteName'].value", "lang": "painless"},
                    size=2**31-1) \
            .metric('sum_core_hours', 'sum', field='CoreHours')

        return s

    def generate(self):
        qresults = self.query()

        t = qresults.to_dict()
        if self.verbose:
            print json.dumps(t, sort_keys=True, indent=4)
            self.logger.debug(json.dumps(t, sort_keys=True))
        else:
            self.logger.debug(json.dumps(t, sort_keys=True))

        response = qresults.execute()
        results = response.aggregations

        consumer = self.create_vo_objects()
        consumer.send(None)

        for vo_bucket in results.vo_bucket.buckets:
            vo = vo_bucket['key'].lower()
            for site_bucket in vo_bucket.site_bucket.buckets:
                site = site_bucket['key']
                wallhrs = site_bucket['sum_core_hours']['value']
                consumer.send((vo, site, wallhrs))

        self.current = False
        self.start_time, self.end_time = prev_month_shift(self.start_time)

        oldqresults = self.query()

        t = oldqresults.to_dict()
        if self.verbose:
            print json.dumps(t, sort_keys=True, indent=4)
            self.logger.debug(json.dumps(t, sort_keys=True))
        else:
            self.logger.debug(json.dumps(t, sort_keys=True))

        response = oldqresults.execute()
        oldresults = response.aggregations

        for vo_bucket in oldresults.vo_bucket.buckets:
            vo = vo_bucket['key'].lower()
            for site_bucket in vo_bucket.site_bucket.buckets:
                site = site_bucket['key']
                wallhrs = site_bucket['sum_core_hours']['value']
                consumer.send((vo, site, wallhrs))

    def create_vo_objects(self):
        while True:
            vo, site, wallhrs = yield
            # print vo, site, wallhrs
            if self.current:
                if vo not in self.vodict:
                    V = VO(vo)
                    self.vodict[vo] = V
                V.add_site(site, wallhrs)

                if site not in self.sitelist:
                    self.sitelist.append(site)

            else:
                if vo not in self.vodict or site not in self.sitelist:
                    continue
                V = self.vodict[vo]
                V.current = self.current
                V.add_site(site, wallhrs)

    def format_report(self):
        report = {}
        sitelist = sorted(self.sitelist)

        report["Site"] = [site for site in sitelist]
        inspos = 2
        for vo, vo_object in sorted(self.vodict.iteritems()):
            if vo not in self.header:
                if vo in opp_vos:
                    # Insert the vos into the header in order after the Site
                    # and Total columns
                    self.header.insert(inspos, vo)
                    inspos += 1
                else:
                    self.header.append(vo)

            curvo = self.vodict[vo]
            report[vo] = [curvo.get_cur_sitehours(site)
                          if site in curvo.sites else 0 for site in sitelist]
            report[vo].append(vo_object.get_cur_totalhours())

        # Column for Previous Month Opportunistic Total
        report["Prev. Month Opp. Total"] = [
            sum((self.vodict[col].get_old_sitehours(site)
                 for col in report if col in opp_vos))
            for site in self.sitelist]
        stagecol = report["Prev. Month Opp. Total"]
        stagecol.append(sum(stagecol))


        report["Site"].append("Total")  # Add "Total" line at bottom of report

        # This is the per-site total column, not the same "Total" as just above
        report["Total"] = [sum((report[col][pos] for col in report
                                if col not in ("Site", "Total", "Percentage Change Month-Month")))
                           for pos in range(len(report["Site"]))]

        report["Opportunistic Total"] = [sum((report[col][pos]
                                              for col in report
                                              if col in opp_vos))
                                         for pos in range(len(report["Site"]))]

        report["Percent Opportunistic"] = [report["Opportunistic Total"][pos] /
                                           report["Total"][pos] * 100
                                           for pos in
                                           range(len(report["Site"]))]

        # Percent Change Month-Month
        report["Percentage Change Month-Month"] = [
            perc_change(report["Prev. Month Opp. Total"][pos],
                        report["Opportunistic Total"][pos])
            for pos in range(len(report["Site"]))]

        # In any modifications, the order of the next three sections must be
        # preserved

        # Month-over-month difference by VO
        for col, values in report.iteritems():
            # Insert a blank line
            # values.insert(-2,'')
            if col == "Site":
                values.append("Prev. Month Total")
                values.append("Percent Change over Prev. Month")
            elif col in self.vodict:
                values.append(self.vodict[col].get_old_totalhours())    # Total
                values.append(perc_change(values[-1], values[-2]))    # Percent change

        # Handle Opp. Total Prev. Month
        stagecol = report['Opportunistic Total']
        stagecol.append(sum((report[vo][-2] for vo in report if vo in opp_vos)))
        stagecol.append(perc_change(stagecol[-1], stagecol[-2]))

        # Handle total column
        stagecol = report['Total']
        stagecol.append(sum((report[vo][-2] for vo in self.vodict)))
        stagecol.append(perc_change(stagecol[-1], stagecol[-2]))

        # Handle percent opportunistic
        report['Percent Opportunistic'].extend(((report["Opportunistic Total"][-1] /
                                               report["Total"][-1] * 100), 'N/A'))

        # Handle Prev Month Columns
        # for column in ("Prev. Month Opp. Total"):
        #     report[column].extend(('N/A', 'N/A'))

        report["Prev. Month Opp. Total"].extend(('N/A', 'N/A'))
        report["Percentage Change Month-Month"].extend(('N/A', 'N/A'))

        # Insert a blank line
        for _, values in report.iteritems():
            values.insert(-3, '')

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
