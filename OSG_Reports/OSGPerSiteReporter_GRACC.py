import xml.etree.ElementTree as ET
from datetime import timedelta, date
import urllib2
import ast
import os
import inspect
import math
import re
import smtplib
import email.utils
from email.mime.text import MIMEText
import datetime
import logging
import json
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

class VO(object):
    def __init__(self, voname):
        self.name = voname
        self.sites = {}

    def add_site(self, sitename, corehours):
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
        self.header = ["Site", "Total"]
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

        s.aggs.bucket('vo_bucket', 'terms', field='VOName', size=1000000000)\
            .bucket('site_bucket', 'terms', script={"inline": "doc['OIM_Site'].value ?: doc['SiteName'].value", "lang":"painless"}, size=1000000000)\
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
        # report["Total"] = []

        for vo, vo_object in sorted(self.vodict.iteritems()):
            self.header.append(vo)
            curvo = self.vodict[vo]
            report[vo] = [curvo.getsitehours(site)
                          if site in curvo.sites else 0 for site in sitelist]

        report["Total"] = [sum((report[col][pos] for col in report
                                if col not in ("Site", "Total")))
                           for pos in range(len(self.sitelist))]
        # print report
        # print len(self.vodict)
        return report
            # for site in sitelist:
            #     if site in curvo.sites:
            #         report[vo].append(curvo.getsitehours(site))
            #     else:
            #         report[vo].append(0)


        #
        # for result_tuple in self.generate():
        #     vo, site, wallhrs = result_tuple
        #
        #     if vo not in self.header:
        #         self.header.append(vo)
        #         report[vo] = []
        #
        #     report["Site"].append(site)
        #     report[vo]
        #


        #
        #
        #
        #
        # for result_tuple in self.generate():
        #     vo, site, probe, project, wallhours = result_tuple
        #     if self.verbose:
        #         print "{0}\t{1}\t{2}\t{3}\t{4}".format(vo, site, probe,
        #                                                project, wallhours)
        #     report["VOName"].append(vo)
        #     report["SiteName"].append(site)
        #     report["ProbeName"].append(probe)
        #     report["ProjectName"].append(project)
        #     report["Wall Hours"].append(wallhours)
        # return report
        #
        #
        # for item in self.generate():
        #     vo, site, wallhrs = item
        #
        #     if vo not in results_dict:
        #         results_dict[item[0]] = {}
        #     results_dict[item[0]][item[1].upper()] = \
        #         int(round(item[2]/3600.,0))
        #
        # siteset = set([site for sites in results_dict.itervalues() for site in sites])
        #
        # for site in siteset:
        #     for vo, sites in results_dict.iteritems():
        #         if site not in sites:
        #             results_dict[vo][site] = 0
        #
        # siteset = sorted(siteset)
        #
        # for vo, sites in results_dict.iteritems():
        #     print "VO: {0}".format(vo)
        #     for site in siteset:
        #         print "\tSite: {0}, Wall Hours: {1}".format(site, sites[site])
        #
        # for vo, sites in results_dict.iteritems():
        #     # print "Site: {0}".format(site)
        #     listnew = []
        #     results_dict[vo] = [sites[site] for site in siteset]
        #     results_dict[vo].insert(0,sum(results_dict[vo]))
        #     # Need to calculate total, put it in the beginning
        #     # print siteset
        #     # print results_dict[vo]
        #     # for vo in voset:
        #     #     print "\tVO: {0}, Wall Hours: {1}".format(vo, vos[vo])
        #     #     listnew.append(vos[vo])
        #     # results_dict[site] = listnew
        #                 # for vo,walldur in vos.iteritems():
        #         # print "\tVO: {0}, Wall Hours: {1}".format(vo, walldur)
        #
        # volist = sorted(list(results_dict.keys()))
        #
        # results_dict['Site'] = [site for site in siteset]
        # results_dict['Total'] = [0 for _ in siteset]     # for now
        #
        # self.header = self.header + volist
        # print results_dict
        # return results_dict

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
        # print osgreport.format_report()
        # osgreport.send_report("siteusage")
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
