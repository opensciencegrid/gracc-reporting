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


class OSGPerSiteReporter(Reporter):
    def __init__(self, configuration, start, end, template=False,
                     verbose=False, is_test=False, no_email=False):
        Reporter.__init__(self, configuration, start, end, verbose, is_test,
                          no_email)
        self.header = ["Site", "Total"]
        self.logfile = logfile
        self.logger = self.setupgenLogger("ProbeReport")
        try:
            self.client = self.establish_client()
        except Exception as e:
            self.logger.exception(e)
        self.title = 'VOs Usage of OSG Sites: {0} - {1}'.format(
            self.start_time, self.end_time)

    def query(self):
        startdate = self.dateparse_to_iso(self.start_time)
        enddate = self.dateparse_to_iso(self.end_time)

        s = Search(using=self.client, index='gracc.osg.summary*') \
            .filter(Q({"range": {"@received": {"gte": "{0}".format(startdate), "lt":"{0}".format(enddate)}}}))\
            .filter('term', ResourceType="Batch")

        s.aggs.bucket('vo_bucket', 'terms', field='VOName', size=1000000000)\
              .bucket('site_bucket', 'terms', field='Site', size=1000000000)\
              .metric('sum_wall_dur', 'sum', field='WallDuration')

        return s

    def generate(self):
        qresults = self.query().execute()
        results = qresults.aggregations

        for vo_bucket in results.vo_bucket.buckets:
            vo = vo_bucket['key']
            for site_bucket in vo_bucket.site_bucket.buckets:
                site = site_bucket['key']
                wallsec = site_bucket['sum_wall_dur']['value']
                yield [vo, site, wallsec]

    def generate_report_file(self):
        results_dict = {}
        for item in self.generate():
            if item[0] not in results_dict:
                results_dict[item[0]] = {}
            results_dict[item[0]][item[1].upper()] = \
                int(round(item[2]/3600.,0))

        siteset = set([site for sites in results_dict.itervalues() for site in sites])

        for site in siteset:
            for vo, sites in results_dict.iteritems():
                if site not in sites:
                    results_dict[vo][site] = 0

        siteset = sorted(siteset)

        for vo, sites in results_dict.iteritems():
            print "VO: {0}".format(vo)
            for site in siteset:
                print "\tSite: {0}, Wall Hours: {1}".format(site, sites[site])

        for vo, sites in results_dict.iteritems():
            # print "Site: {0}".format(site)
            listnew = []
            results_dict[vo] = [sites[site] for site in siteset]
            results_dict[vo].insert(0,sum(results_dict[vo]))
            # Need to calculate total, put it in the beginning
            # print siteset
            # print results_dict[vo]
            # for vo in voset:
            #     print "\tVO: {0}, Wall Hours: {1}".format(vo, vos[vo])
            #     listnew.append(vos[vo])
            # results_dict[site] = listnew
                        # for vo,walldur in vos.iteritems():
                # print "\tVO: {0}, Wall Hours: {1}".format(vo, walldur)

        volist = sorted(list(results_dict.keys()))

        results_dict['Site'] = [site for site in siteset]
        results_dict['Total'] = [0 for site in siteset]     # for now

        self.header = self.header + volist
        # print results_dict
        return results_dict

    def format_report(self):
        return self.generate_report_file()


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

        # osgreport.generate_report_file()
        # print osgreport.format_report()
        osgreport.send_report("siteusage")
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
