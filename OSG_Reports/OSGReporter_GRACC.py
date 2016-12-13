#!/usr/bin/python

import xml.etree.ElementTree as ET
from datetime import timedelta, date
import urllib2
import ast
import os
import inspect
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

logfile = 'osgreporter.log'

class OSGReporter(Reporter):
    def __init__(self, config, report_type, limit, start, end=None, isSum=True,
                 verbose=False):
        Reporter.__init__(self, config, start, end, verbose, raw=False)
        self.header=["Project Name","PI","Institution","Field of Science","Wall Hours"]
        self.logfile = logfile
        self.logger = self.setupgenLogger("ProbeReport")
        try:
            self.client = self.establish_client()
        except Exception as e:
            self.logger.exception(e)
        self.report_type = report_type
        self.limit = limit
        self.isSum = isSum
        if self.report_type == "OSG":
            self.title = "{0}-Direct Projects {1} - {2}".format(
                self.report_type, self.start_time, self.end_time)
        if self.report_type == "XD":
            self.title = "OSG-{0} Projects {1} - {2}".format(
            self.report_type, self.start_time, self.end_time)

    def query(self):
        """Method to query Elasticsearch cluster for OSGReporter information"""
        # Gather parameters, format them for the query
        starttimeq = self.dateparse_to_iso(self.start_time)
        endtimeq = self.dateparse_to_iso(self.end_time)

        probes = [_.strip("'") for _ in re.split(",", self.config.get(
            "query", "{0}_probe_list".format(self.report_type)))]

        if self.verbose:
            print probes

        # Elasticsearch query and aggregations
        s = Search(using=self.client, index=self.indexpattern) \
                .filter("range", EndTime={"gte": starttimeq, "lt": endtimeq}) \
                .filter("range", WallDuration={"gt": 0}) \
                .filter("terms", ProbeName=probes) \
                .filter("term", ResourceType="Payload")[0:0]
        # Size 0 to return only aggregations
        # Bucket, metric aggs
        Bucket = s.aggs.bucket("group_ProjectName", "terms", field="ProjectName",
                               size=1000000000)\
                    .bucket("group_PIName", "terms", field="PIName")\
                    .bucket("group_Organization", "terms", field="Organization")\
                    .bucket("group_FOS", "terms", field="FieldOfScience")
        Bucket.metric("CoreHours_sum", "sum", field="CoreHours")

        return s

    def runquery(self):
        """Execute the query and check the status code before returning the response"""
        try:
            response = self.query().execute()
            if not response.success():
                raise
            results = response.aggregations
            return results
        except Exception as e:
            print e, "Error accessing Elasticsearch"
            sys.exit(1)


    def run_report(self):
        """Takes data from query response and parses it to send to other functions for processing"""
        results = self.runquery()
        for pname_bucket in results.group_ProjectName.buckets:
            pname = pname_bucket.key
            # print pname
            for pi_bucket in pname_bucket.group_PIName.buckets:
                pi= pi_bucket.key
                # print pi
                for org_bucket in pi_bucket.group_Organization.buckets:
                    org = org_bucket.key
                    # print org
                    for fos_bucket in org_bucket.group_FOS.buckets:
                        fos = fos_bucket.key
                        print pname, pi, org, fos, fos_bucket.CoreHours_sum.value



            # p = self.pnc.getProject(projectname.key, self.report_type)
            # if not p:
            #     continue
            # else:
            #     p.setUsage(None, p.projecname.CoreHours_sum.value, 0)
            #         # CPUHours, WallHours, njobs
            # if self.verbose:
            #     print projectname.key, \
            #         p.getProjectName(), \
            #         p.getPI(),\
            #         p.getInstitution(),\
            #         p.getFOS(),\
            #         p.projectname.CoreHours_sum.value



    def generate_report_file(self, report):
        pass

    def format_report(self):
        report = {}


    def send_report(self, report_type="test"):
        pass


if __name__=="__main__":
    
    args = Reporter.parse_opts()

    config = Configuration.Configuration()
    config.configure(args.config)

    try:
        r = OSGReporter(config,
                        args.report_type,
                        args.limit,
                        args.start,
                        args.end,
                        True,
                        args.verbose)
        r.run_report()
        # r.sendReport("project")

    except Exception as e:
        with open(logfile, 'a') as f:
            f.write(traceback.format_exc())
        print >> sys.stderr, traceback.format_exc()
        runerror(config, e, traceback.format_exc())
        sys.exit(1)
    sys.exit(0)

