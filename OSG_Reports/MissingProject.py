#!/usr/bin/python

import os
import inspect
import re
import json
import traceback
import smtplib
import email.utils
from email.mime.text import MIMEText
import sys
import copy

from elasticsearch import Elasticsearch
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
from ProjectNameCollector import ProjectNameCollector


MAXINT = 2**31 - 1

class MissingProjectReport(Reporter):
    def __init__(self, report_type, config, start, end=None,
                 verbose=False, no_email=False, is_test=False):
        Reporter.__init__(self, report_type, config, start, end, verbose, raw=False,
                          no_email=no_email, is_test=is_test)

        # self.report_type = self.validate_report_type(report_type)
        self.header = ["Project Name", "PI", "Institution", "Field of Science"]
        # self.logfile = logfile
        # self.logger = self.__setupgenLogger("MissingProject")
        # # try:
        # #     self.client = self.establish_client()
        # # except Exception as e:
        #     self.logger.exception(e)
        self.report_type = report_type
        self.logger.info("Report Type: {0}".format(self.report_type))
        self.fname = 'OIM_Project_Name_Request_for_{0}'.format(self.report_type)

    def query(self):
        """
        Method to query Elasticsearch cluster for OSGReporter information

        :return:
        """

        # Gather parameters, format them for the query
        starttimeq = self.dateparse_to_iso(self.start_time)
        endtimeq = self.dateparse_to_iso(self.end_time)

        probes = [_.strip("'") for _ in re.split(",", self.config.get(
            "query", "{0}_probe_list".format(self.report_type)))]

        if self.verbose:
            print probes
        s = Search(using=self.client, index=self.indexpattern) \
                .filter("range", EndTime={"gte": starttimeq, "lt": endtimeq}) \
                .filter("range", WallDuration={"gt": 0}) \
                .filter("terms", ProbeName=probes) \
                .filter("term", ResourceType="Payload") \
                .filter("exists", field="RawProjectName")[0:0]

        self.unique_terms = ['OIM_PIName', 'RawProjectName', 'ProbeName',
                 'Host_description', 'VOName']

        curBucket = s.aggs.bucket("OIM_PIName", "missing", field="OIM_PIName")

        for term in self.unique_terms[1:]:
            curBucket = curBucket.bucket(term, "terms", field=term, size=MAXINT)

        return s

    def runquery(self):
        """Execute the query and check the status code before returning the response

        :return:
        """
        resultset = self.query()
        t = resultset.to_dict()
        if self.verbose:
            print json.dumps(t, sort_keys=True, indent=4)
            self.logger.debug(json.dumps(t, sort_keys=True))
        else:
            self.logger.debug(json.dumps(t, sort_keys=True))

        try:
            response = resultset.execute()
            if not response.success():
                raise
            results = response.aggregations
            self.logger.debug("Elasticsearch query executed successfully")
            # print response
            # print results
            return results
        except Exception as e:
            print e, "Error accessing Elasticsearch"
            sys.exit(1)

    def run_report(self):
        """

        :return:
        """
        results = self.runquery()
            # Some logic for if XD project, use similar logic as ProjectNameCollector does to get XD name
            # Else, in other cases, email us.

            # Also need to get more buckets (see GRACC-38 for more details)

        unique_terms = self.unique_terms

        def recurseBucket(curData, curBucket, index, data):
            """
            Recursively process the buckets down the nested aggregations

            :param curData: Current parsed data that describes curBucket and will be copied and appended to
            :param bucket curBucket: A elasticsearch bucket object
            :param int index: Index of the unique_terms that we are processing
            :param data: list of dicts that holds results of processing

            :return: None.  But this will operate on a list *data* that's passed in and modify it
            """
            curTerm = unique_terms[index]

            # Check if we are at the end of the list
            if not curBucket[curTerm]['buckets']:
                # Make a copy of the data
                nowData = copy.deepcopy(curData)
                data.append(nowData)
            else:
                # Get the current key, and add it to the data
                for bucket in curBucket[curTerm]['buckets']:
                    nowData = copy.deepcopy(curData)    # Hold a copy of curData so we can pass that in to any future recursion
                    nowData[curTerm] = bucket['key']
                    if index == (len(unique_terms) - 1):
                        # reached the end of the unique terms
                        # for metric in metrics:
                        #     nowData[metric[0]] = bucket[metric[0]].value
                            # Add the doc count
                        nowData["Count"] = bucket['doc_count']
                        data.append(nowData)
                    else:
                        recurseBucket(nowData, bucket, index + 1, data)

        # print __recurse_buckets(results.OIM_PIName, {}, 1)
        data = []
        recurseBucket({}, results.OIM_PIName, 1, data)
        print data

        for item in data:
            self.check_project(item)
            # p_object = self.check_project(item['RawProjectName'])
            # if p_object:
            #     yield p_object
            # else:       # What to do for N/A/ stuff?
            #     pass
        self.send_email_to_OSG_support()
    #
    # @staticmethod
    # def no_name(name):
    #     return name == 'N/A' or name == "UNKNOWN"

    def check_osg_or_osg_connect(self, data):
        return ((self.report_type == 'OSG-Connect')
                or (self.report_type == 'OSG' and data['VOName'].lower() in
                    ['osg', 'osg-connect'])
                )

    def check_project(self, data):
        """

        :return:
        """
        PNC = ProjectNameCollector(self.config)
        p_name = data['RawProjectName']
        if PNC.no_name(p_name):
            print "No Name!"
            PNC.create_request_to_register_oim(p_name, self.report_type)
            # self.send_email_to_OSG_support(fname)# Probably need different method to handle these.  Talk to Tanya
            # Want to email OSG Support about this record
            return None
        elif self.check_osg_or_osg_connect(data):
            # Email Tanya, Rob, myself, etc. about why this isn't registered
            pass
        else:
            # Do checks like in ProjectNameCollector
            # This was just a check - delete when committing
            # PNC2 =  ProjectNameCollector(self.config)
            # p_object = PNC2.get_project('TG-ENG150008', source=self.report_type)
            # print p_object.__class__
            # print p_object.get_project_name(),p_object.get_pi(),p_object.get_institution(),p_object.get_fos()
            return PNC.get_project(p_name, source=self.report_type)

    def send_email_to_OSG_support(self, fname):
        """

        :return:
        """
        COMMASPACE = ', '

        try:
            smtpObj = smtplib.SMTP(self.config.get('email', 'smtphost'))
        except Exception as e:
            self.logger.error(e)
            return

        with open(self.fname, 'r') as f:
            msg = MIMEText(f.read())







if __name__ == '__main__':
    args = Reporter.parse_opts()

    config = Configuration.Configuration()
    config.configure(args.config)

    # try:
    r = MissingProjectReport(args.report_type,
                             config,
                             args.start,
                             args.end,
                             verbose=args.verbose,
                             is_test=args.is_test,
                             no_email=args.no_email)
    r.run_report()
    # r.format_report()
        # r.send_report("Project")
        # r.logger.info("OSG Project Report executed successfully")
    # except Exception as e:
    #     with open(logfile, 'a') as f:
    #         f.write(traceback.format_exc())
    #     print >> sys.stderr, traceback.format_exc()
    #     runerror(config, e, traceback.format_exc())
    #     sys.exit(1)
    sys.exit(0)


