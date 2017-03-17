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


@Reporter.init_reporter_parser
def parse_opts(parser):
    """
    Specific argument parser for this report.  The decorator initializes the
    argparse.ArgumentParser object, calls this function on that object to
    modify it, and then returns the Namespace from that object.
    :param parser: argparse.ArgumentParser object that we intend to add to
    :return: None
    """
    # Report-specific args
    parser.add_argument("-r", "--report-type", dest="report_type",
                        help="Report type (OSG, XD. or OSG-Connect")


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
        self.fadminname = 'OIM_Admin_email_for_{0}'.format(self.report_type)
        self.fxdadminname = 'OIM_XD_Admin_email_for_{0}'.format(self.report_type)

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
        for pair in ((self.fadminname, True, False),
                     (self.fname, False, False),
                     (self.fxdadminname, False, True)):
            if os.path.exists(pair[0]):
                self.send_email(admins=pair[1], xd_admins=pair[2])
                os.unlink(pair[0])

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

        try:
            p_name = data['RawProjectName']
        except AttributeError:
            pass    # What to do if no RawProjectName?

        if PNC.no_name(p_name):
            # print "No Name!"
            self.write_noname_message(data)
            return
        elif self.check_osg_or_osg_connect(data):
            # print "OSG-Connect flag"
            PNC.create_request_to_register_oim(p_name, self.report_type, altfile=self.fadminname)
            return
        else:   # We found project info in ProjectNameCollector - XD project
            # print "something else"
            p_info = PNC.get_project(p_name, source=self.report_type)
            # PNC.create_request_to_register_oim(p_name, self.report_type)
            if not p_info:
                self.write_XD_not_in_db_message(p_name)
            return

    def write_XD_not_in_db_message(self, name):
        """

        :param name:
        :return:
        """
        msg = "The project {0} that was reported in Payload records to GRACC" \
              " is not registered in the XD database.  Please investigate and" \
              " register it if it is needed.\n".format(name)

        with open(self.fxdadminname, 'a') as f:
            f.write(msg)

        return


    def write_noname_message(self, data):
        """
        Message to be sent to GOC for records with no project name.

        :param data:
        :return:
        """
        msg = "Payload records dated between {start} and {end} with:\n" \
              "\t Host description: {hd}\n" \
              "\t VOName: {vo}\n" \
              "\t ProbeName: {probe}\n" \
              "were reported with no ProjectName to GRACC.  Please " \
              "investigate.\n\n".format(start=self.start_time,
                                        end=self.end_time,
                                        hd=data['Host_description'],
                                        vo=data['VOName'],
                                        probe=data['ProbeName'])

        with open(self.fname, 'a') as f:
            f.write(msg)

        return


    def send_email(self, admins=False, xd_admins=False):
        """
        Sets email parameters and sends email

        :return:
        """
        COMMASPACE = ', '
        # emailfrom = self.email_info["from_email"]

        if admins:
            self.email_info["to_emails"] = \
                self.config.get('email', 'osg-connect_to_emails').split(',')
            self.email_info["to_names"] = \
                self.config.get('email', 'osg-connect_to_names').split(',')
            fname = self.fadminname
        elif xd_admins:
            self.email_info["to_emails"] = \
                self.config.get('email', 'xd_admins_to_emails').split(
                    ',')
            self.email_info["to_names"] = \
                self.config.get('email', 'xd_admins_to_names').split(',')
            fname = self.fxdadminname
        else:
            fname = self.fname

        # print emailsto

        try:
            smtpObj = smtplib.SMTP(self.email_info['smtphost'])
        except Exception as e:
            self.logger.error(e)
            return

        with open(fname, 'r') as f:
            msg = MIMEText(f.read())

        to_stage = [email.utils.formataddr(pair)
                    for pair in zip(
                self.email_info['to_names'],
                self.email_info['to_emails'])]

        msg['Subject'] = "Test to OSG Support"
        msg['To'] = COMMASPACE.join(to_stage)
        msg['From'] = email.utils.formataddr((self.email_info['from_name'],
                                              self.email_info['from_email']))

        try:
            smtpObj = smtplib.SMTP(self.email_info["smtphost"])
            smtpObj.sendmail(
                self.email_info['from_email'],
                self.email_info['to_emails'],
                msg.as_string())
            smtpObj.quit()
            # self.logger.info("Sent Email for {0}".format(self.resource))
            # os.unlink(self.emailfile)
        except Exception as e:
            self.logger.exception("Error:  unable to send email.\n{0}\n".format(e))
            raise

        return

if __name__ == '__main__':
    args = parse_opts()

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


