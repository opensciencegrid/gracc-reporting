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

# from elasticsearch import Elasticsearch
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
logfile = 'missingproject.log'

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
    logfile = 'missingproject.log'

    def __init__(self, report_type, config, start, end=None,
                 verbose=False, no_email=False, is_test=False):
        Reporter.__init__(self, report_type, config, start, end, verbose,
                          raw=False, no_email=no_email, is_test=is_test,
                          logfile=self.logfile)
        self.report_type = self.validate_report_type(report_type)
        self.logger.info("Report Type: {0}".format(self.report_type))
        
        # Temp files
        self.fname = 'OIM_Project_Name_Request_for_{0}'.format(self.report_type)
        self.fxdadminname = 'OIM_XD_Admin_email_for_{0}'.format(self.report_type)
        for f in (self.fname, self.fxdadminname):  # Cleanup
            if os.path.exists(f):
                os.unlink(f)

    @staticmethod
    def validate_report_type(report_type):
        """
        Validates that the report being run is one of three types.

        :param str report_type: One of OSG, XD, or OSG-Connect
        :return report_type: report type
        """
        validtypes = {"OSG": "OSG-Direct", "XD": "OSG-XD",
                      "OSG-Connect": "OSG-Connect"}
        if report_type in validtypes:
            return report_type
        else:
            raise Exception("Must use report type {0}".format(
                ', '.join((name for name in validtypes)))
            )

    def query(self):
        """
        Method to query Elasticsearch cluster for OSGReporter information

        :return elasticsearch_dsl.Search: Search object containing ES query
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
                 'CommonName', 'VOName']
        self.metrics = ['CoreHours']

        curBucket = s.aggs.bucket("OIM_PIName", "missing", field="OIM_PIName")

        for term in self.unique_terms[1:]:
            curBucket = curBucket.bucket(term, "terms", field=term, size=MAXINT)

        curBucket.metric(self.metrics[0], 'sum', field=self.metrics[0])

        return s

    def runquery(self):
        """Execute the query and check the status code before returning the response

        :return Response.aggregations: Returns aggregations property of
        elasticsearch response
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
            return results
        except Exception as e:
            print e, "Error accessing Elasticsearch"
            sys.exit(1)

    def run_report(self):
        """Higher level method to handle the process flow of the report
        being run"""
        results = self.runquery()
        unique_terms = self.unique_terms
        metrics = self.metrics

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
                        for metric in metrics:
                            nowData[metric] = bucket[metric].value
                            # Add the doc count
                        nowData["Count"] = bucket['doc_count']
                        data.append(nowData)
                    else:
                        recurseBucket(nowData, bucket, index + 1, data)

        data = []
        recurseBucket({}, results.OIM_PIName, 1, data)
        if self.verbose:
            self.logger.info(data)

        if len(data) == 1 and not data[0]:  # No data.
            return

        # Check the missing projects
        for item in data:
            self.check_project(item)

        # Send the emails, delete temp files
        for group in ((self.fname, False),
                     (self.fxdadminname, True)):
            if os.path.exists(group[0]):
                self.send_email(xd_admins=group[1])
                self.logger.info("Sent email from file {0}".format(group[0]))
                os.unlink(group[0])

    def check_osg_or_osg_connect(self, data):
        """
        Checks to see if data describing project is OSG's responsibility to
        maintain

        :param dict data: Aggregated data about a missing project from ES query
        :return bool:
        """
        return ((self.report_type == 'OSG-Connect')
                or (self.report_type == 'OSG' and data['VOName'].lower() in
                    ['osg', 'osg-connect'])
                )

    def check_project(self, data):
        """
        Handles the logic for what to do with records that don't have OIM info

        :param dict data: Aggregated data about a missing project from ES query
        :return:
        """
        PNC = ProjectNameCollector(self.config)

        p_name = data.get('RawProjectName')

        if not p_name or PNC.no_name(p_name):
            # No real Project Name in records
            self.write_noname_message(data)
            return
        elif self.check_osg_or_osg_connect(data):
            # OSG should have kept this up to date
            PNC.create_request_to_register_oim(p_name, self.report_type)
            return
        else:
            # XD project, most likely
            p_info = PNC.get_project(p_name, source=self.report_type)
            if not p_info and self.report_type == 'XD':
                # Project not in XD database
                self.write_XD_not_in_db_message(p_name)
            return

    def write_XD_not_in_db_message(self, name):
        """
        Appends message to a temp file that indicates that an XD project is not
        registered in the XD database.

        :param str name: name of XD project
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

        :param dict data: Aggregated data about a missing project from ES query
        :return:
        """

        for field in ('CommonName', 'VOName', 'ProbeName', 'CoreHours',
                      'RawProjectName', 'Count'):
            if not data.get(field):
                data[field] = "{0} not reported".format(field)


        msg = "{count} Payload records dated between {start} and {end} with:\n" \
              "\t CommonName: {cn}\n" \
              "\t VOName: {vo}\n" \
              "\t ProbeName: {probe}\n" \
              "\t Wall Hours: {ch}\n " \
              "were reported with no ProjectName (\"{pn}\") to GRACC.  Please " \
              "investigate.\n\n".format(count=data['Count'],
                                        start=self.start_time,
                                        end=self.end_time,
                                        cn=data['CommonName'],
                                        vo=data['VOName'],
                                        probe=data['ProbeName'],
                                        ch=data['CoreHours'],
                                        pn=data['RawProjectName'])

        with open(self.fname, 'a') as f:
            f.write(msg)

        return

    def send_email(self, xd_admins=False):
        """
        Sets email parameters and sends email.

        :param bool xd_admins: Flag to override self.email_info dict to send
        a notification email to the xd_admins as listed in the config file.
        :return:
        """
        COMMASPACE = ', '

        if xd_admins:
            self.email_info["to_emails"] = \
                self.config.get('email', 'xd_admins_to_emails').split(
                    ',')
            self.email_info["to_names"] = \
                self.config.get('email', 'xd_admins_to_names').split(',')
            fname = self.fxdadminname
            self.logger.info("xd_admins flag is True.  Sending email to "
                             "xd_admins")
        else:
            fname = self.fname

        if self.test_no_email(self.email_info["to_emails"]):
            return

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

        if xd_admins:
            msg['Subject'] = 'XD Projects not found in XD database'
        else:
            msg['Subject'] = 'Records with no Project or Projects not ' \
                             'registered in OIM'
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
        except Exception as e:
            self.logger.exception("Error:  unable to send email.\n{0}\n".format(e))
            raise

        return

if __name__ == '__main__':
    args = parse_opts()

    config = Configuration.Configuration()
    config.configure(args.config)

    try:
        r = MissingProjectReport(args.report_type,
                                 config,
                                 args.start,
                                 args.end,
                                 verbose=args.verbose,
                                 is_test=args.is_test,
                                 no_email=args.no_email)
        r.run_report()
    except Exception as e:
        with open(logfile, 'a') as f:
            f.write(traceback.format_exc())
        print >> sys.stderr, traceback.format_exc()
        runerror(config, e, traceback.format_exc())
        sys.exit(1)
    sys.exit(0)

