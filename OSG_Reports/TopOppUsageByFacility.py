#!/usr/bin/python

import sys
import os
import inspect
import traceback
import re
import json
import datetime
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

import TextUtils
import TimeUtils
import Configuration
import NiceNum
from Reporter import Reporter, runerror

logfile = 'topoppusage.log'


# Helper functions
def coroutine(func):
    """Decorator to prime coroutines by advancing them to their first yield
    point

    :param function func: Coroutine function to prime
    :return function: Coroutine that's been primed
    """
    def wrapper(*args, **kwargs):
        cr = func(*args, **kwargs)
        cr.next()
        return cr
    return wrapper


def get_time_range(start=None, end=None, months=None):
    pass

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
    pass


class TopOppUsageByFacility(Reporter):
    """
    """
    def __init__(self, config, start=None, end=None, template=None,
                 is_test=False, no_email=False,
                 verbose=False, rank=10, months=None):
        report = 'news'
        self.vo = vo
        Reporter.__init__(self, report, config, start, end, verbose=verbose,
                          logfile=logfile, no_email=no_email, is_test=is_test)
        self.rank = rank
        self.template = template
        self.text = ''
        self.table = ''
        self.title = "Opportunistic Resources provided by the top {0} OSG " \
                     "Sites for the OSG Open Facility ({1} - {2})".format(
            self.rank, self.start_time, self.end_time
        )

    def run_report(self):
        """Handles the data flow throughout the report generation.  Generates
        the raw data, the HTML report, and sends the email.

        :return None
        """
        self.generate()
        self.generate_report_file()
        self.send_report()
        return

    def query(self):
        """
        Method to query Elasticsearch cluster for EfficiencyReport information

        :return elasticsearch_dsl.Search: Search object containing ES query
        """
        # Gather parameters, format them for the query
        starttimeq = self.dateparse_to_iso(self.start_time)
        endtimeq = self.dateparse_to_iso(self.end_time)
        wildcardVOq = '*' + self.vo.lower() + '*'
        wildcardProbeNameq = 'condor:fifebatch?.fnal.gov'

        if self.verbose:
            self.logger.info(self.indexpattern)

        # Elasticsearch query and aggregations
        s = Search(using=self.client, index=self.indexpattern) \
            .filter("wildcard", VOName=wildcardVOq) \
            .filter("wildcard", ProbeName=wildcardProbeNameq) \
            .filter("range", EndTime={"gte": starttimeq, "lt": endtimeq}) \
            .filter("range", WallDuration={"gt": 0}) \
            .filter("term", Host_description="GPGrid") \
            .filter("term", ResourceType="Payload")[0:0]
        # Size 0 to return only aggregations

        # Bucket aggs
        Bucket = s.aggs.bucket('group_VOName', 'terms', field='ReportableVOName') \
            .bucket('group_HostDescription', 'terms', field='Host_description') \
            .bucket('group_CommonName', 'terms', field='CommonName')

        # Metric aggs
        Bucket.metric('WallHours', 'sum', field='CoreHours') \
            .metric('CPUDuration_sec', 'sum', field='CpuDuration')

        return s

    def run_query(self):
        """Execute the query and check the status code before returning the
        response

        :return Response.aggregations: Returns aggregations property of
        elasticsearch response
        """
        s = self.query()
        t = s.to_dict()
        if self.verbose:
            print json.dumps(t, sort_keys=True, indent=4)
            self.logger.debug(json.dumps(t, sort_keys=True))
        else:
            self.logger.debug(json.dumps(t, sort_keys=True))

        try:
            response = s.execute()
            if not response.success():
                raise Exception("Error accessing Elasticsearch")

            if self.verbose:
                print json.dumps(response.to_dict(), sort_keys=True, indent=4)

            results = response.aggregations
            self.logger.info('Ran elasticsearch query successfully')
            return results
        except Exception as e:
            self.logger.exception(e)
            raise

    def generate(self):
        """
        Runs the ES query, checks for success, and then
        sends the raw data to parser for processing.

        :return: None
        """
        results = self.run_query()
        return

    def generate_report_file(self):
        """
        Takes the HTML template and inserts the appropriate information to
        generate the final report file

        :return: None
        """
        # header = ['Experiment', 'Facility', 'User', 'Used Wall Hours',
        #           'Efficiency']
        # htmlheader = '<th>' + '</th><th>'.join(header) + '</th>'
        # htmldict = dict(title=self.title, header=htmlheader, table=self.table)
        # self.text = "".join(open(self.template).readlines())
        # self.text = self.text.format(**htmldict)
        return

    def send_report(self):
        """
        Sends the HTML report file in an email (or doesn't if self.no_email
        is set to True)

        :return: None
        """
        if self.test_no_email(self.email_info["to_emails"]):
            return

        TextUtils.sendEmail(
                            (self.email_info["to_names"],
                             self.email_info["to_emails"]),
                            self.title,
                            {"html": self.text},
                            (self.email_info["from_name"],
                             self.email_info["from_email"]),
                            self.email_info["smtphost"])

        self.logger.info("Report sent for {0}".format(self.vo))

        return


if __name__ == "__main__":
    args = parse_opts()

    # Set up the configuration
    config = Configuration.Configuration()
    config.configure(args.config)

    try:

        # Create a report object, create a report for the VO, and send it
        r = TopOppUsageByFacility(config,
                                  start=args.start,
                                  end=args.end,
                                  template=args.template,
                                  months=args.months,
                                  is_test=args.is_test,
                                  no_email=args.no_email,
                                  verbose=args.verbose,
                                  rank=rank)
        r.run_report()
        print "Efficiency Report execution successful"

    except Exception as e:
        errstring = '{0}: Error running Efficiency Report for {1}. ' \
                    '{2}'.format(datetime.datetime.now(), args.vo, traceback.format_exc())
        with open(logfile, 'a') as f:
            f.write(errstring)
        print >> sys.stderr, errstring
        runerror(config, e, errstring)
        sys.exit(1)
    sys.exit(0)
