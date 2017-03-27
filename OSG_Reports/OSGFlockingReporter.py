import os
import inspect
import datetime
import json
import traceback
import sys
from re import split

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

logfile = 'osgflockingreport.log'
MAXINT = 2**31 - 1

# Helper functions
def running_total():
    """Calculates the running total of numbers that are fed in.
    Yields the running total so far
    """
    total = 0
    while True:
        number = yield total
        total += number

@Reporter.init_reporter_parser
def parse_opts(parser):
    """
    Don't need to add any options to Reporter.parse_opts
    """
    pass


class FlockingReport(Reporter):
    """Class to hold information for and to run OSG Flocking report

    :param Configuration.Configuration config: Report Configuration object
    :param str start: Start time of report range
    :param str end: End time of report range
    :param str template: Filename of HTML template to generate report
    :param bool verbose: Verbose flag
    :param bool is_test: Whether or not this is a test run.
    :param bool no_email: If true, don't actually send the email
    """
    def __init__(self, config, start, end, template=False,
                 verbose=False, is_test=False, no_email=False):
        report = 'Flocking'
        Reporter.__init__(self, report, config, start, end=end,
                          template=template, verbose=verbose,
                          no_email=no_email, is_test=is_test,
                          raw=False, logfile=logfile)
        self.verbose = verbose
        self.no_email = no_email
        self.is_test = is_test
        self.title = "OSG Flocking: Usage of OSG Sites for {0} - {1}".format(self.start_time, self.end_time)
        self.header = ["VOName", "SiteName", "ProbeName", "ProjectName",
                       "Wall Hours"]

    def query(self):
        """Method to query Elasticsearch cluster for Flocking Report
        information

        :return elasticsearch_dsl.Search: Search object containing ES query
        """
        # Gather parameters, format them for the query
        starttimeq = self.dateparse_to_iso(self.start_time)
        endtimeq = self.dateparse_to_iso(self.end_time)

        if self.verbose:
            self.logger.info(self.indexpattern)

        probes = self.config.get('{0}_report'.format(self.report_type.lower()),
                                 'flocking_probe_list')
        probeslist = split(',', probes)

        # Elasticsearch query and aggregations
        s = Search(using=self.client, index=self.indexpattern) \
                .filter("range", EndTime={"gte": starttimeq, "lt": endtimeq}) \
                .filter("terms", ProbeName=probeslist)\
                .filter("term", ResourceType="Payload")[0:0]
        # Size 0 to return only aggregations

        # Bucket aggs
        Bucket = s.aggs.bucket('group_Site', 'terms', field='SiteName', size=MAXINT) \
            .bucket('group_VOName', 'terms', field='ReportableVOName', size=MAXINT) \
            .bucket('group_ProbeName', 'terms', field='ProbeName', size=MAXINT) \
            .bucket('group_ProjectName', 'terms', field='ProjectName', missing='N/A', size=MAXINT)

        # Metric aggs
        Bucket.metric("CoreHours_sum", "sum", field="CoreHours")

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
                raise
            results = response.aggregations
            return results
        except Exception as e:
            print e, "Error accessing Elasticsearch"
            raise

    def generate(self):
        """Higher-level generator method that calls the lower-level functions
        to generate the raw data for this report.

        Yields rows of raw data
        """
        results = self.run_query()

        # Iterate through the buckets to get our data, yield it
        for site in results.group_Site.buckets:
            sitekey = site.key
            for vo in site.group_VOName.buckets:
                vokey = vo.key
                for probe in vo.group_ProbeName.buckets:
                    probekey = probe.key
                    projects = (project for project in probe.group_ProjectName.buckets)
                    for project in projects:
                        yield (sitekey, vokey, probekey, project.key, project.CoreHours_sum.value)



    def format_report(self):
        """Report formatter.  Returns a dictionary called report containing the
        columns of the report.

        :return dict: Constructed dict of report information for
        Reporter.send_report to send report from"""
        report = {}
        tot = running_total()
        tot.send(None)

        for name in self.header:
            if name not in report:
                report[name] = []

        for result_tuple in self.generate():
            vo, site, probe, project, wallhours = result_tuple
            if self.verbose:
                print "{0}\t{1}\t{2}\t{3}\t{4}".format(*result_tuple)
            report["VOName"].append(vo)
            report["SiteName"].append(site)
            report["ProbeName"].append(probe)
            report["ProjectName"].append(project)
            report["Wall Hours"].append(wallhours)
            runtot = tot.send(wallhours)

        for col in self.header:
            if col == 'VOName':
                report[col].append('Total')
            elif col == 'Wall Hours':
                report[col].append(runtot)
            else:
                report[col].append('')

        if self.verbose:
            print "The total Wall hours in this report are {0}".format(runtot)

        return report

    def run_report(self):
        """Higher level method to handle the process flow of the report
        being run"""
        self.send_report(title=self.title)


def main():
    args = parse_opts()

    # Set up the configuration
    config = Configuration.Configuration()
    config.configure(args.config)
    try:

        # Create an FlockingReport object, and run the report
        f = FlockingReport(config,
                           args.start,
                           args.end,
                           verbose=args.verbose,
                           is_test=args.is_test,
                           no_email=args.no_email,
                           template=args.template)
        f.run_report()
        print "OSG Flocking Report execution successful"
    except Exception as e:
        errstring = '{0}: Error running OSG Flocking Report. ' \
                    '{1}'.format(datetime.datetime.now(), traceback.format_exc())
        with open(logfile, 'a') as f:
            f.write(errstring)
        print >> sys.stderr, errstring
        runerror(config, e, errstring)
        sys.exit(1)
    sys.exit(0)

if __name__ == "__main__":
    main()

