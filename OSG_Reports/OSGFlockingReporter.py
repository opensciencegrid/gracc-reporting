import os
import inspect
import smtplib
import email.utils
from email.mime.text import MIMEText
import datetime
import json
import traceback
import sys

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


class FlockingReport(Reporter):
    """Class to generate the probe report"""
    def __init__(self, configuration, start, end, template=False,
                     verbose=False, is_test=False, no_email=False):
        Reporter.__init__(self, configuration, start, end, verbose, raw=False)
        self.logfile = logfile
        self.logger = self.setupgenLogger("FlockingReport")
        try:
            self.client = self.establish_client()
        except Exception as e:
            self.logger.exception(e)
        self.no_email = no_email
        self.is_test = is_test
        self.title = "OSG Flocking Report"
        self.header = ["VOName", "SiteName", "ProbeName", "ProjectName",
                       "Wall Hours"]

    def query(self):
        """Method to query Elasticsearch cluster for EfficiencyReport
        information"""
        # Gather parameters, format them for the query
        starttimeq = self.dateparse_to_iso(self.start_time)
        endtimeq = self.dateparse_to_iso(self.end_time)

        print self.indexpattern

        # Elasticsearch query and aggregations
        s = Search(using=self.client, index=self.indexpattern) \
                .filter("range", EndTime={"gte": starttimeq, "lt": endtimeq}) \
                .filter("terms", ProbeName=["condor:amundsen.grid.uchicago.edu",
                    "condor:csiu.grid.iu.edu",
                    "condor:glide.bakerlab.org",
                    "condor:gw68.quarry.iu.teragrid.org",
                    "condor:iplant-condor-iu.tacc.utexas.edu",
                    "condor:iplant-condor.tacc.utexas.edu",
                    "condor:otsgrid.iit.edu",
                    "condor:scott.grid.uchicago.edu",
                    "condor:submit1.bioinformatics.vt.edu",
                    "condor:submit.mit.edu",
                    "condor:SUBMIT.MIT.EDU",
                    "condor:workflow.isi.edu"])\
                .filter("term", ResourceType="Payload")[0:0]
        # Size 0 to return only aggregations

        # Bucket aggs
        Bucket = s.aggs.bucket('group_Site', 'terms', field='SiteName') \
            .bucket('group_VOName', 'terms', field='ReportableVOName') \
            .bucket('group_ProbeName', 'terms', field='ProbeName') \
            .bucket('group_ProjectName', 'terms', field='ProjectName', missing='N/A')

        # Metric aggs
        Bucket.metric("CoreHours_sum", "sum", field="CoreHours")

        return s

#
# def parseCN(cn):
#     """Parse the CN to grab the email address and user"""
#     m = cilogon_match.match(cn)      # CILogon certs
#     if m:
#         pass
#     else:
#         m = non_cilogon_match.match(cn)
#     user = m.group(1)
#     return user

#
# def calc_eff(wallhours, cpusec):
#     """Calculate the efficiency given the wall hours and cputime in seconds.  Returns percentage"""
#     return round(((cpusec / 3600) / wallhours) * 100, 1)

    def run_query(self):
        """Execute the query and check the status code before returning the response"""
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
            sys.exit(1)

    def generate(self):
        """Higher-level method that calls the lower-level functions to
        generate the raw data for this report.
        """
        # pline = self.printline()
        # pline.send(None)

        results = self.run_query()

        for site in results.group_Site.buckets:
            sitekey = site.key
            for vo in site.group_VOName.buckets:
                vokey = vo.key
                for probe in vo.group_ProbeName.buckets:
                    probekey = probe.key
                    projects = (project for project in probe.group_ProjectName.buckets)
                    for project in projects:
                        # print "WHOA!"
                        yield (sitekey, vokey, probekey, project.key, project.CoreHours_sum.value)

    def generate_report_file(self, report=None):
        """Takes data from query response and parses it to send to other functions for processing
        Will handle HTML vs. csv file generation."""
        pass


    def printlines(self):
        """Coroutine to print each line to stdout"""
        print "{0}\t{1}\t{2}\t{3}\t{4}".format("VOName", "SiteName", "ProbeName", "ProjectName", "Wall Hours")
        for linetuple in self.generate():
            site, vo, probe, project, wallhours = linetuple
            print "{0}\t{1}\t{2}\t{3}\t{4}".format(vo, site, probe, project, wallhours)

    def run_report(self):
        """Higher level method to handle the process flow of the report being run"""
        return self.printlines()


    def format_report(self):
        report = {}
        for name in self.header:
            if name not in report:
                report[name] = []

        for result_tuple in self.generate():
            vo, site, probe, project, wallhours = result_tuple
            if self.verbose:
                print "{0}\t{1}\t{2}\t{3}\t{4}".format(vo, site, probe,
                                                       project, wallhours)
            report["VOName"].append(vo)
            report["SiteName"].append(site)
            report["ProbeName"].append(probe)
            report["ProjectName"].append(project)
            report["Wall Hours"].append(wallhours)

        return report










def main():
    args = Reporter.parse_opts()

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
                           no_email=args.no_email)
        # f.generate()
        f.send_report("Flocking")

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

