import os
import inspect
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
        report = 'Flocking'
        Reporter.__init__(self, report, configuration, start, end=end,
                          template=template, verbose=verbose,
                          no_email=no_email, raw=False, logfile=logfile)
        self.verbose = verbose
        self.no_email = no_email
        self.is_test = is_test
        self.title = "OSG Flocking: Usage of OSG Sites for {0} - {1}".format(self.start_time, self.end_time)
        self.header = ["VOName", "SiteName", "ProbeName", "ProjectName",
                       "Wall Hours"]

    def query(self):
        """Method to query Elasticsearch cluster for Flocking Report
        information"""
        # Gather parameters, format them for the query
        starttimeq = self.dateparse_to_iso(self.start_time)
        endtimeq = self.dateparse_to_iso(self.end_time)

        if self.verbose:
            self.logger.info(self.indexpattern)

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
            raise

    def generate(self):
        """Higher-level generator method that calls the lower-level functions
        to generate the raw data for this report.
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
        """Takes the results from the elasticsearch query and returns a dict
        that can be used by the Reporter.send_report method to generate HTML,
        CSV, and plain text output"""
        report = {}
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
        return report

    def run_report(self):
        """Higher level method to handle the process flow of the report
        being run"""
        self.send_report(title=self.title)


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

