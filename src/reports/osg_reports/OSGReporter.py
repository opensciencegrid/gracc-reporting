import re
import traceback
import sys
import copy

from elasticsearch_dsl import Search

from . import Reporter, runerror, get_configfile, get_template, Configuration
from MissingProject import MissingProjectReport

logfile = 'osgreporter.log'
default_templatefile = 'template_project.html'
MAXINT = 2**31 - 1


# Helper Functions
def key_to_lower(bucket):
    return bucket.key.lower()


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
    parser.add_argument('--nosum', dest="isSum", action='store_false',
                        help="Do not show a total line")


class OSGReporter(Reporter):
    def __init__(self, report_type, config, start, end=None, isSum=True,
                 verbose=False, no_email=False, is_test=False, template=None,
                 ov_logfile=None):

        if ov_logfile:
            rlogfile = ov_logfile
            logfile_override = True
        else:
            rlogfile = logfile
            logfile_override = False

        Reporter.__init__(self, report_type, config, start, end, verbose,
                          raw=False, no_email=no_email, is_test=is_test,
                          template=template, logfile=rlogfile,
                          logfile_override=logfile_override)
        self.isSum = isSum
        self.report_type = self._validate_report_type(report_type)
        self.header = ["Project Name", "PI", "Institution", "Field of Science",
                     "Wall Hours"]
        self.logger.info("Report Type: {0}".format(self.report_type))
        self.isSum = isSum

    def run_report(self):
        """Higher level method to handle the process flow of the report
        being run"""
        self.send_report(title=self.title)

    def query(self):
        """Method to query Elasticsearch cluster for OSGReporter information

        :return elasticsearch_dsl.Search: Search object containing ES query
        """
        # Gather parameters, format them for the query
        starttimeq = self.dateparse_to_iso(self.start_time)
        endtimeq = self.dateparse_to_iso(self.end_time)

        probes = [rawprobe.strip("'") for rawprobe in
                  re.split(",", self.config.get(
                               "project",
                               "{0}_probe_list".format(self.report_type)))]

        if self.verbose:
            self.logger.debug(probes)
            self.logger.debug(self.indexpattern)

        # Elasticsearch query and aggregations
        s = Search(using=self.client, index=self.indexpattern) \
                .filter("range", EndTime={"gte": starttimeq, "lt": endtimeq}) \
                .filter("range", WallDuration={"gt": 0}) \
                .filter("terms", ProbeName=probes) \
                .filter("term", ResourceType="Payload")[0:0]
        # Size 0 to return only aggregations
        # Bucket, metric aggs
        Bucket = s.aggs.bucket("ProjectName", "terms", field="ProjectName",
                               size=MAXINT, order={"_term":"asc"},
                               missing="UNKNOWN")\
                    .bucket("OIM_PIName", "terms", field="OIM_PIName", missing="UNKNOWN", size=MAXINT)\
                    .bucket("OIM_Organization", "terms", field="OIM_Organization", missing="UNKNOWN", size=MAXINT)\
                    .bucket("OIM_FieldOfScience", "terms", field="OIM_FieldOfScience", missing="UNKNOWN", size=MAXINT)

        Bucket.metric("CoreHours", "sum", field="CoreHours")

        return s

    def generate_report_file(self):
        """Takes data from query response and parses it to send to other
        functions for processing"""
        results = self.run_query()

        unique_terms = ['ProjectName', 'OIM_PIName', 'OIM_Organization',
                        'OIM_FieldOfScience']
        metrics = ['CoreHours']

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
                for bucket in self.sorted_buckets(curBucket[curTerm], key=key_to_lower):
                    nowData = copy.deepcopy(
                        curData)  # Hold a copy of curData so we can pass that in to any future recursion
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
        recurseBucket({}, results, 0, data)
        allterms = copy.copy(unique_terms)
        allterms.extend(metrics)

        print data
        for entry in data:
            yield [entry[field].encode('ascii', 'replace') if isinstance(entry[field], unicode) else entry[field] for field in allterms]

    def format_report(self):
        """Report formatter.  Returns a dictionary called report containing the
        columns of the report.

        :return dict: Constructed dict of report information for
        Reporter.send_report to send report from"""
        report = {}
        for name in self.header:
            if name not in report:
                report[name] = []

        for result_list in self.generate_report_file():
            if self.verbose:
                print "{0}\t{1}\t{2}\t{3}\t{4}".format(*result_list)
            mapdict = dict(zip(self.header, result_list))
            for key, item in mapdict.iteritems():
                report[key].append(item)

        if self.isSum:
            tot = sum(report['Wall Hours'])
            for field in self.header:
                if field == 'Project Name':
                    report[field].append('Total')
                elif field == 'Wall Hours':
                    report[field].append(tot)
                else:
                    report[field].append('')

            if self.verbose:
                self.logger.info("The total wall hours in this report are "
                                 "{0}".format(tot))

        return report

    def _validate_report_type(self, report_type):
        """
        Validates that the report being run is one of three types.  Sets
        title of report if it's given a valid report type

        :param str report_type: One of OSG, XD, or OSG-Connect
        :return report_type: report type
        """
        validtypes = {"OSG": "OSG-Direct", "XD": "OSG-XD",
                      "OSG-Connect": "OSG-Connect"}
        if report_type in validtypes:
            self.title = "{0} Project Report for {1} - {2}".format(
                validtypes[report_type], self.start_time, self.end_time)
            return report_type
        else:
            raise Exception("Must use report type {0}".format(
                ', '.join((name for name in validtypes)))
            )


def main():
    args = parse_opts()

    # Set up the configuration
    config = Configuration.Configuration()
    config.configure(get_configfile(override=args.config))

    templatefile = get_template(override=args.template, deffile=default_templatefile)

    try:
        r = OSGReporter(args.report_type,
                        config,
                        args.start,
                        args.end,
                        template=templatefile,
                        isSum=args.isSum,
                        verbose=args.verbose,
                        is_test=args.is_test,
                        no_email=args.no_email,
                        ov_logfile=args.logfile)
        r.run_report()
        r.logger.info("OSG Project Report executed successfully")

        m = MissingProjectReport(args.report_type,
                                 config,
                                 args.start,
                                 args.end,
                                 verbose=args.verbose,
                                 is_test=args.is_test,
                                 no_email=args.no_email)
        m.run_report()
    except Exception as e:
        with open(logfile, 'a') as f:
            f.write(traceback.format_exc())
        print >> sys.stderr, traceback.format_exc()
        runerror(config, e, traceback.format_exc())
        sys.exit(1)
    sys.exit(0)


if __name__=="__main__":
    main()