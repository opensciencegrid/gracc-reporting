#!/usr/bin/python

import sys
import os
import inspect
import traceback
import json
from elasticsearch_dsl import A, Search

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

import NiceNum
import Configuration
from Reporter import Reporter, runerror
import TextUtils

logfile = 'wastedhours.log'


@Reporter.init_reporter_parser
def parse_opts(parser):
    """
    Don't need to add any options to Reporter.parse_opts
    """
    pass


class User:
    """
    Holds all user-specific information for this report

    :param str user_name: username of user
    """

    def __init__(self, user_name):
        self.user = user_name
        self.success = [0, 0]
        self.failure = [0, 0]

    def add_failure(self, njobs, wall_duration):
        """
        Adds to the Failure list for user

        :param njobs: Number of jobs in summary record
        :param wall_duration: Wall duration in summary record
        :return:
        """
        self.failure = [ex + new for ex, new in
                        zip(self.failure, [njobs, wall_duration])]

    def add_success(self, njobs, wall_duration):
        """
        Adds to the Success list for user

        :param njobs: Number of jobs in summary record
        :param wall_duration: Wall duration in summary record
        :return:
        """
        self.success = [ex + new for ex, new in
                        zip(self.success, [njobs, wall_duration])]

    def get_failure_rate(self):
        """
        Calculates the failure rate of a user's jobs
        """
        failure_rate = 0
        if self.success[0] + self.failure[0] > 0:
            failure_rate = self.failure[0] * 100. /\
                           (self.success[0] + self.failure[0])
        return failure_rate

    def get_waste_per(self):
        """
        Gets a user's wasted hours
        """
        waste_per = 0
        if self.success[1] + self.failure[1] > 0:
            waste_per = self.failure[1] * 100. /\
                        (self.success[1] + self.failure[1])
        return waste_per


class Experiment:
    """
    Hold all experiment-specific information for this report

    :param exp_name: Experiment name
    """
    def __init__(self, exp_name):
        self.experiment = exp_name
        self.success = [0, 0]
        self.failure = [0, 0]
        self.users = {}

    def add_user(self, user_name, user):
        """
        Adds user to an experiment

        :param user_name: username of user
        :param User user: User object holding user's info
        :return: None
        """
        self.users[user_name] = user


class WastedHoursReport(Reporter):
    """
    Class to hold information about and run Wasted Hours report.
    :param Configuration.Configuration config: Report Configuration object
    :param str start: Start time of report range
    :param str end: End time of report range
    :param str template: Filename of HTML template to generate report
    :param bool is_test: Whether or not this is a test run.
    :param bool verbose: Verbose flag
    :param bool no_email: If true, don't actually send the email
    """
    def __init__(self, config, start, end, template, is_test=True,
                 verbose=False, no_email=False):
        report = 'WastedHours'
        Reporter.__init__(self, report, config, start, end=end,
                          verbose=verbose, raw=False, is_test=is_test,
                          no_email=no_email, logfile=logfile)
        self.template = template
        self.experiments = {}
        self.connect_str = None
        self.text = ''
        self.fn = "user_wasted_hours_report.{0}".format(
            self.end_time.split(" ")[0].replace("/", "-"))
        self.title = "{0:s} Wasted Hours on GPGrid ({1:s} - {2:s})"\
                            .format("FIFE", self.start_time, self.end_time)

    def run_report(self):
        """Higher-level method to run all the other methods in report
        generation"""
        self.generate()
        self.generate_report_file()
        self.send_report()
        if os.path.exists(self.fn):
            os.unlink(self.fn)
        return

    def query(self):
        """
        Method to query Elasticsearch cluster for EfficiencyReport information

        :return elasticsearch_dsl.Search: Search object containing ES query
        """
        wildcardProbeNameq = 'condor:fifebatch?.fnal.gov'

        starttimeq = self.dateparse_to_iso(self.start_time)
        endtimeq = self.dateparse_to_iso(self.end_time)

        s = Search(using=self.client, index=self.indexpattern) \
            .filter("wildcard", ProbeName=wildcardProbeNameq) \
            .filter("range", EndTime={"gte": starttimeq, "lt": endtimeq})

        # Aggregations

        Buckets = s.aggs.bucket('group_status', 'filters', filters={
            'Success': {'bool': {'must': {'term': {'Resource_ExitCode': 0}}}},
            'Failure': {
                'bool': {'must_not': {'term': {'Resource_ExitCode': 0}}}}})\
            .bucket('group_VO', 'terms', field='VOName', size=2**31-1) \
            .bucket('group_CommonName','terms', field='CommonName',
                    size=2**31-1)

        # Metrics
        Buckets.metric('numJobs', 'sum', field='Count')\
            .metric('WallHours', 'sum', field='CoreHours')

        if self.verbose:
            print s.to_dict()

        return s

    def generate(self):
        """
        Generates the raw data for the report, stores in Experiment, User
        objects

        :return: None
        """
        resultquery = self.query()

        response = resultquery.execute()
        return_code_success = response.success()
        if not return_code_success:
            raise Exception('Error querying elasticsearch')

        results = response.aggregations

        if self.verbose:
            print json.dumps(response.to_dict(), sort_keys=True, indent=4)

        table_results = []
        for status in results.group_status.buckets:
            for VO in results.group_status.buckets[status].group_VO.buckets:
                for CommonName in VO['group_CommonName'].buckets:
                    table_results.append([CommonName.key, VO.key, status,
                                          CommonName['numJobs'].value,
                                          CommonName['WallHours'].value])

        if len(table_results) == 1 and len(table_results[0].strip()) == 0:
            print >> sys.stdout, "Nothing to report"
            return

        for line in table_results:
            if self.verbose:
                print line
            name = line[0]
            expname = line[1]
            status = line[2]
            count = int(line[3])
            hours = float(line[4])
            if expname not in self.experiments:
                exp = Experiment(expname)
                self.experiments[expname] = exp
            else:
                exp = self.experiments[expname]
            if name not in exp.users:
                user = User(name)
                exp.add_user(name, user)
            else:
                user = exp.users[name]
            if status == 'Success':
                user.add_success(count, hours)
            else:
                user.add_failure(count, hours)

        return

    def generate_report_file(self):
        """Reads the Experiment and User objects and generates the report
        HTML file

        :return: None
        """
        if len(self.experiments) == 0:
            print "No experiments"
            return
        total_hrs = 0
        total_jobs = 0
        table = ""
        for key, exp in self.experiments.items():
            for uname, user in exp.users.items():
                failure_rate = round(user.get_failure_rate(), 1)
                waste_per = round(user.get_waste_per(), 2)
                table += '\n<tr><td align="left">{0:s}</td>' \
                         '<td align="left">{1:s}</td>' \
                         '<td align="right">{2:s}</td>' \
                         '<td align="right">{3:s}</td>' \
                         '<td align="right">{4:.1f}</td>' \
                         '<td align="right">{5:s}</td>' \
                         '<td align="right">{6:s}</td>' \
                         '<td align="right">{7:.1f}</td></tr>'.format(
                    key,
                    uname,
                    NiceNum.niceNum(user.success[0] + user.failure[0]),
                    NiceNum.niceNum(user.failure[0]),
                    failure_rate,
                    NiceNum.niceNum(user.success[1] + user.failure[1],1),
                    NiceNum.niceNum(user.failure[1], 1),
                    waste_per
                )
                if self.verbose:
                    total_hrs += (user.success[1] + user.failure[1])
                    total_jobs += (user.success[0] + user.failure[0])

        headerlist = ['Experiment', 'User', 'Total #Jobs', '# Failures',
                      'Failure Rate (%)', 'Wall Duration (Hours)',
                      'Time Wasted (Hours)', '% Hours Wasted']

        header = ''.join(('<th>{0}</th>'.format(elt) for elt in headerlist))

        # Yes, the header and footer are the same on purpose
        htmldict = dict(title=self.title, table=table,
                        header=header, footer=header)
        self.text = "".join(open(self.template).readlines())
        self.text = self.text.format(**htmldict)

        if self.verbose:
            print total_jobs, total_hrs
        return

    def send_report(self):
        """
        Sends the HTML report file in an email (or doesn't if self.no_email
        is set to True)

        :return: None
        """
        if self.test_no_email(self.email_info["to_emails"]):
            return

        TextUtils.sendEmail((self.email_info["to_names"],
                             self.email_info["to_emails"]),
                            self.title,
                            {"html": self.text},
                            (self.email_info["from_name"],
                             self.email_info["from_email"]),
                            self.email_info["smtphost"])
        return



if __name__ == "__main__":
    args = parse_opts()

    config = Configuration.Configuration()
    config.configure(args.config)

    try:
        report = WastedHoursReport(config,
                                   args.start,
                                   args.end,
                                   args.template,
                                   is_test=args.is_test,
                                   verbose=args.verbose,
                                   no_email=args.no_email)
        try:
            report.run_report()
        except (Exception, KeyboardInterrupt) as e:
            # Make sure to clean up files if there's an error or
            # keyboard interrupt
            if os.path.exists(report.fn):
                os.unlink(report.fn)
            raise
    except Exception as e:
        print >> sys.stderr, traceback.format_exc()
        runerror(config, e, traceback.format_exc())
        sys.exit(1)

    sys.exit(0)
