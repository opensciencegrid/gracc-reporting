import sys
import traceback

from elasticsearch_dsl import Search

from . import Reporter, runerror, get_configfile, get_template
from . import TextUtils, NiceNum

default_templatefile = 'template_wasted_hours.html'
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
    :param str config: Report Configuration file
    :param str start: Start time of report range
    :param str end: End time of report range
    :param str template: Filename of HTML template to generate report
    :param bool is_test: Whether or not this is a test run.
    :param bool verbose: Verbose flag
    :param bool no_email: If true, don't actually send the email
    """
    def __init__(self, config, start, end, template, is_test=True,
                 verbose=False, no_email=False, ov_logfile=None):
        report = 'WastedHours'

        if ov_logfile:
            rlogfile = ov_logfile
            logfile_override = True
        else:
            rlogfile = logfile
            logfile_override = False

        self.title = "{0:s} Wasted Hours on GPGrid ({1:s} - {2:s})"\
                            .format("FIFE", start, end)

        Reporter.__init__(self, report, config, start, end=end,
                          verbose=verbose, is_test=is_test,
                          no_email=no_email, logfile=rlogfile,
                          logfile_override=logfile_override)
        self.template = template
        self.experiments = {}
        self.connect_str = None
        self.text = ''

    def run_report(self):
        """Higher-level method to run all the other methods in report
        generation"""
        self.generate()
        self.generate_report_file()
        self.send_report()
        return

    def query(self):
        """
        Method to query Elasticsearch cluster for EfficiencyReport information

        :return elasticsearch_dsl.Search: Search object containing ES query
        """
        wildcardProbeNameq = 'condor:fifebatch?.fnal.gov'

        starttimeq = self.start_time.isoformat()
        endtimeq = self.end_time.isoformat()

        s = Search(using=self.client, index=self.indexpattern) \
            .filter("wildcard", ProbeName=wildcardProbeNameq) \
            .filter("range", EndTime={"gte": starttimeq, "lt": endtimeq})[0:0]

        # Aggregations

        Buckets = s.aggs.bucket('group_status', 'filters', filters={
            'Success': {'bool': {'must': {'term': {'Resource_ExitCode': 0}}}},
            'Failure': {
                'bool': {'must_not': {'term': {'Resource_ExitCode': 0}}}}}) \
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
        Generates the raw data for the report, sends it to a parser

        :return: None
        """
        results = self.run_query()

        data_parser = self._parse_data_to_experiments()
        data_parser.send(None)
        for status in results.group_status.buckets:
            for VO in results.group_status.buckets[status].group_VO.buckets:
                for CommonName in VO['group_CommonName'].buckets:
                    data_parser.send((CommonName.key, VO.key, status,
                                          CommonName['numJobs'].value,
                                          CommonName['WallHours'].value))

        return

    def _parse_data_to_experiments(self):
        """Coroutine that parses raw data and stores the information in the
        Experiment and User class instances"""
        while True:
            name, expname, status, count, hours = yield
            count = int(count)
            hours = float(hours)

            if self.verbose:
                print name, expname, status, count, hours

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

    def generate_report_file(self):
        """Reads the Experiment and User objects and generates the report
        HTML file

        :return: None
        """
        if len(self.experiments) == 0:
            print "No experiments; nothing to report"
            self.no_email = True
            return
        total_hrs = 0
        total_jobs = 0
        table = ""

        def tdalign(info, align):
            """HTML generator to wrap a table cell with alignment"""
            return '<td align="{0}">{1}</td>'.format(align, info)

        for key, exp in self.experiments.items():
            for uname, user in exp.users.items():
                failure_rate = round(user.get_failure_rate(), 1)
                waste_per = round(user.get_waste_per(), 1)

                linemap = ((key, 'left'), (uname, 'left'),
                           (NiceNum.niceNum(user.success[0] + user.failure[0]), 'right'),
                           (NiceNum.niceNum(user.failure[0]), 'right'),
                           (failure_rate, 'right'),
                           (NiceNum.niceNum(user.success[1] + user.failure[1],1), 'right'),
                           (NiceNum.niceNum(user.failure[1], 1), 'right'), (waste_per, 'right'))

                table += '\n<tr>' + ''.join((tdalign(key, al) for key, al in linemap)) + '</tr>'

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
        
        with open(self.template, 'r') as f:
            self.text = f.read()

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
        if self.test_no_email(self.email_info['to']['email']):
            return

        TextUtils.sendEmail((self.email_info['to']['name'],
                             self.email_info['to']['email']),
                            self.title,
                            {"html": self.text},
                            (self.email_info['from']['name'],
                             self.email_info['from']['email']),
                            self.email_info["smtphost"])
        self.logger.info("Sent reports to {0}".format(", ".join(self.email_info['to']['email'])))
        return


def main():
    args = parse_opts()

    # Set up the configuration
    config = get_configfile(override=args.config)

    templatefile = get_template(override=args.template, deffile=default_templatefile)

    try:
        r = WastedHoursReport(config,
                                   args.start,
                                   args.end,
                                   templatefile,
                                   is_test=args.is_test,
                                   verbose=args.verbose,
                                   no_email=args.no_email,
                                   ov_logfile=args.logfile)
        r.run_report()
    except Exception as e:
        runerror(config, e, traceback.format_exc(), logfile)
        sys.exit(1)

    sys.exit(0)


if __name__ == "__main__":
    main()