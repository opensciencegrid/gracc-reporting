import sys
import traceback
import re

from elasticsearch_dsl import Search

from . import Reporter, runerror, get_configfile, get_template, coroutine
from . import TextUtils, NiceNum


"""
Should use same calculation as current Wasted Hours report to get wasted hours, percentage, etc. I think this means we'll use a very similar query.
Add FIFE Logo at the top,
Sort by Wasted Hours (Most to least)
Should be able to constrain by host description (let's constrain to gpgrid for us) (CONFIGURABLE) - argparse (DONE)
I was also thinking, maybe we only want to show the top 100 or something like that (CONFIGURABLE) - argparse (DONE)
don't show user who wasted less than X% (CONFIGURABLE) - config file
and ran less than 1000 hours (CONFIGURABLE) - config file 
"""

default_templatefile = 'template_topwastedhoursvo.html'
logfile = 'topwastedhoursvo.log'
perc_cutoff = 0.5
hours_cutoff = 1000


@Reporter.init_reporter_parser
def parse_opts(parser):
    """
    Specific argument parser for this report.  The decorator initializes the
    argparse.ArgumentParser object, calls this function on that object to
    modify it, and then returns the Namespace from that object.

    :param parser: argparse.ArgumentParser object that we intend to add to
    :return: None
    """
    parser.add_argument("-E", "--experiment", dest="vo",
                        help="experiment name", default=None, required=True)
    parser.add_argument("-F", "--facility", dest="facility",
                        help="facility name/host description", default=None, required=True)
    parser.add_argument("-N", "--numrank", dest="numrank",
                        help="Number of Users to rank",
                        default=100, type=int)
    pass


class User:
    """
    Holds all user-specific information for this report

    :param str user_name: username of user
    """

    def __init__(self, user_name):
        self.user = user_name

        # When filled, these both will be of the form
        # {'Njobs': <value>, 'CoreHours': <value>}
        self.success = {}
        self.failure = {}

        self.total_Njobs = 0
        self.total_CoreHours = 0

    @staticmethod
    def _check_datadict(data):
        """
        Data validation for incoming data dictionary
        :param dict data: Dict of the form
        {'Status': [Success|Failure], ['Njobs'|'CoreHours']: <value>}

        :return bool: True if data passes test
        """
        return 'Status' in data and ('Njobs' in data or 'CoreHours' in data)

    def add_data(self, datadict):
        """
        Adds to the success or failure dict for User instance

        :param njobs: Number of jobs in summary record
        :param wall_duration: Wall duration in summary record
        :return None:
        """
        if self._check_datadict(datadict):
            rawattr = datadict['Status'].lower()
            selfrawattr = getattr(self, rawattr)
            del datadict['Status']
            for key in datadict:
                selfrawattr[key] = datadict[key]

                # Update the appropriate total
                totalkey = 'total_{0}'.format(key)
                setattr(self, totalkey, getattr(self, totalkey) + datadict[key])
        else:
            # Data validation
            raise ValueError("Improper format for data passing.  "
                             "Must be a dict with keys Status, and (Njobs or CoreHours)")

    def get_job_failure_percent(self):
        """
        Calculates the failure rate of a user's jobs
        (Njobs failed / Njobs total)
        """
        failure_rate = 0
        if self.total_Njobs > 0:
            failure_rate = (self.failure['Njobs'] / self.total_Njobs) * 100.
        return failure_rate

    def get_wasted_hours_percent(self):
        """
        Gets a user's wasted hours percentage
        (Failed CoreHours/ Total CoreHours)
        """
        waste_per = 0
        if self.total_CoreHours > 0:
            waste_per = (self.failure['CoreHours'] / self.total_CoreHours) * 100.
        return waste_per


# class Experiment:
#     """
#     Hold all experiment-specific information for this report
#
#     :param exp_name: Experiment name
#     """
#     def __init__(self, exp_name):
#         self.experiment = exp_name
#         self.success = [0, 0]
#         self.failure = [0, 0]
#         self.users = {}
#
#     def add_user(self, user_name, user):
#         """
#         Adds user to an experiment
#
#         :param user_name: username of user
#         :param User user: User object holding user's info
#         :return: None
#         """
#         self.users[user_name] = user


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
    def __init__(self, config, start, end, template, vo,
                 numrank=100, facility=None, is_test=True,
                 verbose=False, no_email=False, ov_logfile=None):
        report = 'TopWastedHoursVO'
        self.vo = vo

        if ov_logfile:
            rlogfile = ov_logfile
            logfile_override = True
        else:
            rlogfile = logfile
            logfile_override = False

        # self.title = "{0:s} Wasted Hours on GPGrid ({1:s} - {2:s})"\
        #                     .format("FIFE", start, end)

        Reporter.__init__(self, report, config, start, end=end,
                          verbose=verbose, is_test=is_test,
                          no_email=no_email, logfile=rlogfile,
                          logfile_override=logfile_override, check_vo=True)
        self.template = template
        self.facility = facility
        self.numrank = numrank
        self._get_configfile_limits()
        self.users = {}
        self.experiments = {}
        self.connect_str = None
        self.text = ''
        self.title = "Top {0} Users in {1} Ranked by Wasted Hours on {2} " \
                     "({3:%Y-%m-%d %H:%M} - {4:%Y-%m-%d %H:%M})".format(
                        self.numrank,
                        self.vo,
                        self.facility,
                        self.start_time,
                        self.end_time)
        self.dnusermatch_CILogon = re.compile('.+CN=UID:(\w+)$')
        self.dnusermatch_FNAL = re.compile('.+\/(.+\.fnal\.gov)$')

    def run_report(self):
        """Higher-level method to run all the other methods in report
        generation"""
        self.generate()
        self.generate_report_file()
        # self.send_report()
        return

    def query(self):
        """
        Method to query Elasticsearch cluster for this report's information

        :return elasticsearch_dsl.Search: Search object containing ES query
        """
        wildcardProbeNameq = 'condor:fifebatch?.fnal.gov'

        starttimeq = self.start_time.isoformat()
        endtimeq = self.end_time.isoformat()

        s = Search(using=self.client, index=self.indexpattern) \
            .filter("wildcard", ProbeName=wildcardProbeNameq) \
            .filter("range", EndTime={"gte": starttimeq, "lt": endtimeq}) \
            .filter("term", Host_description=self.facility) \
            .filter("term", VOName=self.vo) \
            .filter("term", ResourceType="Payload") \
            [0:0]   # Only print aggregations

        # Aggregations

        Buckets = s.aggs.bucket('DN', 'terms', field='DN', size=2**31-1) \
            .bucket('Status', 'filters', filters={
                'Success': {'bool': {'must': {'term': {'Resource_ExitCode': 0}}}},
                'Failure': {'bool': {'must_not': {'term': {'Resource_ExitCode': 0}}}}})

        # Metrics
        Buckets.metric('Njobs', 'sum', field='Njobs')\
            .metric('CoreHours', 'sum', field='CoreHours')

        if self.verbose:
            print s.to_dict()

        return s

    def generate(self):
        """
        Generates the raw data for the report

        :return: None
        """
        results = self.run_query()
        userparser = self._parse_data_to_users()

        for bucket in results.DN.buckets:
            dn = bucket.key
            for status in ('Success', 'Failure'):
                for label in ('CoreHours', 'Njobs'):
                    statusbucket = getattr(bucket.Status.buckets, status)
                    userparser.send((dn, status, label,
                                     getattr(statusbucket, label).value))

        return

    @coroutine
    def _parse_data_to_users(self):
        """

        :return:
        """
        while True:
            dn, status, label, value = yield
            user = self._parse_dn(dn)

            if user not in self.users:
                u = User(user)
                self.users[user] = u
            else:
                u = self.users[user]
            u.add_data({'Status': status, label: value})

    def _parse_dn(self, trydn):
        """
        Parse a DN to extract the FNAL username

        :param str trydn: DN that we want to try to parse
        :return userid: Username or trydn if parsing failed
        """
        userid = trydn
        try:
            # Grabs the first parenthesized subgroup in the
            # hit['CommonName'] string, where that subgroup comes
            # after "CN=UID:"
            userid = self.dnusermatch_CILogon.match(trydn).group(1)
        except AttributeError:
            # If this doesn't match CILogon standard, see if it
            # matches *.fnal.gov string at the end.  If so,
            # it's a managed proxy most likely, so give the last part of
            # the string.
            # e.g. for DN "/DC=org/DC=opensciencegrid/O=Open Science Grid/OU=Services/CN=novaproduction/nova-offline.fnal.gov" grab "nova-offline.fnal.gov"
            try:
                userid = self.dnusermatch_FNAL.match(trydn).group(1)
            except AttributeError:
                userid = trydn  # Just print the DN string, move on
        finally:
            return userid

    def _get_configfile_limits(self):
        """Get limits from config file"""
        for attr in ('hours_cutoff', 'perc_cutoff'):
            try:
                value = self.config[self.vo.lower()][self.report_type.lower()][attr]
            except KeyError:
                value = globals()[attr]
                self.logger.warning('Could not find value for attribute {0}'
                                    ' in config file.  Will use module '
                                    'default of {1}'.format(attr, value))
            finally:
                setattr(self, attr, value)


    def generate_report_file(self):
        """Reads the User objects and generates the report
        HTML file

        :return: None
        """
        # All of self.users sorted
        sorteduserlist = sorted(self.users.values(),
                                key=lambda user: user.get_wasted_hours_percent(),
                                reverse=True)

        # Generate report lines, with some cutoffs
        all_report_lines_gen = (
            (user.user,
             self.vo,
             NiceNum.niceNum(user.failure['CoreHours'], 1),
             NiceNum.niceNum(user.get_wasted_hours_percent(), 0.1),
             NiceNum.niceNum(user.total_CoreHours, 1),
             NiceNum.niceNum(user.failure['Njobs'], 1),
             NiceNum.niceNum(user.get_job_failure_percent(), 0.1),
             NiceNum.niceNum(user.total_Njobs, 1))
            for user in sorteduserlist
            # Cutoffs:  Core hours and Wasted Hours Percent
            if user.total_CoreHours >= self.hours_cutoff
               and user.get_wasted_hours_percent() / 100. >= self.perc_cutoff
        )

        top_lines_gen = ((count,) + line
                         for count, line in enumerate(all_report_lines_gen, start=1)
                         if count <= self.numrank
                         )


        # Break these into internal functions

        table = ''

        # Header HTML
        headerlist = ['Rank', 'User', 'VO', 'Hours Wasted',
                  '% Hours Wasted of Total', 'Total Used Wall Hours',
                  'Total Jobs Failed', '% Jobs Failed', 'Total Jobs Ran']

        if self.verbose:
            print headerlist
        #     for line in top_lines_gen:
        #         print line

        header = ''.join(('<th>{0}</th>'.format(elt) for elt in headerlist))

        # Generate table lines
        def tdalign(info, align):
            """HTML generator to wrap a table cell with alignment"""
            return '<td align="{0}">{1}</td>'.format(align, info)

        lineal = ('right', 'left', 'left', 'right', 'right', 'right', 'right', 'right', 'right')
        for line in top_lines_gen:
            if self.verbose:
                print line
            linemap = zip(line, lineal)
            table += '\n<tr>' + ''.join((tdalign(key, al) for key, al in linemap)) + '</tr>'

        htmldict = dict(title=self.title, table=table, header=header)

        with open(self.template, 'r') as f:
            self.text = f.read()

        self.text = self.text.format(**htmldict)

        with open('/tmp/test.html', 'w') as f:
            f.write(self.text)

        # if self.verbose:
        #     print total_jobs, total_hrs





        # Read in the template
        # Create HTML lines - make a table
        # Make all substitutions

        # Cutoff:  Only act on the first self.numrank entries
        # for count, line in enumerate(all_report_lines_gen, start=1):
        #     if count <= self.numrank:
        #         # Do some work here
        #         print count, line

        # print self.title

    def _line_to_html(self):


        # if len(self.experiments) == 0:
        #     print "No experiments; nothing to report"
        #     self.no_email = True
        #     return
        # total_hrs = 0
        # total_jobs = 0
        # table = ""
        #
        # def tdalign(info, align):
        #     """HTML generator to wrap a table cell with alignment"""
        #     return '<td align="{0}">{1}</td>'.format(align, info)
        #
        # for key, exp in self.experiments.items():
        #     for uname, user in exp.users.items():
        #         failure_rate = round(user.get_failure_rate(), 1)
        #         waste_per = round(user.get_waste_per(), 1)
        #
        #         linemap = ((key, 'left'), (uname, 'left'),
        #                    (NiceNum.niceNum(user.success[0] + user.failure[0]), 'right'),
        #                    (NiceNum.niceNum(user.failure[0]), 'right'),
        #                    (failure_rate, 'right'),
        #                    (NiceNum.niceNum(user.success[1] + user.failure[1],1), 'right'),
        #                    (NiceNum.niceNum(user.failure[1], 1), 'right'), (waste_per, 'right'))
        #
        #         table += '\n<tr>' + ''.join((tdalign(key, al) for key, al in linemap)) + '</tr>'
        #
        #         if self.verbose:
        #             total_hrs += (user.success[1] + user.failure[1])
        #             total_jobs += (user.success[0] + user.failure[0])
        #
        # headerlist = ['Experiment', 'User', 'Total #Jobs', '# Failures',
        #               'Failure Rate (%)', 'Wall Duration (Hours)',
        #               'Time Wasted (Hours)', '% Hours Wasted']
        #
        # header = ''.join(('<th>{0}</th>'.format(elt) for elt in headerlist))
        #
        # # Yes, the header and footer are the same on purpose
        # htmldict = dict(title=self.title, table=table,
        #                 header=header, footer=header)
        #
        # with open(self.template, 'r') as f:
        #     self.text = f.read()
        #
        # self.text = self.text.format(**htmldict)
        #
        # if self.verbose:
        #     print total_jobs, total_hrs
        # return
        pass

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
                              args.vo,
                              numrank=args.numrank,
                              facility=args.facility,
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