import sys
import traceback
import re

from elasticsearch_dsl import Search

from . import Reporter, runerror, get_configfile, get_template, coroutine
from . import TextUtils, NiceNum

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
                        help="experiment name", type=unicode, required=True)
    parser.add_argument("-F", "--facility", dest="facility",
                        help="facility name/host description", type=unicode,
                        required=True)
    parser.add_argument("-N", "--numrank", dest="numrank",
                        help="Number of Users to rank",
                        default=100, type=int)


class User:
    """
    Holds all user-specific information for this report

    :param str user_name: username of user (or DN if username couldn't
    be determined)
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

        :param dict datadict:  Dictionary of data that satsifies
            self._check_datadict(datadict) == True
        :return None:
        """
        if self._check_datadict(datadict):
            rawattr = datadict['Status'].lower()
            selfrawattr = getattr(self, rawattr)
            del datadict['Status']
            for key, item in datadict.iteritems():
                selfrawattr[key] = item

                # Update the appropriate total
                totalkey = 'total_{0}'.format(key)
                setattr(self, totalkey, getattr(self, totalkey) + item)
        else:
            raise ValueError("Improper format for data passing.  "
                             "Must be a dict with keys Status, and (Njobs or CoreHours)")

    def get_job_failure_percent(self):
        """
        Calculates the failure rate of a user's jobs
        (Njobs failed / Njobs total)

        :return float: Failure rate as a percentage (not decimal)
        """
        return (self.failure['Njobs'] / self.total_Njobs) * 100. if self.total_Njobs > 0 \
            else 0.

    def get_wasted_hours_percent(self):
        """
        Gets a user's wasted hours percentage
        (Failed CoreHours/ Total CoreHours)

        :return float: Wasted Hours as a percentage of total hours (not decimal)
        """
        return (self.failure['CoreHours'] / self.total_CoreHours) * 100. if self.total_CoreHours > 0 \
            else 0.


class TopWastedHoursReport(Reporter):
    """
    Class to hold information about and run Top Wasted Hours report.
    :param str config: Report Configuration file
    :param str start: Start time of report range
    :param str end: End time of report range
    :param str template: Filename of HTML template to generate report
    :param str vo: VO we want to filter this report on
    :param int numrank: How many entries that match other filters we want to show
    :param str facility: Host description in GRACC (e.g. GPGrid)
    :param bool is_test: Whether or not this is a test run.
    :param bool verbose: Verbose flag
    :param bool no_email: If true, don't actually send the email
    :param str ov_logfile: Path to override logfile
    """
    def __init__(self, config, start, end, template, vo,
                 numrank=100, facility=None, is_test=True,
                 verbose=False, no_email=False, ov_logfile=None):
        report = 'TopWastedHoursVO'
        self.vo = vo

        logfile_fname = ov_logfile if ov_logfile is not None else logfile
        logfile_override = True if ov_logfile is not None else False


        super(TopWastedHoursReport, self).__init__(report, config, start,
                                                   end=end, verbose=verbose,
                                                   is_test=is_test,
                                                   no_email=no_email,
                                                   logfile=logfile_fname,
                                                   logfile_override=logfile_override,
                                                   check_vo=True)

        self.template = template
        self.facility = facility
        self.numrank = numrank
        self._get_configfile_limits()
        self.users = {}
        self.text = ''
        self.title = "Top {0} Users in {1} Ranked by Percent Wasted Hours on {2} " \
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
        smsg = "Sent reports to {0}".format(
            ", ".join(self.email_info['to']['email']))
        self.send_report(successmessage=smsg)
        return

    def _get_configfile_limits(self):
        """Get limits from config file"""
        for attr in ('hours_cutoff', 'perc_cutoff'):
            try:
                value = self.config[self.report_type.lower()][self.vo.lower()][attr]
            except KeyError:
                value = globals()[attr]
                self.logger.warning('Could not find value for attribute {0}'
                                    ' in config file.  Will use module '
                                    'default of {1}'.format(attr, value))
            finally:
                setattr(self, attr, value)

    def query(self):
        """
        Method to query Elasticsearch cluster for this report's information

        :return elasticsearch_dsl.Search: Search object containing ES query
        """
        wildcardProbeNameq = 'condor:*.fnal.gov'
        starttimeq = self.start_time.isoformat()
        endtimeq = self.end_time.isoformat()

        if self.verbose:
            self.logger.info(self.indexpattern)

        s = Search(using=self.client, index=self.indexpattern) \
            .filter("wildcard", ProbeName=wildcardProbeNameq) \
            .filter("range", EndTime={"gte": starttimeq, "lt": endtimeq}) \
            .filter("term", Host_description=self.facility) \
            .filter("terms", VOName=self.vo_list) \
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
        Parse the response from the ES server and extract the raw data for
        the report

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
        Coroutine to create User objects and populate them with data

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
        :return str: Username or trydn if parsing failed
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

    def generate_report_file(self):
        """Reads the User objects and generates the report
        HTML file

        :return: None
        """
        # Organize raw data into lines for HTML processing
        # All of self.users sorted
        sorteduserlist = sorted(self.users.itervalues(),
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

        # Enforce cutoff for number of entries to include (self.numrank)
        top_lines_gen = ((count,) + line
                         for count, line in enumerate(all_report_lines_gen, start=1)
                         if count <= self.numrank
                         )

        # Generate HTML for report

        # Column info in (column name, column alignment) form
        columns_setup = [('Rank', 'right'),
                         ('User', 'left'),
                         ('VO', 'left'),
                         ('Hours Wasted', 'right'),
                         ('% Hours Wasted of Total', 'right'),
                         ('Total Used Wall Hours', 'right'),
                         ('Total Jobs Failed', 'right'),
                         ('% Jobs Failed', 'right'),
                         ('Total Jobs Run', 'right')]
        table = ''

        # Generate table lines
        def tdalign(info, align):
            """HTML generator to wrap a table cell with alignment"""
            return '<td align="{0}">{1}</td>'.format(align, info)

        lineal = [elt[1] for elt in columns_setup]
        for line in top_lines_gen:
            if self.verbose:
                print line
            linemap = zip(line, lineal)
            table += '\n<tr>' + ''.join((tdalign(info, al) for info, al in linemap)) + '</tr>'

        if len(table) == 0:
            self.logger.info('The report is empty.  Will not send anything.')
            sys.exit(0)

        # Generate header HTML
        headernames = (elt[0] for elt in columns_setup)
        header = ''.join(('<th>{0}</th>'.format(elt) for elt in headernames))

        # Put it all into the template
        htmldict = dict(title=self.title, table=table, header=header)

        with open(self.template, 'r') as f:
            self.text = f.read()

        self.text = self.text.format(**htmldict)

        return


def main():
    args = parse_opts()

    # Set up the configuration
    config = get_configfile(flag='fife', override=args.config)

    templatefile = get_template(override=args.template, deffile=default_templatefile)

    try:
        r = TopWastedHoursReport(config,
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