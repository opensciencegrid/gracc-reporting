import sys
import re
import datetime

from elasticsearch_dsl import Search, Q

from . import Reporter, runerror, get_configfile, get_template, coroutine
from . import TextUtils, NiceNum

default_templatefile = 'template_efficiency.html'
logfile = 'efficiencyreport.log'


# Helper functions

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
    parser.add_argument("-E", "--experiment", dest="vo",
                        help="experiment name", type=unicode, required=True)
    parser.add_argument("-F", "--facility", dest="facility",
                        help="facility name", type=unicode, required=True)


class Efficiency(Reporter):
    """
    Class to hold information about and to run Efficiency report.

    :param config: Report Configuration file
    :param str start: Start time of report range
    :param str end: End time of report range
    :param str vo: Experiment to run report on
    :param float hour_limit: Minimum number of hours a user must have run to
    get reported on
    :param float eff_limit: Efficiency limit below which we want to report on a
    user that satisfies hour_limit requirement
    :param str facility: Facility on which we're running report
    :param str template: Filename of HTML template to generate report
    :param bool is_test: Whether or not this is a test run.
    :param bool no_email: If true, don't actually send the email
    :param bool verbose: Verbose flag
    """
    def __init__(self, config, start, end, vo, facility, template,
                 is_test=False, no_email=False, verbose=False, ov_logfile=None):
        report = 'Efficiency'
        self.vo = vo

        logfile_fname = ov_logfile if ov_logfile is not None else logfile
        logfile_override = True if ov_logfile is not None else False

        super(Efficiency, self).__init__(report, config, start, end=end,
                                         verbose=verbose,
                                         logfile=logfile_fname,
                                         logfile_override=logfile_override,
                                         no_email=no_email, is_test=is_test,
                                         check_vo=True)

        self.hour_limit, self.eff_limit = self.__get_limits()
        self.facility = facility
        self.template = template
        self.text = ''
        self.table = ''
        self.cilogon_match = re.compile('.+CN=UID:(\w+)')

        # Patch until we get Gratia probe to report CN again
        # self.non_cilogon_match = re.compile('/CN=([\w\s]+)/?.+?') # Original
        self.non_cilogon_match = re.compile('.+/CN=([\w\s]+)/?.+?')
        # End patch
        self.title = "{0} Users with Low Efficiency ({1}) on the OSG Sites " \
                      "({2} - {3})".format(self.vo, self.eff_limit, start, end)

    def __get_limits(self):
        """

        :return: Grab the min-hours and min-efficiency limits from config dict
        """
        try:
            return (self.config[self.report_type.lower()][self.vo.lower()]['min_{0:s}'.format(tag)]
                    for tag in ('hours', 'efficiency'))
        except KeyError as err:
            err.message += "\n The VO {0:s} was not found in the config file, " \
                           "or did not have an {1:s} report section."  \
                           " Please review the config file to see if changes" \
                           " need to be made and try again\n".format(self.vo.lower(), self.report_type)
            raise

    def run_report(self):
        """Handles the data flow throughout the report generation.  Generates
        the raw data, the HTML report, and sends the email.

        :return None
        """
        self.generate()

        if not self.table:
            self.no_email = True
            self.logger.warn("Report empty for {0}".format(self.vo))
            return

        self.generate_report_file()
        smsg = "Report sent for {0} to {1}".format(
            self.vo, ", ".join(self.email_info['to']['email']))
        self.send_report(successmessage=smsg)
        return

    def query(self):
        """
        Method to query Elasticsearch cluster for EfficiencyReport information

        :return elasticsearch_dsl.Search: Search object containing ES query
        """
        # Gather parameters, format them for the query
        starttimeq = self.start_time.isoformat()
        endtimeq = self.end_time.isoformat()
        wildcardProbeNameq = 'condor:*.fnal.gov'

        if self.verbose:
            self.logger.info(self.indexpattern)

        # Elasticsearch query and aggregations
        s = Search(using=self.client, index=self.indexpattern) \
            .filter("wildcard", ProbeName=wildcardProbeNameq) \
            .filter("range", EndTime={"gte": starttimeq, "lt": endtimeq}) \
            .filter("range", WallDuration={"gt": 0}) \
            .filter("term", Host_description=self.facility) \
            .filter("term", ResourceType="Payload")[0:0]
        # Size 0 to return only aggregations

        qlist = (Q("wildcard", VOName='*{0}*'.format(votest))
                 for votest in self.vo_list
                 if votest != self.vo.lower())

        # Initialize OR chain
        q = Q("wildcard", VOName='*{0}*'.format(self.vo.lower()))

        for query in qlist:     # Build OR chain to check all VOs in volist
            q = q | query

        s = s.query(q)[0:0]

        # Bucket aggs
        Bucket = s.aggs.bucket('group_VOName', 'terms', field='ReportableVOName') \
            .bucket('group_HostDescription', 'terms', field='Host_description') \
            .bucket('group_DN', 'terms', field='DN')   # Patch while we fix gratia probes to include CommonName Field

            # .bucket('group_CommonName', 'terms', field='CommonName')     # Original
            # End patch

        # Metric aggs
        Bucket.metric('WallHours', 'sum', field='CoreHours') \
            .metric('CPUDuration_sec', 'sum', field='CpuDuration')

        return s

    def generate(self):
        """
        Runs the ES query, checks for success, and then
        sends the raw data to parser for processing.

        :return: None
        """
        results = self.run_query()
        pline = self._parse_lines()

        vos = (vo for vo in results.group_VOName.buckets)
        hostdesc = (hd for vo in vos for hd in vo.group_HostDescription.buckets)

        # Patch while we fix gratia probes to include CommonName Field
        # cns = (cn for hd in hostdesc for cn in hd.group_CommonName.buckets)   # Original

        # Note that I'm keeping the variable cns to make the patch less involved.
        # But it's really dns.
        cns = (cn for hd in hostdesc for cn in hd.group_DN.buckets)
        # End patch

        for cn in cns:
            if cn.WallHours.value > self.hour_limit:
                pline.send((cn.key, cn.WallHours.value, cn.CPUDuration_sec.value))

    @coroutine
    def _parse_lines(self):
        """
        Coroutine: For each set of dn, wall hours, cpu time,
        this gets username, calculates efficiency, and sends to HTML formatter
        """
        html_formatter = self._generate_report_lines()
        while True:
            dn, wallhrs, cputime = yield
            user = self._parseCN(dn)
            eff = self._calc_eff(wallhrs, cputime)
            if eff < self.eff_limit:
                if self.verbose:
                    print "{0}\t{1}\t{2}%".format(user, wallhrs, round(eff*100, 1))
                html_formatter.send((user, wallhrs, eff))

    @coroutine
    def _generate_report_lines(self):
        """Coroutine: This generates an HTML line from the raw data
        line and sends it to the tablebuilder"""
        tablebuilder = self._generate_data_table()
        epoch_stamps = self.get_epoch_stamps_for_grafana()
        elist = [elt for elt in epoch_stamps]
        elist_vo = [elt for elt in elist]
        elist_vo.append(self.vo.lower())

        vo_link = 'https://fifemon.fnal.gov/monitor/dashboard/db/' \
                  'experiment-efficiency-details?' \
                  'from={0}&to={1}' \
                  '&var-experiment={2}'.format(*elist_vo)
        vo_html = '<a href="{0}">{1}</a>'.format(vo_link, self.vo)

        while True:
            user, wallhrs, eff = yield

            elist.append(user)
            user_link = "https://fifemon.fnal.gov/monitor/dashboard/db/" \
                        "user-efficiency-details?" \
                        "from={0}&to={1}" \
                        "&var-user={2}".format(*elist)
            user_html = '<a href="{0}">{1}</a>'.format(user_link, user)

            elist.pop()
            htmlline = '<tr><td align="left">{0}</td>' \
                       '<td align="left">{1}</td>'.format(vo_html, self.facility) \
                        + '<td align="left">{0}</td>' \
                          '<td align="right">{1}</td>' \
                          '<td align="right">{2}</td></tr>'.format(user_html,
                                                                   NiceNum.niceNum(wallhrs),
                                                                   round(float(eff), 2))
            tablebuilder.send(htmlline)

    @coroutine
    def _generate_data_table(self):
        """Coroutine: This compiles the data table lines and creates
        the table text (HTML) string"""
        self.table = ""
        while True:
            newline = yield
            self.table += newline

    @staticmethod
    def _calc_eff(wallhours, cpusec):
        """
        Calculate the efficiency given the wall hours and cputime in
        seconds.  Returns percentage

        :param float wallhours: Wall Hours of a bucket
        :param float cpusec: CPU time (in seconds) of a bucket
        :return float: Efficiency of that bucket
        """
        return (cpusec / 3600) / wallhours if wallhours > 0 else 0.

    def _parseCN(self, cn):
        """Parse the CN to grab the username

        :param str cn: CN string from record
        :return str: username as pulled from cn
        """
        m = self.cilogon_match.match(cn)  # CILogon certs
        if m:
            pass
        else:
            m = self.non_cilogon_match.match(cn)
        user = m.group(1)
        return user

    def generate_report_file(self):
        """
        Takes the HTML template and inserts the appropriate information to
        generate the final report file

        :return: None
        """
        header = ['Experiment', 'Facility', 'User', 'Used Wall Hours',
                  'Efficiency']
        htmlheader = '<th>' + '</th><th>'.join(header) + '</th>'
        htmldict = dict(title=self.title, header=htmlheader, table=self.table)

        with open(self.template, 'r') as f:
            self.text = f.read()

        self.text = self.text.format(**htmldict)
        return


def main():
    args = parse_opts()

    # Set up the configuration
    config = get_configfile(flag='fife', override=args.config)

    templatefile = get_template(override=args.template,
                                deffile=default_templatefile)

    try:
        # Create an Efficiency object, create a report for the VO, and send it
        e = Efficiency(config,
                       args.start,
                       args.end,
                       args.vo,
                       args.facility,
                       templatefile,
                       args.is_test,
                       args.no_email,
                       args.verbose,
                       ov_logfile=args.logfile)
        e.run_report()
        print "Efficiency Report execution successful"

    except Exception as e:
        errstring = '{0}: Error running Efficiency Report for {1}. ' \
                    '{2}'.format(datetime.datetime.now(), args.vo,
                                 e)
        runerror(config, e, errstring, logfile)
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
