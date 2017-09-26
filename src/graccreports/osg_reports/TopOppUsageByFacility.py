import sys
import traceback
import datetime
import copy
from dateutil.relativedelta import *

from elasticsearch_dsl import Search

from . import Reporter, runerror, get_configfile, get_template, coroutine
from . import TextUtils, NiceNum
from NameCorrection import NameCorrection


logfile = 'topoppusage.log'
default_templatefile = 'template_topoppusage.html'
MAXINT = 2**31-1
facilities = {}


# Helper functions
def get_time_range(start=None, end=None, months=None):
    """
    Returns time ranges for current and prior reporting periods  Can either
    handle absolute range (start-end) or a certain number of months prior to
    today's date (months)

    :param str start: Start of current reporting period.  Date/time as 
    datetime.datetime object
    :param str end: Same as start, but end of current reporting period
    :param int months: Number of months prior to current date, to define
    reporting period.
    :return tuple: Tuple of tuples of datetime.datetime objects representing
    date ranges (cur_period, prior_period)  
    """
    if months:
        if start or end:
            raise Exception("Cannot define both months and start/end times")
        end = datetime.datetime.today()
        diff = relativedelta(months=months)
        start = end - diff
    else:
        diff = relativedelta(end, start)
    pri_end = start - relativedelta(days=1)
    pri_start = pri_end - diff
    return (start, end), (pri_start, pri_end)


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
    parser.add_argument("-m", "--months", dest="months",
                        help="Number of months to run report for",
                        default=None, type=int)
    parser.add_argument("-N", "--numrank", dest="numrank",
                        help="Number of Facilities to rank",
                        default=None, type=int)


class Facility(object):
    """
    Class to hold facility information for this report
    :param str name: Name of the facility
    """

    # Mapping dict to check type of data before it's added to an instance of
    # this class
    typedict = {'rg': basestring, 'res': basestring, 'entry': dict,
                'old_entry': dict}

    def __init__(self, name):
        self.name = name
        self.totalhours = 0
        self.oldtotalhours = 0
        self.oldrank = None

        """ Initialize all the data lists
        rg_list = Resource Group List
        res_list = Resource list
        entry_list = Entries of current reporting period data
        old_entry_list = Entries of prior reporting period data
        """
        for st in ('rg', 'res', 'entry', 'old_entry'):
            setattr(self, '{0}_list'.format(st), [])

    def add_hours(self, hours, old=False):
        """
        Add an entry's hours to the appropriate runnig total
        :param float hours: Number of hours from an entry to add
        :param bool old: If true, add hours to prior reporting period total
        :return: None
        """
        if old:
            self.oldtotalhours += hours
        else:
            self.totalhours += hours

    def add_to_list(self, flag, item):
        """
        Method to add information to one of the data lists for this class

        :param str flag: Which list to add to
        :param str, dict item: What to add to the list
        :return: None
        """
        if not isinstance(item, self.typedict[flag]):
            raise TypeError("The item {0} must be of type {1} to add to {2}"\
                    .format(item, self.typedict[flag], flag))
        else:
            tmplist = getattr(self, '{0}_list'.format(flag))
            tmplist.append(item)
            setattr(self, '{0}_list'.format(flag), tmplist)

            if flag == 'entry':
                termsmap = [('OIM_ResourceGroup', 'rg'),
                            ('OIM_Resource', 'res')]
                # Recursive call of the function to auto add RG and Resource
                for key, fl in termsmap:
                    if key in item:
                        self.add_to_list(fl, item[key])
        return


class TopOppUsageByFacility(Reporter):
    """
    Class to hold information and generate Top Opp Usage by Facility report

    :param str config: Report Configuration file
    :param str start: Start time of report range
    :param str end: End time of report range
    :param str template: Filename of HTML template to generate report
    :param bool is_test: Whether or not this is a test run.
    :param bool no_email: If true, don't actually send the email
    :param bool verbose: Verbose flag
    :param int numrank: Number of Facilities to rank.  Default 10.
    :param int months: Number of months prior to today to set start of report
    range
    """
    def __init__(self, config, start=None, end=None, template=None,
                 is_test=False, no_email=False, verbose=False, numrank=10,
                 months=None, ov_logfile=None):
        report = 'news'

        logfile_fname = ov_logfile if ov_logfile is not None else logfile
        logfile_override = True if ov_logfile is not None else False

        super(TopOppUsageByFacility, self).__init__(report, config, start, end,
                                                    verbose=verbose, no_email=no_email,
                                                    is_test=is_test, logfile=logfile_fname,
                                                    logfile_override=logfile_override)

        self.numrank = numrank
        self.template = template
        self.text = ''
        self.table = ''
        self.daterange = get_time_range(self.start_time, self.end_time, months)

        dates_formatted = (x.strftime("%Y-%m-%d %H:%M") for x in self.daterange[0])
        self.title = "Opportunistic Resources provided by the top {0} OSG " \
                     "Sites for the OSG Open Facility ({1} - {2})".format(
                        self.numrank, *dates_formatted)

    def run_report(self):
        """Handles the data flow throughout the report generation.  Generates
        the raw data, the HTML report, and sends the email.

        :return None
        """
        self.generate()
        self.generate_report_file()
        smsg = "Sent reports to {0}".format(
            ", ".join(self.email_info['to']['email']))
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

        probelist = self.config[self.report_type.lower()]['OSG_flocking_probe_list']

        if self.verbose:
            self.logger.info(self.indexpattern)
            self.logger.info(probelist)

        # Elasticsearch query and aggregations
        s = Search(using=self.client, index=self.indexpattern) \
                .filter("range", EndTime={"gte": starttimeq, "lt": endtimeq}) \
                .filter("terms", ProbeName=probelist) \
                .filter("term", ResourceType="Payload")[0:0]

        # Size 0 to return only aggregations

        self.unique_terms = ['OIM_Facility', 'OIM_ResourceGroup',
                             'OIM_Resource']
        cur_bucket = s.aggs.bucket('OIM_Facility', 'terms', field='OIM_Facility',
                                   size=MAXINT)

        for term in self.unique_terms[1:]:
            cur_bucket = cur_bucket.bucket(term, 'terms', field=term,
                                           size=MAXINT, missing='Unknown')

        cur_bucket.metric('CoreHours', 'sum', field='CoreHours')

        s.aggs.bucket('Missing', 'missing', field='OIM_Facility')\
            .bucket('Host_description', 'terms', field='Host_description',
                    size=MAXINT)\
            .metric('CoreHours', 'sum', field='CoreHours')

        return s

    def generate(self):
        """
        Runs the ES query, checks for success, and then
        sends the raw data to parser for processing.

        :return: None
        """
        self.current = True     # Current reporting period or prior

        for self.start_time, self.end_time in self.daterange:
            results = self.run_query()
            f_parser = self._parse_to_facilities()

            unique_terms = self.unique_terms
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
                    for bucket in curBucket[curTerm]['buckets']:
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

            for elt in results['Missing']['Host_description']['buckets']:
                n = NameCorrection(elt['key'], self.config)
                info = n.get_info()
                if info:
                    info['CoreHours'] = elt['CoreHours']['value']
                    data.append(info)

            for entry in data:
                f_parser.send(entry)

            self.current = False

        # Get prior rank
        for oldrank, f in enumerate(
                sorted(facilities.itervalues(), key=lambda x: x.oldtotalhours,
                    reverse=True), start=1):
            f.oldrank = oldrank

        if self.verbose:
            for f in facilities.itervalues():
                print f.name, f.totalhours

        return

    @coroutine
    def _parse_to_facilities(self):
        """
        Coroutine that parses raw data dicts, creates the Facility class
        instances, and stores information in them
        """
        while True:
            entry = yield
            fname = entry['OIM_Facility']

            if fname not in facilities:
                facilities[fname] = Facility(fname)
            f_class = facilities[fname]
            if self.current:
                f_class.add_to_list('entry', entry)
                f_class.add_hours(entry['CoreHours'])
            else:
                f_class.add_to_list('old_entry', entry)
                f_class.add_hours(entry['CoreHours'], old=True)

    def generate_report_file(self):
        """
        Takes the HTML template and inserts the appropriate information to
        generate the final report file

        :return: None
        """
        header = ['Facility', 'Resource Groups', 'Resources', 'Current Rank',
                  'Current Hrs', 'Prior Rank', 'Prior Hrs']

        totaller = self._total_line_gen()
        self.table = ''
        rankhrs = 0     # Total of ranked facilities hours, current period
        prihrs = 0      # Total of all prior period hours
        tothrs = 0      # Total of all current period hours

        for rank, f in enumerate(
                sorted(facilities.itervalues(), key=lambda x: x.totalhours,
                    reverse=True), start=1):    # Creates the ranking here

            tothrs += f.totalhours
            prihrs += f.oldtotalhours

            if rank <= self.numrank: # Only list the top <numrank> facilities
                totaller.send((rank, f))
                rankhrs += f.totalhours

        # Prepare and generate the HTML file from the stored information
        summarytext = "TOTAL WALL HOURS FOR THE OSG OPEN FACILITY: {0}<br/>" \
                      "PRIOR PERIOD HOURS: {1}<br/>" \
                      "TOP {2} SITES WALL HOURS: {3}".format(
            NiceNum.niceNum(tothrs),
            NiceNum.niceNum(prihrs),
            self.numrank,
            NiceNum.niceNum(rankhrs)
        )

        htmlheader = '<th>' + '</th><th>'.join(header) + '</th>'
        htmldict = dict(title=self.title, header=htmlheader, table=self.table,
                        summary=summarytext)

        with open(self.template, 'r') as f:
            self.text = f.read()

        self.text = self.text.format(**htmldict)

        return

    @staticmethod
    def tdalign(info, align):
        """HTML generator to wrap a table cell with alignment"""
        return '<td align="{0}">{1}</td>'.format(align, info)

    @coroutine
    def _total_line_gen(self):
        """
        Coroutine to generate the Facility-level lines from the Facility class
        instances
        """

        while True:
            rank, fclass = yield
            detailler = self._detail_line_gen()

            line = '<tr>{0}{1}{2}{3}{4}{5}{6}</tr>\n'.format(
                self.tdalign(fclass.name, 'left'),
                self.tdalign('<br/>'.join(fclass.rg_list),'left'),
                self.tdalign('<br/>'.join(fclass.res_list), 'left'),
                self.tdalign(rank, 'right'),
                self.tdalign(NiceNum.niceNum(fclass.totalhours), 'right'),
                self.tdalign(fclass.oldrank, 'right'),
                self.tdalign(NiceNum.niceNum(fclass.oldtotalhours), 'right')
                )

            self.table += line

            if len(fclass.res_list) > 1:    # If there's more than one resource
                detailler.send(fclass)

    @coroutine
    def _detail_line_gen(self):
        """
        Coroutine to generate the Resource-level lines for the facilities
        """
        while True:
            fclass = yield

            # Optimization to make sure we don't have to iterate later
            oldres_dict = {old_entry['OIM_Resource']: old_entry['CoreHours']
                           for old_entry in fclass.old_entry_list}

            for entry in fclass.entry_list:
                dline = '{0}{1}{2}{3}{4}{5}'.format(
                    '<td></td>',
                    self.tdalign(entry['OIM_ResourceGroup'], 'left'),
                    self.tdalign(entry['OIM_Resource'], 'left'),
                    '<td></td>',
                    self.tdalign(NiceNum.niceNum(entry['CoreHours']), 'right'),
                    '<td></td>'
                )

                try:
                    oldhrs = NiceNum.niceNum(oldres_dict[entry['OIM_Resource']])
                except KeyError:    # Resource not in old entry dict
                    oldhrs = 'Unknown'
                finally:
                    dline += self.tdalign(oldhrs, 'right')

                dline = '<tr>' + dline + '</tr>\n'
                self.table += dline


def main():
    args = parse_opts()

    # Set up the configuration
    config = get_configfile(override=args.config)

    templatefile = get_template(override=args.template, deffile=default_templatefile)

    try:

        # Create a report object, create a report for the VO, and send it
        r = TopOppUsageByFacility(config,
                                  start=args.start,
                                  end=args.end,
                                  template=templatefile,
                                  months=args.months,
                                  is_test=args.is_test,
                                  no_email=args.no_email,
                                  verbose=args.verbose,
                                  numrank=args.numrank,
                                  ov_logfile=args.logfile)
        r.run_report()
        print "Top Opportunistic Usage per Facility Report execution successful"

    except Exception as e:
        errstring = '{0}: Error running Top Opportunistic Usage Report. ' \
                    '{1}'.format(datetime.datetime.now(),
                                 traceback.format_exc())
        runerror(config, e, errstring, logfile)
        sys.exit(1)


if __name__ == "__main__":
    main()
    sys.exit(0)
