import xml.etree.ElementTree as ET
import urllib2
import ast
import os
import re
import smtplib
import email.utils
from email.mime.text import MIMEText
import datetime
import dateutil
import logging
import traceback
import sys

from elasticsearch_dsl import Search, Q

from . import Reporter, runerror, get_configfile


logfile = 'probereport.log'
today = datetime.datetime.now()


@Reporter.init_reporter_parser
def parse_opts(parser):
    """
    Don't need to add any options to Reporter.parse_opts
    """
    pass


class OIMInfo(object):
    """Class to hold and operate on OIM information

    :param bool verbose: Verbose flag
    :param str config: Configuration file
    :param str ov_logfile: Path to logfile override
    """
    # Default OIM URLs
    oim_url = {'rg': 'http://myosg.grid.iu.edu/rgsummary/xml?summary_attrs_showhierarchy=on&summary_attrs_showwlcg=on&summary_attrs_showservice=on&summary_attrs_showfqdn=on&gip_status_attrs_showtestresults=on&downtime_attrs_showpast=&account_type=cumulative_hours&ce_account_type=gip_vo&se_account_type=vo_transfer_volume&bdiitree_type=total_jobs&bdii_object=service&bdii_server=is-osg&all_resources=on&facility_sel%5B%5D=10009&gridtype=on&gridtype_1=on&service=on&service_sel%5B%5D=1&active=on&active_value=1&disable=on&disable_value=0&has_wlcg=on',
        'dt':'http://myosg.grid.iu.edu/rgdowntime/xml?summary_attrs_showservice=on&summary_attrs_showrsvstatus=on&summary_attrs_showfqdn=on&gip_status_attrs_showtestresults=on&downtime_attrs_showpast=&account_type=cumulative_hours&ce_account_type=gip_vo&se_account_type=vo_transfer_volume&bdiitree_type=total_jobs&bdii_object=service&bdii_server=is-osg&start_type=7daysago&start_date={0}%2F{1}%2F{2}&end_type=now&end_date={3}%2F{4}%2F{5}&all_resources=on&facility_sel%5B%5D=10009&gridtype=on&gridtype_1=on&service=on&service_sel%5B%5D=1&active=on&active_value=1&disable=on&disable_value=0&has_wlcg=on'}

    def __init__(self, verbose=False, config=None, ov_logfile=None):
        self.config = Reporter._parse_config(config)
        self.verbose = verbose

        logfile_fname = ov_logfile if ov_logfile is not None else logfile
        logfile_override = True if ov_logfile is not None else False

        self.logfile = self.get_logfile_path(logfile_fname,
                                             override=logfile_override)
        self.logger = self.setupgenLogger("ProbeReport-OIM")
        self.root = None
        self.resourcedict = {}

        self.xml_file = self.get_file_from_OIM(tag='rg')
        if self.xml_file:
            self.rgparse_xml()
            self.logger.info('Successfully parsed OIM file')
        else:
            raise

    def setupgenLogger(self, reportname):
        """Create logger for this class

        :param str reportname: Name of report that gets put in the log
        :return logger: Note that this logger is separte from the ProbeReport
        logger, though it writes to the same file
        """
        logger = logging.getLogger(reportname)
        logger.setLevel(logging.DEBUG)

        # Console handler - info
        ch = logging.StreamHandler()
        if self.verbose:
            ch.setLevel(logging.INFO)
        else:
            ch.setLevel(logging.WARNING)

        # FileHandler
        fh = logging.FileHandler(self.logfile)
        fh.setLevel(logging.DEBUG)
        logfileformat = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        fh.setFormatter(logfileformat)

        logger.addHandler(ch)
        logger.addHandler(fh)

        return logger

    def get_logfile_path(self, fn, override=False):
        """
        Gets log file location.  First tries user override, then tries config 
        file, then some standard locations

        :param str fn: Filename of logfile
        :param bool override: Override this method by feeding in a logfile path
        :return str: Path to logfile where we have permission to write
        """

        if override:
            print "Writing log to {0}".format(fn)
            return fn

        try_locations = ['/var/log', os.path.expanduser('~'), '/tmp']

        try:
            configdir = self.config['default_logdir']
            if configdir in try_locations:
                try_locations.remove(configdir)
            try_locations.insert(0, configdir)
        except KeyError:    # No entry in configfile
            pass

        d = 'gracc-reporting'

        for prefix in try_locations:
            dirpath = os.path.join(prefix, d)
            filepath = os.path.join(prefix, d, fn)

            errmsg = "Couldn't write logfile to {0}.  " \
                     "Moving to next path".format(filepath)

            successmsg = "Writing log to {0}".format(filepath)

            # Does the dir exist?  If not, can we create it?
            if not os.path.exists(dirpath):
                # Try to make the logfile directory
                try:
                    os.mkdir(dirpath)
                except OSError as e:  # Permission Denied or missing directory
                    print e
                    print errmsg
                    continue  # Don't try to write an empty file

            # So dir exists.  Can we write to the logfiles there?
            try:
                with open(filepath, 'a') as f:
                    f.write('')
            except (IOError,
                    OSError) as e:  # Permission Denied comes through as an IOError
                print e, '\n', errmsg
            else:
                print successmsg
                break
        else:
            # If none of the prefixes work for some reason, write to local dir
            filepath = fn

        return filepath

    def dateslist_init(self):
        """Creates dates lists to get passed into OIM urls"""
        startdate = today - datetime.timedelta(days=7)
        rawdateslist = [startdate.month, startdate.day, startdate.year,
                        today.month, today.day, today.year]
        return ['0' + str(elt) if len(str(elt)) == 1 else str(elt)
                for elt in rawdateslist]

    def get_file_from_OIM(self, tag='rg'):
        """Get RG file from OIM for parsing, return the XML file

        :param str tag: If 'rg', go to the resource group OIM page and grab
        that data.  If 'dt', get downtimes page

        :return Response: Response from URL we tried to contact
        """
        tagdict = {'rg': 'Resource Group', 'dt': 'Downtimes'}

        label = tagdict[tag]

        try:
            oim_url = self.config['probe']['oim_url'][tag]
        except KeyError:
            oim_url = self.oim_url[tag]
        finally:
            # print oim_url
            if tag == 'dt':
                oim_url = oim_url.format(*self.dateslist_init())
                print oim_url

        if self.verbose:
            self.logger.info(oim_url)

        try:
            oim_xml = urllib2.urlopen(oim_url)
            self.logger.info("Got OIM {0} file successfully".format(label))
        except (urllib2.HTTPError, urllib2.URLError) as e:
            self.logger.error("Couldn't get OIM {0} file".format(label))
            self.logger.exception(e)
            if tag == 'rg':
                sys.exit(1)
            else:
                return None

        return oim_xml

    def parse(self, other_xml_file=False):
        """
        Parse XML file

        :param bool other_xml_file: If true, we're looking at a downtimes file.
        If false, we're looking at an RG file.
        :return: XML etree root
        """
        if other_xml_file:
            xml_file = other_xml_file
            exit_on_fail = False
            label = "Downtimes"
        else:
            xml_file = self.xml_file
            exit_on_fail = True
            label = "Resource Group"
        try:
            tree = ET.parse(xml_file)
            self.logger.info("Parsing OIM {0} File".format(label))
            root = tree.getroot()
        except Exception as e:
            self.logger.error("Couldn't parse OIM {0} File".format(label))
            self.logger.exception(e)
            if exit_on_fail:
                sys.exit(1)
            else:
                return None

        return root

    def rgparse_xml(self):
        """Take the RG XML file and get relevant information, store it in class
        structures"""
        self.root = self.parse()
        for resourcename_elt in self.root.findall('./ResourceGroup/Resources/Resource'
                                             '/Name'):
            resourcename = resourcename_elt.text

            # Check that resource is active
            activepath = './ResourceGroup/Resources/Resource/' \
                         '[Name="{0}"]/Active'.format(resourcename)
            if not ast.literal_eval(self.root.find(activepath).text):
                continue

            # Skip if resource is disabled
            disablepath = './ResourceGroup/Resources/Resource/' \
                         '[Name="{0}"]/Disable'.format(resourcename)
            if ast.literal_eval(self.root.find(disablepath).text):
                continue

            if resourcename not in self.resourcedict:
                resource_grouppath = './ResourceGroup/Resources/Resource/' \
                                     '[Name="{0}"]/../..'.format(resourcename)
                self.resourcedict[resourcename] = \
                    self.get_resource_information(resource_grouppath,
                                                  resourcename)
        return

    def get_resource_information(self, rgpath, rname):
        """
        Get Resource Info from OIM

        :param str rgpath: XPath path to Resource Group Element to be parsed
        :param rname: Name of resource
        :return dict: dictionary that has relevant OIM information
        """
        xpathsdict = self.config['probe']['xpaths']

        returndict = {}

        # Resource group-specific info
        resource_group_elt = self.root.find(rgpath)
        for key, path in xpathsdict['rg_pathdictionary'].iteritems():
            try:
                returndict[key] = resource_group_elt.find(path).text
            except AttributeError:
                # Skip this.  It means there's no information for this key
                pass

        # Resource-specific info
        resource_elt = resource_group_elt.find(
            './Resources/Resource/[Name="{0}"]'.format(rname))
        for key, path in xpathsdict['r_pathdictionary'].iteritems():
            try:
                returndict[key] = resource_elt.find(path).text
            except AttributeError:
                # Skip this.  It means there's no information for this key
                pass

        return returndict

    def get_downtimes(self):
        """Get downtimes from OIM, return list of probes on resources that are
        in downtime currently

        :return list: List of probe FQDNs that are currently in downtime
        """
        nolist = []
        xml_file = self.get_file_from_OIM(tag='dt')
        if not xml_file:
            return nolist

        root = self.parse(xml_file)
        if not root:
            return nolist

        down_fqdns = []

        for dtelt in root.findall('./CurrentDowntimes/Downtime'):
            fqdn = dtelt.find('./ResourceFQDN').text
            dstime = datetime.datetime.strptime(dtelt.find('./StartTime').text,
                                                "%b %d, %Y %H:%M %p UTC")
            detime = datetime.datetime.strptime(dtelt.find('./EndTime').text,
                                                "%b %d, %Y %H:%M %p UTC")
            if dstime < today < detime:
                self.logger.info("{0} in downtime".format(fqdn))
                down_fqdns.append(fqdn)

        return down_fqdns

    def get_fqdns_for_probes(self):
        """Parses resource dictionary and grabs the FQDNs and Resource Names
        if the resource is flagged as WLCG Interop Accting = True

        :return dict: dictionary with FQDNs and Resource Names
        """
        downtimes = self.get_downtimes()
        oim_probe_dict = {}
        for resourcename, info in self.resourcedict.iteritems():
            if ast.literal_eval(info['WLCGInteropAcct']) and \
                            info['FQDN'] not in downtimes:
                oim_probe_dict[info['FQDN']] = info['Resource']
        return oim_probe_dict


class ProbeReport(Reporter):
    """
    Class to hold information about and generate the probe report

    :param strconfig: Report Configuration file
    :param datetime.datetime start: Start time of report range
    :param bool verbose: Verbose flag
    :param bool is_test: Whether or not this is a test run.
    :param bool no_email: If true, don't actually send the email
    """
    def __init__(self, config, start, verbose=False, is_test=False,
                 no_email=False, ov_logfile=None):
        report = "Probe"

        logfile_fname = ov_logfile if ov_logfile is not None else logfile
        logfile_override = True if ov_logfile is not None else False

        super(ProbeReport, self).__init__(report, config, start, end=start,
                          logfile=logfile_fname,
                          logfile_override=logfile_override, is_test=is_test,
                          verbose=verbose,no_email=no_email)

        self.probematch = re.compile("(.+):(.+)")
        self.estimeformat = re.compile("(.+)T(.+)\.\d+Z")
        self.emailfile = '/tmp/filetoemail.txt'
        self.probe, self.resource = None, None
        self.historyfile = self.statefile_path()
        self.newhistory = []
        self.reminder = False

    def statefile_path(self):
        """
        Gets the logfile directory, makes sure that state file gets put in same
        directory
        
        :return str: Path to statefile $logdir/probereporthistory.log 
        """
        fn = 'probereporthistory.log'
        logdir = os.path.abspath(os.path.dirname(self.logfile))
        return os.path.join(logdir, fn)

    def query(self):
        """Method to query Elasticsearch cluster for Probe Report
        information

        :return elasticsearch_dsl.Search: Search object containing ES query
        """
        startdateq = self.start_time.isoformat()

        if self.verbose: self.logger.info(self.indexpattern)

        s = Search(using=self.client, index=self.indexpattern)\
            .filter(Q({"range": {"@received": {"gte": "{0}".format(startdateq)}}}))\
            .filter("term", ResourceType="Batch")[0:0]

        s.aggs.bucket('group_probename', 'terms', field='ProbeName',
                               size=2**31-1)

        return s

    def lastreportinit(self):
        """Reset the start/end times for the ES query and generate a new
        index pattern based on those"""
        self.start_time = today.replace(
            day=1) - datetime.timedelta(days=1)
        self.end_time = today

        return

    def lastreportquery(self):
        """Method to query Elasticsearch cluster for Probe Report
        information regarding when a probe last reported

        :return elasticsearch_dsl.Search: Search object containing ES query
        """
        ls = Search(using=self.client, index=self.indexpattern)\
            .filter(Q({"range":{"@received":{"gte":"now-1M"}}}))\
            .filter("term", ResourceType="Batch")\
            .filter("wildcard", ProbeName="*:{0}".format(self.probe))[0:0]

        ls.aggs.metric('datemax', 'max', field='@received')

        return ls

    def get_last_report_date(self):
        """
        Runs the last reported-date query and returns the result

        :return str: String describing last report date of a probe
        """
        aggs = self.run_query(overridequery=self.lastreportquery)

        try:
            rawdate = aggs.datemax.value_as_string
            return "{0} at {1} UTC".format(
                *self.estimeformat.match(rawdate)
                .groups())
        except AttributeError:     # no value_as_string = no result
            return "over 1 month ago"
        except Exception as e:
            self.logger.exception(e)

    def get_probenames(self):
        """Function that parses the results of the elasticsearch query and
        parses the ProbeName field for the FQDN of the probename

        :return set: Set of all FQDNs of the probes returned by the ES query
        """
        proberecords = (rec for rec in self.results.group_probename.buckets)
        probenames = (self.probematch.match(proberecord.key)
                      for proberecord in proberecords)
        probes = (probename.group(2).lower()
                  for probename in probenames if probename)
        return set(probes)

    def generate(self, oimdict):
        """Higher-level method that calls the lower-level functions to
        generate the raw data for this report.

        :param dict oimdict:

        :return set: set of probes that are in OIM but not in the last two days of
        records.
        """
        self.results = self.run_query()
        probes = self.get_probenames()

        if self.verbose:
            self.logger.info("Probes in last two days of records: {0}".format(sorted(probes)))

        self.logger.info("Successfully analyzed ES data vs. OIM data")
        oimset = set((key for key in oimdict))
        return oimset.difference(probes)

    def getprev_reported_probes(self):
        """Generator function that yields the probes from the previously
        reported file, as well as whether the previous report date was recent
        or not.  'Recent' is defined in the ::cutoff:: variable.
        """
        # Cutoff is a week ago, probrepdate is last report date for
        # a probe
        cutoff = today - datetime.timedelta(days=7)
        with open(self.historyfile, 'r') as h:
            for line in h:
                proberepdate = dateutil.parser.parse(
                    re.split('\t', line)[1].strip())
                curprobe = re.split('\t', line)[0]

                if proberepdate > cutoff:
                    self.newhistory.append(line)  # Append line to new history
                    self.logger.debug("{0} has been reported on in the past"
                                      " week.  Will not resend report".format(
                        curprobe))
                    prev_reported_recent = True
                else:
                    prev_reported_recent = False

                yield curprobe, prev_reported_recent

    def generate_report_file(self, oimdict):
        """Generator function that generates the report files to send in email.
        This is where we exclude sending emails for those probes we've reported
        on in the last week.

        Yields if there are emails to send, returns otherwise"""
        missingprobes = self.generate(oimdict)

        prev_reported = set()
        prev_reported_recent = set()
        if os.path.exists(self.historyfile):
            for curprobe, is_recent_probe in self.getprev_reported_probes():
                if curprobe not in missingprobes:
                    self.newhistory.pop()   # The probe is no longer missing.  Remove it
                    continue
                prev_reported.add(curprobe)
                if is_recent_probe:
                    prev_reported_recent.add(curprobe)

        assert prev_reported.issuperset(prev_reported_recent)
        prev_reported_old = prev_reported.difference(prev_reported_recent)
        assert prev_reported.issuperset(prev_reported_old)

        self.lastreportinit()
        for elt in missingprobes.difference(prev_reported_recent):
            # Only operate on probes that weren't reported in the last week
            self.probe = elt
            self.resource = oimdict[elt]
            self.lastreport_date = self.get_last_report_date()

            if self.probe in prev_reported_old:
                self.reminder = True    # Reminder flag
            else:
                self.reminder = False

            with open(self.emailfile, 'w') as f:
                # Generate email file
                f.write(self.emailtext())

            # Append line to new history
            self.newhistory.append('{0}\t{1}\n'.format(elt, today.date()))
            yield

        return

    def emailsubject(self):
        """Format the subject for our emails"""
        remindertext = 'REMINDER: ' if self.reminder else ''
        return "{0}{1} Reporting Account Failure dated {2}"\
            .format(remindertext, self.resource, today.date())

    def emailtext(self):
        """Format the text for our emails"""
        infostring = (self.probe, self.resource, self.lastreport_date)
        text = 'The probe installed on {0} at {1} has not reported'\
               ' GRACC records to OSG for the last two days. The last ' \
               'date we received a record from {0} was {2}.  If this '\
               'is due to maintenance or a retirement of this '\
               'node, please let us know.  If not, please check to see '\
               'if your Gratia reporting is active.  The GRACC page at '\
               'https://gracc.opensciencegrid.org/dashboard/db/probe-status'\
               ' shows the latest date a probe has reported records to OSG.'.format(*infostring)
        self.logger.debug('Probe: {0}, Resource {1}, Last Report Date: {2}'.format(*infostring))
        return text

    def send_report(self):
        """Send our emails"""
        if self.test_no_email(self.email_info['to']['email']):
            self.logger.info("Resource name: {0}\tProbe Name: {1}"
                             .format(self.resource, self.probe))

            if os.path.exists(self.emailfile):
                os.unlink(self.emailfile)

            return

        with open(self.emailfile, 'rb') as fp:
            msg = MIMEText(fp.read())

        msg['To'] = ', '.join(self.email_info['to']['email'])
        msg['From'] = email.utils.formataddr((self.email_info['from']['name'],
                                              self.email_info['from']['email']))
        msg['Subject'] = self.emailsubject()

        try:
            smtpObj = smtplib.SMTP(self.email_info["smtphost"])
            smtpObj.sendmail(self.email_info['from']['email'],
                             self.email_info['to']['email'],
                             msg.as_string())
            smtpObj.quit()
            self.logger.info("Sent Email for {0} to {1}".format(self.resource, ', '.join(self.email_info['to']['email'])))
            os.unlink(self.emailfile)
        except Exception as e:
            self.logger.exception("Error:  unable to send email.\n{0}\n".format(e))
            raise

        return

    def cleanup_history(self):
        """Clean up our history file.  We basically rewrite the entire history
        file based on what was populated into self.newhistory in the
        generate_report_file method of this class
        """
        with open(self.historyfile, 'w') as cleanup:
            for line in self.newhistory:
                cleanup.write(line)
        return

    def run_report(self, oimdict):
        """The higher level method that controls the generation and sending
        of the probe report using other methods in this class."""
        rep_files = self.generate_report_file(oimdict)

        try:
            for _ in rep_files:
                self.send_report()
        except Exception as e:
            self.logger.exception(e)

        self.logger.info('Any new reports sent')
        self.cleanup_history()
        return


def main():
    args = parse_opts()

    # Set up the configuration
    config = get_configfile(override=args.config)

    try:
        # Get OIM Information
        oiminfo = OIMInfo(args.verbose, config=config, ov_logfile=args.logfile)
        oim_probe_fqdn_dict = oiminfo.get_fqdns_for_probes()

        startdate = today - datetime.timedelta(days=2)

        # Set up and send probe report
        preport = ProbeReport(config,
                              startdate,
                              verbose=args.verbose,
                              is_test=args.is_test,
                              no_email=args.no_email,
                              ov_logfile=args.logfile)

        preport.run_report(oim_probe_fqdn_dict)
        print 'Probe Report Execution finished'
    except Exception as e:
        runerror(config, e, traceback.format_exc(), logfile)
        sys.exit(1)

    return

if __name__ == '__main__':
    main()
    sys.exit(0)