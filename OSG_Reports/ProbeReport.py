import xml.etree.ElementTree as ET
from datetime import timedelta, date
import urllib2
import ast
import os
import inspect
import re
import smtplib
import email.utils
from email.mime.text import MIMEText
import datetime
import logging
import traceback

from elasticsearch_dsl import Search, Q


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

#import TextUtils
import Configuration
from Reporter import Reporter, runerror

logfile = 'probereport.log'


class OIMInfo(object):
    def __init__(self, verbose=False):
        self.verbose = verbose
        self.logfile = logfile
        self.logger = self.setupgenLogger("ProbeReport-OIM")
        self.e = None
        self.root = None
        self.resourcedict = {}

        self.xml_file = self.get_file_from_OIM()
        if self.xml_file:
            self.parse()
            self.logger.info('Successfully parsed OIM file')

    def setupgenLogger(self, reportname):
        # Create Logger
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

    def get_file_from_OIM(self):
        today = date.today()
        startdate = today - timedelta(days=7)
        rawdateslist = [startdate.month, startdate.day, startdate.year,
                        today.month, today.day, today.year]
        dateslist = ['0' + str(elt) if len(str(elt)) == 1 else str(elt)
                     for elt in rawdateslist]

        oim_url = 'http://myosg.grid.iu.edu/rgsummary/xml?summary_attrs_showhierarchy=on' \
                  '&summary_attrs_showwlcg=on&summary_attrs_showservice=on&summary_attrs_showfqdn=on&gip_status_attrs_showtestresults=on&downtime_attrs_showpast=&account_type=cumulative_hours&ce_account_type=gip_vo&se_account_type=vo_transfer_volume&bdiitree_type=total_jobs&bdii_object=service&bdii_server=is-osg&start_type=7daysago&start_date={0}%2F{1}%2F{2}&end_type=now&end_date={3}%2F{4}%2F{5}&all_resources=on&facility_sel%5B%5D=10009&gridtype=on&gridtype_1=on&service=on&service_sel%5B%5D=1&active=on&active_value=1&disable_value=1&has_wlcg=on'\
        .format(*dateslist)

        try:
            oim_xml = urllib2.urlopen(oim_url)
            self.logger.info("Got OIM file successfully")
        except (urllib2.HTTPError, urllib2.URLError) as e:
            self.logger.exception(e)
            # Email too?

        return oim_xml

    def parse(self):
        self.e = ET.parse(self.xml_file)
        self.root = self.e.getroot()
        self.logger.info("Parsing OIM File")

        for resourcename_elt in self.root.findall('./ResourceGroup/Resources/Resource'
                                             '/Name'):
            resourcename = resourcename_elt.text
            activepath = './ResourceGroup/Resources/Resource/' \
                                     '[Name="{0}"]/Active'.format(resourcename)
            if not ast.literal_eval(self.root.find(activepath).text):
                continue
            if resourcename not in self.resourcedict:
                resource_grouppath = './ResourceGroup/Resources/Resource/' \
                                     '[Name="{0}"]/../..'.format(resourcename)
                self.resourcedict[resourcename] = self.get_resource_information(resource_grouppath,
                                                                      resourcename)
        return

    def get_resource_information(self, rgpath, rname):
        """Uses parsed XML file and finds the relevant information based on the
         dictionary of XPaths.  Searches by resource.

         Arguments:
             resource_grouppath (string): XPath path to Resource Group
             Element to be parsed
             resourcename (string): Name of resource

         Returns dictionary that has relevant OIM information
         """

        # This could (and probably should) be moved to a config file
        rg_pathdictionary = {
            'Facility': './Facility/Name',
            'Site': './Site/Name',
            'ResourceGroup': './GroupName'}

        r_pathdictionary = {
            'Resource': './Name',
            'ID': './ID',
            'FQDN': './FQDN',
            'WLCGInteropAcct': './WLCGInformation/InteropAccounting'
        }

        returndict = {}

        # Resource group-specific info
        resource_group_elt = self.root.find(rgpath)
        for key, path in rg_pathdictionary.iteritems():
            try:
                returndict[key] = resource_group_elt.find(path).text
            except AttributeError:
                # Skip this.  It means there's no information for this key
                pass

        # Resource-specific info
        resource_elt = resource_group_elt.find(
            './Resources/Resource/[Name="{0}"]'.format(rname))
        for key, path in r_pathdictionary.iteritems():
            try:
                returndict[key] = resource_elt.find(path).text
            except AttributeError:
                # Skip this.  It means there's no information for this key
                pass

        return returndict

    def get_fqdns_for_probes(self):
        oim_probe_dict = {}
        for resourcename, info in self.resourcedict.iteritems():
            if ast.literal_eval(info['WLCGInteropAcct']):
                oim_probe_dict[info['FQDN']] = info['Resource']
        return oim_probe_dict


class ProbeReport(Reporter):
    def __init__(self, configuration, start, end, template, is_test,
                     verbose, no_email):
        Reporter.__init__(self, configuration, start, end, verbose)
        self.logfile = logfile
        self.logger = self.setupgenLogger("ProbeReport")
        try:
            self.client = self.establish_client()
        except Exception as e:
            self.logger.exception(e)
        self.probematch = re.compile("(.+):(.+)")
        self.emailfile = 'filetoemail.txt'

    def query(self):

        startdateq = self.dateparse_to_iso(self.start_time)

        s = Search(using=self.client, index='gracc.osg.raw*')\
            .filter(Q({"range": {"@received": {"gte": "{0}".format(startdateq)}}}))\
            .filter(Q({"term": {"ResourceType": "Batch"}}))

        Bucket = s.aggs.bucket('group_probename', 'terms', field='ProbeName',
                               size=1000000000)
        return s

    def get_probenames(self):
        probelist = []
        for proberecord in self.results.group_probename.buckets:
            probename = self.probematch.match(proberecord.key)
            if probename:
                probelist.append(probename.group(2).lower())
            else:
                continue
        self.logger.info(','.join(probelist))
        return set(probelist)

    def generate(self):
        resultset = self.query()
        response = resultset.execute()
        self.results = response.aggregations
        self.logger.info("Successfully queried Elasticsearch")
        probes = self.get_probenames()
        self.logger.info("Successfully analyzed ES data vs. OIM data")
        return probes

    def generate_report_file(self, oimdict, report=None):
        self.esprobes = self.generate()
        oimset = set([key for key in oimdict.keys()])
        with open(self.emailfile, 'w') as f:
            for elt in oimset.difference(self.esprobes):
                f.write('{0}\t{1}\n'.format(oimdict[elt], elt))
        self.logger.info("File written ")
        return

    def send_report(self, report_type="test"):
        #
        # Title: (Site)
        # Gratia
        # Probe
        # Reporting
        # Failure
        #
        # On(Last
        # Record) (Site)
        # stopped
        # reporting
        # GRACC
        # records
        # to
        # OSG.If
        # this is due
        # to
        # maintenance or a
        # retirement
        # of
        # this
        # node, please
        # let
        # us
        # know.If
        # not, please
        # check
        # to
        # see if your
        # Gratia
        # reporting is active.

        admin_emails = re.split('[; ,]', self.config.get("email", "test_to"))
        emailfrom = self.config.get("email","from")
        with open(self.emailfile, 'rb') as fp:
            msg = MIMEText(fp.read())

        msg['To'] = email.utils.formataddr(('Admins', admin_emails))
        msg['From'] = email.utils.formataddr(('GRACC Operations', emailfrom))
        msg['Subject'] = "Gratia Probe report from {0}" \
            .format(datetime.date.today())

        try:
            smtpObj = smtplib.SMTP('smtp.fnal.gov')
            smtpObj.sendmail(emailfrom, admin_emails, msg.as_string())
            smtpObj.quit()
            self.logger.info("Sent Email")
        except Exception as e:
            self.logger.exception("Error:  unable to send email.\n{0}\n".format(e))
            raise

        os.unlink(self.emailfile)

        return

def main():
    args = Reporter.parse_opts()

    config = Configuration.Configuration()
    config.configure(args.config)

    oiminfo = OIMInfo(args.verbose)

    oim_probe_fqdn_dict = oiminfo.get_fqdns_for_probes()

    startdate = datetime.date.today() - timedelta(days=2)

    esinfo = ProbeReport(config,
                           startdate,
                           startdate,
                           args.template,
                           args.is_test,
                           args.verbose,
                           args.no_email)

    esinfo.generate_report_file(oim_probe_fqdn_dict)
    esinfo.send_report()
    print 'Probe Report Execution finished'


if __name__ == '__main__':
    main()