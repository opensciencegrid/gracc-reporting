import abc
import optparse
from datetime import datetime
import re
import smtplib
from email.mime.text import MIMEText

from elasticsearch import Elasticsearch

import TextUtils
from Configuration import checkRequiredArguments
from IndexPattern import indexpattern_generate
from TimeUtils import TimeUtils


class Reporter(TimeUtils):
    __metaclass__ = abc.ABCMeta

    def __init__(self, config, start, end=None, verbose=False, raw=True):
        """Constructor for OSGReporter
        Args:
                config(Configuration) - configuration file
                start(str) - start date (YYYY/MM/DD) of the report
                end(str,optional) - end date (YYYY/MM/DD) of the report, defaults to 1 month from start date
                verbose(boolean,optional) - print debug messages to stdout
        """
        TimeUtils.__init__(self)
        self.header = []
        self.config = config.config
        self.start_time = start
        self.verbose = verbose
        self.end_time = end
        self.epochrange = None
        self.indexpattern = indexpattern_generate(self.start_time,
                                                  self.end_time, raw)

    def format_report(self):
        pass

    @staticmethod
    def establish_client():
        """Initialize and return the elasticsearch client"""
        client = Elasticsearch(['https://gracc.opensciencegrid.org/q'],
                               use_ssl=True,
                               # verify_certs = True,
                               # ca_certs = 'gracc_cert/lets-encrypt-x3-cross-signed.pem',
                               # client_cert = 'gracc_cert/gracc-reports-dev.crt',
                               # client_key = 'gracc_cert/gracc-reports-dev.key',
                               timeout=60)
        return client

    @abc.abstractmethod
    def query(self):
        """Method to define subclass Elasticsearch query"""
        pass

    @abc.abstractmethod
    def send_report(self, report_type="test"):
        """Send reports as ascii, csv, html attachements """
        text = {}
        content = self.format_report()
        print "header", self.header
        emailReport = TextUtils.TextUtils(self.header)
        text["text"] = emailReport.printAsTextTable("text", content)
        text["csv"] = emailReport.printAsTextTable("csv", content)
        text["html"] = "<html><body><h2>%s</h2><table border=1>%s</table></body></html>" % (self.title, emailReport.printAsTextTable("html", content),)
        emails = self.config.get("email", "%s_to" % (report_type,)).split(",")
        names = self.config.get("email", "%s_realname" % (report_type,)).split(",")
        TextUtils.sendEmail((names, emails), self.title, text, ("Gratia Operation", self.config.get("email", "from")), self.config.get("email", "smtphost"))

    @staticmethod
    def parse_opts():
        """Parses command line options"""
        usage = "Usage: %prog [options]"
        parser = optparse.OptionParser(usage)
        parser.add_option("-c", "--config", dest="config", type="string",
                          help="report configuration file (required)")
        parser.add_option("-v", "--verbose",
                          action="store_true", dest="verbose", default=False,
                          help="print debug messages to stdout")
        parser.add_option("-E", "--experiment",
                          dest="vo", type="string",
                          help="experiment name")
        parser.add_option("-F", "--facility",
                         dest="facility", type="string",
                         help="facility name")
        parser.add_option("-T", "--template",
                          dest="template", type="string",
                          help="template_file")
        parser.add_option("-s", "--start", type="string",
                          dest="start", help="report start date YYYY/MM/DD HH:mm:SS or YYYY-MM-DD HH:mm:SS (required)")
        parser.add_option("-e", "--end", type="string",
                          dest="end", help="report end date YYYY/MM/DD HH:mm:SS or YYYY-MM-DD HH:mm:SS")
        parser.add_option("-d", "--dryrun", action="store_true", dest="is_test", default=False,
                          help="send emails only to _testers")
        parser.add_option("-D", "--debug",
                          action="store_true", dest="debug", default=False,
                          help="print detailed debug messages to log file")
        parser.add_option("-n", "--nomail",
                          action="store_true", dest="no_email", default=False,
                          help="Do not send the email.  Use this with -v to also get verbose output")

        options, arguments = parser.parse_args()
        checkRequiredArguments(options, parser)
        return options, arguments

def runerror(config, error, traceback):
    """Global method to email admins if report run errors out"""
    admin_emails = re.split('[; ,]', config.config.get("email", "test_to"))

    msg = MIMEText("ERROR: {0}\n\nTRACEBACK: {1}".format(error, traceback))
    msg['Subject'] = "ERROR PRODUCING REPORT: Date Generated {0}".format(
        datetime.now())
    msg['From'] = 'sbhat@fnal.gov'
    msg['To'] = ', '.join(admin_emails)

    try:
        s = smtplib.SMTP('smtp.fnal.gov')
        s.sendmail('sbhat@fnal.gov', admin_emails, msg.as_string())
        print "Successfully sent error email"
    except Exception as e:
        err = "Error:  unable to send email.\n%s\n" % e
        print err
        raise
    finally:
        s.quit()

    return None
