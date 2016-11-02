import abc
import argparse
from datetime import datetime
import re
import smtplib
from email.mime.text import MIMEText
import logging

from elasticsearch import Elasticsearch

import TextUtils
from IndexPattern import indexpattern_generate
from TimeUtils import TimeUtils


class Reporter(TimeUtils):
    __metaclass__ = abc.ABCMeta

    def __init__(self, config, start, end=None, verbose=False, raw=True):
        """Constructor for OSGReporter
        Args:
                config(Configuration) - configuration file
                start(str) - start date (YYYY/MM/DD) of the report
                end(str,optional) - end date (YYYY/MM/DD) of the report,
                    defaults to 1 month from start date
                verbose(boolean,optional) - print debug messages to stdout
        """
        TimeUtils.__init__(self)
        self.header = []
        if config:
            self.config = config.config
        self.start_time = start
        self.verbose = verbose
        self.end_time = end
        self.epochrange = None
        self.indexpattern = indexpattern_generate(self.start_time,
                                                  self.end_time, raw)
        self.logfile = ('reports.log')  # Can be overwritten in __init__ method
                                        # in subclasses

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
    def generate_report_file(self, report):
        """Method to generate the report class"""
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
        parser = argparse.ArgumentParser()
        parser.add_argument("-c", "--config", dest="config",
                            default=None, help="report configuration file")
        parser.add_argument("-v", "--verbose", dest="verbose",
                            action="store_true", default=False,
                            help="print debug messages to stdout")
        parser.add_argument("-E", "--experiment", dest="vo",
                            help="experiment name", default=None)
        parser.add_argument("-F", "--facility", dest="facility",
                            help="facility name", default=None)
        parser.add_argument("-T", "--template",dest="template",
                            help="template_file", default=None)
        parser.add_argument("-s", "--start", dest="start",
                            help="report start date YYYY/MM/DD HH:mm:SS or "
                                 "YYYY-MM-DD HH:mm:SS (required)")
        parser.add_argument("-e", "--end", dest="end",
                            help="report end date YYYY/MM/DD HH:mm:SS or "
                                 "YYYY-MM-DD HH:mm:SS")
        parser.add_argument("-d", "--dryrun", dest="is_test",
                            action="store_true", default=False,
                            help="send emails only to _testers")
        parser.add_argument("-D", "--debug", dest="debug",
                            action="store_true", default=False,
                            help="print detailed debug messages to log file")
        parser.add_argument("-n", "--nomail", dest="no_email",
                            action="store_true", default=False,
                            help="Do not send the email.  "
                                 "Use this with -v to also get verbose output")

        arguments = parser.parse_args()
        return arguments

    def setupgenLogger(self, reportname):
        """Creates logger for Reporter class.

        For non-verbose use, use WARNING level (or above)
        to have messages show up on screen, INFO or DEBUG otherwise.

        For verbose use, use INFO level or above for messages to show on screen

        Returns logging.getLogger object
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
        s.quit()
        print "Successfully sent error email"
    except Exception as e:
        err = "Error:  unable to send email.\n%s\n" % e
        print err
        raise

    return None
