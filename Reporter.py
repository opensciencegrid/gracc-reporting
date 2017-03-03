import abc
import argparse
from datetime import datetime
import re
import sys
import smtplib
from email.mime.text import MIMEText
import logging
import operator

from elasticsearch import Elasticsearch

import TextUtils
from IndexPattern import indexpattern_generate
from TimeUtils import TimeUtils


class ContextFilter(logging.Filter):
    """This is a class to inject contextual information into the record

    :param str vo: VO to inject into Reporter information
    """
    def __init__(self, vo):
        self.vo = vo

    def filter(self, record):
        """Add vo to record

        :param LogRecord record: Logger record to append info to
        :return bool: Success or not
        """
        record.vo = self.vo
        return True


class Reporter(TimeUtils):
    """
    Base class for all OSG reports

    :param str report: Which report is getting run
    :param Configuration.Configuration config: Report Configuration object
    :param str start: Start time of report range
    :param str end: End time of report range
    :param bool verbose: Verbose flag
    :param bool raw: True = Use GRACC raw ES indices;
        False = use Summary indices)
    :param bool allraw: True = short-circuit index-pattern optimization and
        search all raw indices
    :param str template: Filename of HTML template to inject report data into
    :param bool is_test: Dry-run or real run
    :param bool no_email: If true, don't send any emails
    :param str title: Report title
    :param str logfile: Filename of log file for report
    """
    __metaclass__ = abc.ABCMeta

    def __init__(self, report, config, start, end=None, verbose=False,
                 raw=True, allraw=False, template=None, is_test=False, no_email=False,
                 title=None, logfile=None):

        TimeUtils.__init__(self)
        self.header = []
        if config:
            self.config = config.config
        self.start_time = start
        self.end_time = end
        self.verbose = verbose
        self.no_email = no_email
        self.is_test = is_test
        self.template = template
        self.epochrange = None
        self.indexpattern = self.indexpattern_generate(raw, allraw)
        self.report_type = report
        if logfile:
            self.logfile = logfile
        else:
            self.logfile = 'reports.log'
        self.email_info = self.__get_email_info()
        self.logger = self.__setupgenLogger()
        self.client = self.__establish_client()

    @staticmethod
    def parse_opts():
        """Parses command line options

        :return: argparse.ArgumentParser object with parsed arguments for report
        """
        parser = argparse.ArgumentParser()
        parser.add_argument("-c", "--config", dest="config",
                            default=None, help="report configuration file",
                            required=True)
        parser.add_argument("-v", "--verbose", dest="verbose",
                            action="store_true", default=False,
                            help="print debug messages to stdout")
        parser.add_argument("-s", "--start", dest="start",
                            help="report start date YYYY/MM/DD HH:mm:SS or "
                                 "YYYY-MM-DD HH:mm:SS")
        parser.add_argument("-e", "--end", dest="end",
                            help="report end date YYYY/MM/DD HH:mm:SS or "
                                 "YYYY-MM-DD HH:mm:SS")
        parser.add_argument("-T", "--template",dest="template",
                            help="template_file", default=None)
        parser.add_argument("-d", "--dryrun", dest="is_test",
                            action="store_true", default=False,
                            help="send emails only to _testers")
        parser.add_argument("-n", "--nomail", dest="no_email",
                            action="store_true", default=False,
                            help="Do not send email. ")

        return parser

    @staticmethod
    def init_reporter_parser(specific_parser):
        """
        Decorator function that initializes all of our report-specific parser
        functions

        :param specific_parser: report-specific parser-function to parse
        :return: Decorated report-specific wrapper function reference
        """
        def wrapper():
            """
            Wrapper function that calls the report-specific parser function
            :return: argparse.ArgumentParser Namespace from specific_parser
            """
            parser = Reporter.parse_opts()
            specific_parser(parser)
            return parser.parse_args()
        return wrapper

    def indexpattern_generate(self, raw=True, allraw=False):
        """Returns the Elasticsearch index pattern based on the class
        variables of start time and end time, and the flags raw and allraw.
        Note that this doesn't inherit raw and allraw from the instance
        attributes in case we want to switch these flags without creating a
        new instance of this class.

        :param bool raw:  Query GRACC raw records (False = query Summary records)
        :param bool allraw: Short-circuit indexpattern_generate and simply look
            at all raw records
        """
        return indexpattern_generate(self.start_time, self.end_time, raw,
                                     allraw)

    def __setupgenLogger(self):
        """Creates logger for Reporter class.

        For non-verbose use, use WARNING level (or above)
        to have messages show up on screen, INFO or DEBUG otherwise.

        For verbose use, use INFO level or above for messages to show on screen

        :return: logging.getLogger object
        """
        logger = logging.getLogger(self.report_type)
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

        try:
            f = ContextFilter(self.vo)
            fh.addFilter(f)
            logfileformat = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(vo)s - %(message)s")
        except (NameError, AttributeError):
            logfileformat = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s")

        fh.setFormatter(logfileformat)

        logger.addHandler(ch)
        logger.addHandler(fh)

        return logger

    def __establish_client(self):
        """Initialize and return the elasticsearch client

        :return: elasticsearch.Elasticsearch object
        """
        try:
            client = Elasticsearch('https://gracc.opensciencegrid.org/q',
                                   use_ssl=True,
                                   verify_certs=False,
                                   # ca_certs = 'gracc_cert/lets-encrypt-x3-cross-signed.pem',
                                   #  client_cert = 'gracc_cert/gracc-reports-dev.crt',
                                   #  client_key = 'gracc_cert/gracc-reports-dev.key',
                                   timeout=60)
        except Exception as e:
            self.logger.exception("Couldn't initialize Elasticsearch instance."
                                  " Error/traceback: {0}".format(e))
            sys.exit(1)
        else:
            return client

    def __get_email_info(self):
        """
        Parses config file to grab email-related information.

        :return dict: Dict of sender, recipient(s), smtphost info
        """
        email_info = {}

        # Get recipient(s) info
        if self.is_test:
            emails = re.split('[; ,]', self.config.get("email", "test_to_emails"))
            names = re.split('[; ,]', self.config.get("email", "test_to_names"))
        else:
            try:
                vo = self.vo
                emails = re.split('[; ,]', self.config.get(vo.lower(), "to_emails") +
                              ',' + self.config.get("email", "test_to_emails"))
                names = []
            except AttributeError:      # No vo-specific info in config file
                emails = re.split('[; ,]', self.config.get("email",
                                                           "{0}_to_emails".format(
                                                               self.report_type))
                                  + ',' + self.config.get("email", "test_to_emails"))
                names = re.split('[; ,]', self.config.get("email",
                                                           "{0}_to_names".format(
                                                               self.report_type))
                                  + ',' + self.config.get("email", "test_to_names"))

        email_info["to_emails"] = emails
        email_info["to_names"] = names

        # Get other global info from config file
        for key in ("from_email", "from_name", "smtphost"):
            email_info[key] = self.config.get("email", key)

        return email_info

    @abc.abstractmethod
    def query(self):
        """Method to define report's Elasticsearch query. Must be overridden"""
        pass

    def generate_report_file(self):
        """Method to generate the report file, if format_report below is not
        used."""
        pass

    def format_report(self):
        """Method to be overridden by reports that need simultaneous
        CSV and HTML generation"""
        pass

    def send_report(self, title=None):
        """Send reports as ascii, csv, html attachments.

        :param str title: Title of report
        """
        text = {}
        content = self.format_report()

        if not content:
            self.logger.error("There is no content being passed to generate a "
                              "report file")
            sys.exit(1)

        if title:
            use_title = title
        else:
            use_title = "GRACC Report"

        if self.test_no_email(self.email_info["to_emails"]):
            return

        emailReport = TextUtils.TextUtils(self.header)
        text["text"] = emailReport.printAsTextTable("text", content)
        text["csv"] = emailReport.printAsTextTable("csv", content)
        htmldata = emailReport.printAsTextTable("html", content,
                                                template=self.template)

        if self.header:
            htmlheader = "\n".join(['<th>{0}</th>'.format(headerelt)
                                    for headerelt in self.header])

        if self.template:
            with open(self.template, 'r') as t:
                htmltext= "".join(t.readlines())

            # Build the HTML file from the template
            htmltext = htmltext.replace('$TITLE', use_title)
            if "$HEADER" in htmltext and htmlheader:
                htmltext = htmltext.replace('$HEADER', htmlheader)
            text["html"] = htmltext.replace('$TABLE', htmldata)

        else:
            text["html"] = "<html><body><h2>{0}</h2><table border=1>{1}</table></body></html>".format(use_title, htmldata)

        TextUtils.sendEmail((self.email_info["to_names"],
                             self.email_info["to_emails"]),
                            use_title, text,
                            (self.email_info["from_name"],
                             self.email_info["from_email"]),
                            self.email_info["smtphost"],
                            html_template=self.template)
        return

    @abc.abstractmethod
    def run_report(self):
        """Method within report that actually runs the various other methods
        in the Reporter and report-specific class.  Must be overridden."""
        pass

    @staticmethod
    def sorted_buckets(agg, key=operator.attrgetter('key')):
        """Sorts the Elasticsearch Aggregation buckets based on the key you
        specify

        :param agg: Aggregations attribute of ES response containing buckets
        :param key: Key to sort buckets on

        :return: sorted buckets
        """
        return sorted(agg.buckets, key=key)

    def test_no_email(self, emails):
        """
        Checks to see if the no_email flag is True, and takes actions if so.

        :param emails: Emails to print out in info message
        :return bool: True if no_email=True, False otherwise
        """
        if self.no_email:
            self.logger.info("Not sending report")
            self.logger.info("Would have sent emails to {0}.".format(
                ', '.join(emails)))
            return True
        else:
            return False


def runerror(config, error, traceback):
    """
    Global method to email admins if report run errors out

    :param Configuration.Configuration config: Report config
    :param str error: Error raised
    :param str traceback: Traceback from error
    :return None
    """
    admin_emails = re.split('[; ,]', config.config.get("email", "test_to_emails"))
    fromemail = config.config.get("email", "from_email")

    msg = MIMEText("ERROR: {0}\n\n{1}".format(error, traceback))
    msg['Subject'] = "ERROR PRODUCING REPORT: Date Generated {0}".format(
        datetime.now())
    msg['From'] = fromemail
    msg['To'] = ', '.join(admin_emails)

    try:
        s = smtplib.SMTP(config.config.get("email", "smtphost"))
        s.sendmail(fromemail, admin_emails, msg.as_string())
        s.quit()
        print "Successfully sent error email"
    except Exception as e:
        err = "Error:  unable to send email.\n%s\n" % e
        print err
        raise

    return None
