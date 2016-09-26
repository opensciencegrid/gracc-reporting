import TextUtils
import abc
import optparse
from datetime import datetime
from Configuration import checkRequiredArguments


class Reporter(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, config, start, end=None, verbose=False):
        """Constructor for OSGReporter
        Args:
                config(Configuration) - configuration file
                start(str) - start date (YYYY/MM/DD) of the report
                end(str,optional) - end date (YYYY/MM/DD) of the report, defaults to 1 month from start date
                verbose(boolean,optional) - print debug messages to stdout
        """

        self.header = []
        self.config = config.config
        self.start_time = start
        self.verbose = verbose
        self.end_time = end

    def format_report(self):
        pass

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
                          dest="end", help="report end date YYYY/MM/DD HH:mm:SS or YYYY-MM-SS HH:mm:SS")
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

    @staticmethod
    def runerror(error, traceback, admin_emails):
        TextUtils.sendEmail(([], admin_emails),
                            "ERROR PRODUCING REPORT: Production Jobs Success Rate on the OSG Sites: Date Generated {}".format(datetime.now()),
                            "ERROR: {}\n\nTRACEBACK: {}".format(error, traceback),
                            ("Gratia Operation", "sbhat@fnal.gov"),
                            "smtp.fnal.gov")
        return