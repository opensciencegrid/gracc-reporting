import abc
import argparse
from datetime import datetime
import sys
import smtplib
from email.mime.text import MIMEText
import logging
import operator
import os
import pkg_resources
import json
import toml
import copy

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
    :param str config: Filename of toml configuration file
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
    :param bool logfile_override: Override default logfile location
    :param bool check_vo: Should we do VO validation?
    :param str althost: Alternate Elasticsearch Host key from config file.
        Must be specified in [elasticsearch] section of
        config file by name (e.g. my_es_cluster="https://hostname.me")
    """
    __metaclass__ = abc.ABCMeta

    def __init__(self, report, config, start, end=None, verbose=False,
                 raw=False, allraw=False, template=None, is_test=False, no_email=False,
                 title=None, logfile=None, logfile_override=False, check_vo=False,
                 althost=None):
        TimeUtils.__init__(self)
        self.configfile = config
        self.config = self._parse_config(config)
        # if config:
        #     self.config = config.config
        self.header = []
        self.althost = althost
        self.start_time = self.parse_datetime(start)
        self.end_time = self.parse_datetime(end)
        self.verbose = verbose
        self.no_email = no_email
        self.is_test = is_test
        self.template = template
        self.epochrange = None
        self.indexpattern = self.indexpattern_generate(raw, allraw)
        self.report_type = report

        if logfile:
            self.logfile = self.get_logfile_path(logfile,
                                                 override=logfile_override)
        else:
            self.logfile = 'reports.log'

        if check_vo:
            self.__check_vo()

        self.email_info = self.__get_email_info()
        self.logger = self.__setupgenLogger()
        self.client = self.__establish_client()

    # Report methods that must or should be implemented
    @abc.abstractmethod
    def query(self):
        """Method to define report's Elasticsearch query. Must be overridden"""
        pass

    def run_query(self, overridequery=None):
        """Execute the query and check the status code before returning the
        relevant info

        :return Response.aggregations OR ES Search object: If the results are
        aggregated (response has aggregations property), returns aggregations
        property of elasticsearch response (most reports).  If not, return the
        search object itself, so it can be scanned using .scan() (JSR, for
        example)
        """

        s = overridequery() if overridequery is not None else self.query()

        t = s.to_dict()
        if self.verbose:
            print json.dumps(t, sort_keys=True, indent=4)
            self.logger.debug(json.dumps(t, sort_keys=True))
        else:
            self.logger.debug(json.dumps(t, sort_keys=True))

        try:
            response = s.execute()
            if not response.success():
                raise Exception("Error accessing Elasticsearch")

            if self.verbose:
                print json.dumps(response.to_dict(), sort_keys=True, indent=4)

            if hasattr(response, 'aggregations') and response.aggregations:
                results = response.aggregations
            else:
                results = s

            self.logger.info('Ran elasticsearch query successfully')
            return results
        except Exception as e:
            self.logger.exception(e)
            raise

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

        use_title = unicode(use_title, 'utf-8')

        if self.test_no_email(self.email_info['to']['email']):
            return

        emailReport = TextUtils.TextUtils(self.header)
        text["text"] = emailReport.printAsTextTable("text", content)
        text["csv"] = emailReport.printAsTextTable("csv", content)
        htmldata = emailReport.printAsTextTable("html", content,
                                                template=self.template)

        if self.header:
            htmlheader = unicode("\n".join(['<th>{0}</th>'.format(headerelt)
                                    for headerelt in self.header]), 'utf-8')

        if self.template:
            with open(self.template, 'r') as t:
                htmltext = unicode("".join(t.readlines()), 'utf-8')

            # Build the HTML file from the template
            htmldict = dict(title=use_title, header=htmlheader, table=htmldata)
            htmltext = htmltext.format(**htmldict)
            text["html"] = htmltext

        else:
            text["html"] = u"<html><body><h2>{0}</h2><table border=1>{1}</table></body></html>".format(use_title, htmldata)

        TextUtils.sendEmail((self.email_info['to']['name'],
                             self.email_info['to']['email']),
                            use_title, text,
                            (self.email_info['from']['name'],
                             self.email_info['from']['email']),
                            self.email_info['smtphost'],
                            html_template=self.template)
        self.logger.info("Sent reports to {0}".format(", ".join(self.email_info['to']['email'])))
        return

    @abc.abstractmethod
    def run_report(self):
        """Method within report that actually runs the various other methods
        in the Reporter and report-specific class.  Must be overridden."""
        pass

    # Helper methods that can be used in subclasses
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

    def indexpattern_generate(self, raw=False, allraw=False):
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

    @staticmethod
    def parse_opts():
        """Parses command line options

        :return: argparse.ArgumentParser object with parsed arguments for report
        """
        parser = argparse.ArgumentParser()
        parser.add_argument("-c", "--config", dest="config",
                            default=None, help="non-standard location of "
                                               "report configuration file")
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
        parser.add_argument("-L", "--logfile", dest="logfile",
                            default=None, help="Specify non-standard location"
                                               "for logfile")

        return parser

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
            logdir = self.config['default_logdir']
            if logdir in try_locations:
                try_locations.remove(logdir)
            try_locations.insert(0, logdir)
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
                    continue  # Don't try to write somewhere we can't

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

    # Non-public methods

    @staticmethod
    def _parse_config(configfile):
        """
        Parse our config file and return the config as dictionary

        :param configfile:  Path to TOML config file to be parsed
        :return: dict of config
        """
        if os.path.exists(configfile):
            try:
                with open(configfile, 'r') as f:
                    config = toml.loads(f.read())
            except toml.TomlDecodeError as e:
                print "Cannot decode toml file"
                print e
                raise
            return config
        else:
            raise OSError("Cannot find file {0:s}".format(configfile))

    def __check_vo(self):
        """
        Check to see if the vo is a section in config file (as of this writing,
        only applies to fife_reports package).  If not, raise KeyError
        :return None: 
        """
        # Put check in for SECTION

        if not self.vo or \
                (self.vo
                    and self.vo.lower() not in self.config['configured_vos']
                    and self.vo.lower() not in self.config[self.report_type.lower()]):
            if self.verbose:
                self.logger.info(self.configfile)
                self.logger.info(self.config)
            raise KeyError("The VO {0} was not found in the config file."
                            " Please review the config file to see if changes"
                            " need to be made and try again.  The config file"
                           " used was {1}".format(self.vo.lower(), self.configfile))
        return

    def __establish_client(self):
        """Initialize and return the elasticsearch client

        :return: elasticsearch.Elasticsearch object
        """
        if self.althost is None:
            hostname = self.config['elasticsearch'].get('hostname',
                                                        'https://gracc.opensciencegrid.org/q')
        else:
            hostname = self.config['elasticsearch'].get(self.althost,
                                                        'https://gracc.opensciencegrid.org/q')

        try:
            client = Elasticsearch(hostname,
                                   use_ssl=True,
                                   verify_certs=False,
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

        :return dict: Dict of sender, recipient(s), smtphost info.  Format is:
            { "to": {"email": ["email1", "email2", ], "name": ["name1", "name2", ]},
              "from": {"email": "email_address", "name": "named person"},
              "smtphost": "host.domain.com"
              }
        """
        email_info = {}
        config_email_info = copy.deepcopy(self.config['email'])

        # Get recipient(s) info
        emails = copy.deepcopy(config_email_info['test']['emails'])
        names = copy.deepcopy(config_email_info['test']['names'])
        if self.is_test:
            pass    # Do nothing.  We want to keep this as our final list
        else:
            attrs = [self.report_type.lower(), 'to_emails']
            try:
                vo = self.vo.lower()
                attrs.insert(1, vo)
                names = []
            except AttributeError:      # No vo-specific info in config file
                try:
                    add_names = copy.deepcopy(
                        self.config[self.report_type.lower()]['to_names']
                    )
                except KeyError:    # This is the project or missing project report
                    try:
                        attrs.insert(0, 'project')
                        add_names = copy.deepcopy(
                            self.config['project'][self.report_type.lower()]['to_names'])
                    except KeyError:    # Some case that shouldn't pop up.  Raise an error
                        raise
                finally:
                    names.extend(add_names)
            finally:
                # Iterate through config keys (attrs) to get emails we want
                add_emails = copy.deepcopy(self.config)
                while len(attrs) != 0:
                    add_emails = add_emails[attrs.pop(0)]
                emails.extend(add_emails)

        email_info["to"] = {}
        email_info["to"]["email"] = emails
        email_info["to"]["name"] = names

        # Get other global info from config file
        for key in ("from", "smtphost"):
            email_info[key] = copy.deepcopy(config_email_info[key])

        return email_info

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

        logger.addHandler(fh)

        # We only want one Stream Handler
        exists_ch = False
        for handler in logger.handlers:
            if handler.__class__.__name__ == "StreamHandler":
                exists_ch = True
                break
        if not exists_ch:
            logger.addHandler(ch)

        if self.is_test:
            logger.info("Running in test mode")

        return logger


def runerror(config, error, traceback, logfile):
    """
    Global function to print, log, and email errors to admins

    :param str config: Config filename
    :param str error: Error raised
    :param str traceback: Traceback from error
    :param str logfile: Filename of logfile
    :return None
    """
    try:
        with open(logfile, 'a') as f:
            f.write(str(error))
    except IOError: # Permission denied
        reallogfile = os.path.join(os.path.expanduser('~'), logfile)
        with open(reallogfile, 'a') as f:
            f.write(str(error))
    print >> sys.stderr, error

    with open(config, 'r') as f:
        c = toml.loads(f.read())
    admin_emails = c['email']['test']['emails']
    from_email = c['email']['from']['email']

    msg = MIMEText("ERROR: {0}\n\n{1}".format(error, traceback))
    msg['Subject'] = "ERROR PRODUCING REPORT: Date Generated {0}".format(
        datetime.now())
    msg['From'] = from_email
    msg['To'] = ', '.join(admin_emails)

    try:
        s = smtplib.SMTP(c['email']['smtphost'])
        s.sendmail(from_email, admin_emails, msg.as_string())
        s.quit()
        print "Successfully sent error email"
    except Exception as e:
        err = "Error:  unable to send email.\n%s\n" % e
        print err
        print error, traceback
        raise

    return None


def get_default_resource(kind, filename):
    """
    Returns the default config file or html template for a report

    :param str kind: Must be 'config', or 'html_templates', unless we expand
    the input file kinds in the future
    :param str filename: The filename of the resource file we're looking for
    :return str: Path of the default resource
    """
    default_path = os.path.join('/etc/gracc-reporting', kind)

    # If the file is in /etc/gracc-reporting/$kind, return that path
    if os.path.exists(default_path):
        print "Reading Resource from {0}".format(default_path)
        resfile = os.path.join(default_path, filename)
        if os.path.exists(resfile):
            return resfile
    # Otherwise, find the file (resource) in the package
    else:
        try:
            return pkg_resources.resource_filename('graccreports',
                                           os.path.join(kind, filename))
        except KeyError as e:    # No resource of that name
            print "The resource you're looking for, {0}, does not exist.  Either" \
                  " override the resource (use --help on your report to see the " \
                  "applicable option) or check how you implemented the resource" \
                  " call in your report.".format(filename)
            print "The error and traceback returned was: \n{0}".format(e)
            raise


def get_configfile(flag='osg', override=None):
    """
    Returns the appropriate config file for the gracc reports

    :param str flag:  In the future, I want this to be 'osg' or 'fife', or
    whatever other subpackages we have.  Right now, the FIFE reports all use
    different config files, so we have to use the separate flags for each
    ('osg', 'efficiency', 'jobrate').
    :param str override: Can be a config file in a non-standard location
    :return str: Absolute path to the config file
    """
    if override and os.path.exists(override):
        return override

    f = '{0}.toml'.format(flag)

    return get_default_resource('config', f)


def get_template(override=None, deffile=None):
    """
    Returns the appropriate HTML template for the gracc reports.  Allows for
    override of default template location, or search in default locations.

    :param str override: Can be a template in a non-standard location
    :param deffile: Default filename.  Should be passed in from calling
    report (something like 'template_flocking.html')
    :return str: Absolute path to template file
    """
    if override and os.path.exists(override):
        return override
    else:
        return get_default_resource('html_templates', deffile)


def coroutine(func):
    """Decorator to prime coroutines by advancing them to their first yield
    point

    :param function func: Coroutine function to prime
    :return function: Coroutine that's been primed
    """
    def wrapper(*args, **kwargs):
        cr = func(*args, **kwargs)
        cr.next()
        return cr
    return wrapper

def force_to_unicode(text):
    """If text is unicode, it is returned as is.
    If it's str, convert it to Unicode using UTF-8 encoding
    """
    return text if isinstance(text, unicode) else text.decode('utf8')