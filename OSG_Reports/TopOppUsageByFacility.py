#!/usr/bin/python

import sys
import os
import inspect
import traceback
import re
import json
import datetime
import copy
from elasticsearch_dsl import Search

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

import TextUtils
import TimeUtils
import Configuration
import NiceNum
from Reporter import Reporter, runerror

logfile = 'topoppusage.log'
MAXINT = 2**31-1
facilities = {}


# Helper functions
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


def get_time_range(start=None, end=None, months=None):
    pass

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

    """
    typedict = {'rg': basestring, 'res': basestring, 'entry': dict,
                'old_entry': dict}

    def __init__(self, name):
        self.name = name
        self.totalhours = 0
        self.oldtotalhours = 0
        for st in ('rg', 'res', 'entry', 'old_entry'):
            setattr(self, '{0}_list'.format(st), [])

    def add_hours(self, hours, old=False):
        """

        :param hours:
        :param old:
        :return:
        """
        if old:
            self.oldtotalhours += hours
        else:
            self.totalhours += hours

    def add_to_list(self, flag, item):
        """

        :param flag:
        :param item:
        :return:
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
    """
    def __init__(self, config, start=None, end=None, template=None,
                 is_test=False, no_email=False,
                 verbose=False, numrank=10, months=None):
        report = 'news'
        Reporter.__init__(self, report, config, start, end, verbose=verbose,
                          logfile=logfile, no_email=no_email, is_test=is_test,
                          raw=False)
        self.numrank = numrank
        self.template = template
        self.text = ''
        self.table = ''
        self.title = "Opportunistic Resources provided by the top {0} OSG " \
                     "Sites for the OSG Open Facility ({1} - {2})".format(
                        self.numrank, self.start_time, self.end_time)
        self.probelist = self._get_probelist()

    def _get_probelist(self):
        """

        :return:
        """
        probes = self.config.get('query', 'OSG_flocking_probe_list')
        return [elt.strip("'") for elt in re.split(',', probes)]

    def run_report(self):
        """Handles the data flow throughout the report generation.  Generates
        the raw data, the HTML report, and sends the email.

        :return None
        """
        self.generate()
        # self.generate_report_file()
        # self.send_report()
        return

    def query(self):
        """
        Method to query Elasticsearch cluster for EfficiencyReport information

        :return elasticsearch_dsl.Search: Search object containing ES query
        """
        # Gather parameters, format them for the query
        starttimeq = self.dateparse_to_iso(self.start_time)
        endtimeq = self.dateparse_to_iso(self.end_time)

        if self.verbose:
            self.logger.info(self.indexpattern)

        # Elasticsearch query and aggregations
        s = Search(using=self.client, index=self.indexpattern) \
                .filter("range", EndTime={"gte": starttimeq, "lt": endtimeq}) \
                .filter("term", ResourceType="Payload") \
                .filter("terms", ProbeName=self.probelist)[0:0]

        # Size 0 to return only aggregations

        self.unique_terms = ['OIM_Facility', 'OIM_ResourceGroup',
                             'OIM_Resource']
        cur_bucket = s.aggs

        for term in self.unique_terms:
            cur_bucket = cur_bucket.bucket(term, 'terms', field=term,
                                           size=MAXINT, missing='Unknown')

        cur_bucket.metric('CoreHours', 'sum', field='CoreHours')

        s.aggs.bucket('Missing', 'missing', field='OIM_Facility')\
            .bucket('Host_description', 'terms', field='Host_description',
                    size=MAXINT)\
            .metric('CoreHours', 'sum', field='CoreHours')

        return s

    def run_query(self):
        """Execute the query and check the status code before returning the
        response

        :return Response.aggregations: Returns aggregations property of
        elasticsearch response
        """
        s = self.query()
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

            results = response.aggregations
            self.logger.info('Ran elasticsearch query successfully')
            return results
        except Exception as e:
            self.logger.exception(e)
            raise

    def generate(self):
        """
        Runs the ES query, checks for success, and then
        sends the raw data to parser for processing.

        :return: None
        """
        self.current = True
        results = self.run_query()
        f_parser = self._parse_to_facilities()
        # print results

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
            print elt
            # Do Name Correction, add an entry to data


        for entry in data:
            print entry
            f_parser.send(entry)
            # yield [entry[field] for field in allterms]

        for f in facilities.itervalues():
            print f.name, f.totalhours, f.entry_list

        return

    @coroutine
    def _parse_to_facilities(self):
        """

        :return:
        """
        # termsmap = [('OIM_ResourceGroup', 'rg'), ('OIM_Resource', 'res')]

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
        # header = ['Experiment', 'Facility', 'User', 'Used Wall Hours',
        #           'Efficiency']
        # htmlheader = '<th>' + '</th><th>'.join(header) + '</th>'
        # htmldict = dict(title=self.title, header=htmlheader, table=self.table)
        # self.text = "".join(open(self.template).readlines())
        # self.text = self.text.format(**htmldict)
        return

    def send_report(self):
        """
        Sends the HTML report file in an email (or doesn't if self.no_email
        is set to True)

        :return: None
        """
        if self.test_no_email(self.email_info["to_emails"]):
            return

        TextUtils.sendEmail(
                            (self.email_info["to_names"],
                             self.email_info["to_emails"]),
                            self.title,
                            {"html": self.text},
                            (self.email_info["from_name"],
                             self.email_info["from_email"]),
                            self.email_info["smtphost"])

        self.logger.info("Report sent for {0}".format(self.vo))

        return


if __name__ == "__main__":
    args = parse_opts()

    # Set up the configuration
    config = Configuration.Configuration()
    config.configure(args.config)

    try:

        # Create a report object, create a report for the VO, and send it
        r = TopOppUsageByFacility(config,
                                  start=args.start,
                                  end=args.end,
                                  template=args.template,
                                  months=args.months,
                                  is_test=args.is_test,
                                  no_email=args.no_email,
                                  verbose=args.verbose,
                                  numrank=args.numrank)
        r.run_report()
        print "Top Opportunistic Usage per Facility Report execution successful"

    except Exception as e:
        errstring = '{0}: Error running Top Opportunistic Usage Report. ' \
                    '{1}'.format(datetime.datetime.now(), traceback.format_exc())
        with open(logfile, 'a') as f:
            f.write(errstring)
        print >> sys.stderr, errstring
        runerror(config, e, errstring)
        sys.exit(1)
    sys.exit(0)
