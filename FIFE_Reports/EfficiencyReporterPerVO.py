#!/usr/bin/python

import sys
import os
import inspect
import traceback
import re
import json
import datetime
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
import Configuration
import NiceNum
from Reporter import Reporter, runerror

logfile = 'efficiencyreport.log'


class Efficiency(Reporter):
    def __init__(self, config, start, end, vo, verbose, hour_limit, eff_limit,
                 facility, is_test=False, no_email=False):
        Reporter.__init__(self, config, start, end, verbose=False)
        self.no_email = no_email
        self.hour_limit = hour_limit
        self.vo = vo
        self.verbose = verbose
        self.logfile = logfile
        self.logger = self.setupgenLogger('efficiencypervo')
        self.eff_limit = eff_limit
        self.facility = facility
        self.is_test = is_test
        self.text = ''
        self.table = ''
        self.fn = "{0}-efficiency.{1}".format(self.vo.lower(),
                                         self.start_time.replace("/", "-"))
        self.cilogon_match = re.compile('.+CN=UID:(\w+)')
        self.non_cilogon_match = re.compile('/CN=([\w\s]+)/?.+?')

    @staticmethod
    def calc_eff(wallhours, cpusec):
        """Calculate the efficiency given the wall hours and cputime in seconds.  Returns percentage"""
        return (cpusec / 3600) / wallhours

    def parseCN(self, cn):
        """Parse the CN to grab the username"""
        m = self.cilogon_match.match(cn)  # CILogon certs
        if m:
            pass
        else:
            m = self.non_cilogon_match.match(cn)
        user = m.group(1)
        return user

    def query(self):
        """Method to query Elasticsearch cluster for EfficiencyReport information"""
        # Gather parameters, format them for the query
        starttimeq = self.dateparse_to_iso(self.start_time)
        endtimeq = self.dateparse_to_iso(self.end_time)
        wildcardVOq = '*' + self.vo.lower() + '*'
        wildcardProbeNameq = 'condor:fifebatch?.fnal.gov'

        # Elasticsearch query and aggregations
        s = Search(using=self.establish_client(), index=self.indexpattern) \
            .query("wildcard", VOName=wildcardVOq) \
            .query("wildcard", ProbeName=wildcardProbeNameq) \
            .filter("range", EndTime={"gte": starttimeq, "lt": endtimeq}) \
            .filter("range", WallDuration={"gt": 0}) \
            .filter("term", Host_description="GPGrid") \
            .filter("term", ResourceType="Payload")[0:0]
        # Size 0 to return only aggregations

        # Bucket aggs
        Bucket = s.aggs.bucket('group_VOName', 'terms', field='ReportableVOName') \
            .bucket('group_HostDescription', 'terms', field='Host_description') \
            .bucket('group_CommonName', 'terms', field='CommonName')

        # Metric aggs
        Bucket.metric('WallHours', 'sum',
                      script="(doc['WallDuration'].value*doc['Processors'].value)/3600") \
            .metric('CPUDuration_sec', 'sum', field='CpuDuration')

        return s

    def run_query(self):
        """Execute the query and check the status code before returning the response"""
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
                raise

            if self.verbose:
                print json.dumps(response.to_dict(), sort_keys=True, indent=4)

            results = response.aggregations
            self.logger.info('Ran elasticsearch query successfully')
            return results
        except Exception as e:
            self.logger.exception("Error accessing Elasticsearch")
            sys.exit(1)

    def parse_lines(self):
        """For each set of dn, wall hours, cpu time, this gets username, calculates efficiency, and sends to
        HTML formatter"""
        html_formatter = self.generate_report_lines()
        html_formatter.send(None)
        while True:
            dn, wallhrs, cputime = yield
            user = self.parseCN(dn)
            eff = self.calc_eff(wallhrs, cputime)
            if eff < self.eff_limit:
                if self.verbose:
                    print "{0}\t{1}\t{2}%".format(user, wallhrs, round(eff*100, 1))
                html_formatter.send((user, wallhrs, eff))

    def generate_report_lines(self):
        """This generates an HTML line from the raw data line and sends it to the tablebuilder"""
        tablebuilder = self.generate_data_table()
        tablebuilder.send(None)

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

    def generate_data_table(self):
        """This compiles the data table lines and creates the table text (HTML) string"""
        self.table = ""
        while True:
            newline = yield
            self.table += newline

    def generate_report_file(self):
        """Takes the HTML template and inserts the appropriate information to generate the final report file"""
        self.text = "".join(open("template_efficiency.html").readlines())
        self.text = self.text.replace("$START", self.start_time)
        self.text = self.text.replace("$END", self.end_time)
        self.text = self.text.replace("$VO", self.vo)
        self.text = self.text.replace("$TABLE", self.table)
        return

    def send_report(self):
        """Sends the HTML report file in an email (or doesn't if self.no_email is set to True)"""
        if self.is_test:
            emails = re.split('[; ,]', self.config.get("email", "test_to"))
        else:
            emails = re.split('[; ,]', self.config.get(self.vo.lower(), "email") +
                              ',' + self.config.get("email", "test_to"))

        if self.no_email:
            self.logger.info("Not sending report")
            self.logger.info("Would have sent emails to {0}.".format(
                ', '.join(emails)))
            return

        TextUtils.sendEmail(
                            ([], emails),
                            "{0} Jobs with Low Efficiency ({1}) "
                            "on the  OSG Sites ({2} - {3})".format(
                                self.vo,
                                self.eff_limit,
                                self.start_time,
                                self.end_time),
                            {"html": self.text},
                            ("GRACC Operations", "sbhat@fnal.gov"),
                            "smtp.fnal.gov")

        self.logger.info("Report sent for {0}".format(self.vo))

        return

    def run_report(self):
        """Handles the data flow throughout the report generation.  Runs the query, generates the HTML report,
        and sends the email"""
        results = self.run_query()
        pline = self.parse_lines()
        pline.send(None)

        vos = (vo for vo in results.group_VOName.buckets)
        hostdesc = (hd for vo in vos for hd in vo.group_HostDescription.buckets)
        cns = (cn for hd in hostdesc for cn in hd.group_CommonName.buckets)

        for cn in cns:
            if cn.WallHours.value > self.hour_limit:
                pline.send((cn.key, cn.WallHours.value, cn.CPUDuration_sec.value))

        if not self.table:
            self.no_email = True
            self.logger.warn("Report empty for {0}".format(self.vo))
            return

        self.generate_report_file()
        self.send_report()
        return


if __name__ == "__main__":
    args = Reporter.parse_opts()

    # Set up the configuration
    config = Configuration.Configuration()
    config.configure(args.config)

    try:
        # Grab VO
        vo = args.vo
        # Grab the limits
        repeff = config.config.get(args.vo.lower(), "efficiency")
        min_hours = config.config.get(args.vo.lower(), "min_hours")

        # Create an Efficiency object, create a report for the VO, and send it
        e = Efficiency(config,
                       args.start,
                       args.end,
                       vo,
                       args.verbose,
                       int(min_hours),
                       float(repeff),
                       args.facility,
                       args.is_test,
                       args.no_email)
        e.run_report()

        print "Efficiency Report execution successful"

    except Exception as e:
        errstring = '{0}: Error running Efficiency Report for {1}. ' \
                    '{2}'.format(datetime.datetime.now(), args.vo, traceback.format_exc())
        with open(logfile, 'a') as f:
            f.write(errstring)
        print >> sys.stderr, errstring
        runerror(config, e, errstring)
        sys.exit(1)
    sys.exit(0)
