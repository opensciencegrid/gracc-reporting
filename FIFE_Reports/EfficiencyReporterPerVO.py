#!/usr/bin/python

import sys
import os
import inspect
import traceback
import re
import json
from elasticsearch_dsl import Q, Search

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

cilogon_match = re.compile('.+CN=UID:(\w+)')
non_cilogon_match = re.compile('/CN=([\w\s]+)/?.+?')
logfile = 'efficiencyreport.log'


class User(object):
    def __init__(self, info):
        """Take CSV as described below and assigns to it the attributes vo, facility, email, user, hours, eff
        CSV format DARKSIDE, Fermigrid,
        /CN = fifegrid/CN = batch/CN = Shawn S. Westerdale/CN = UID:shawest,
        13411.2019444, 0.969314375191
        New CSV format 'uboone', 'GPGrid', '/CN=fifegrid/CN=batch/CN=Elena Gramellini/CN=UID:elenag', '1337.86666667', '0.857747616437'
        """
        tmp = info.split(',')
        self.vo = tmp[0].lower()
        self.facility = tmp[1]
        self.email, self.user = self.parseCN(tmp[2])
        self.hours = int(float(tmp[3]))
        self.eff = round(float(tmp[4]), 2)

    def parseCN(self, cn):
        """Parse the CN to grab the email address and user"""
        m = cilogon_match.match(cn)      # CILogon certs
        if m:
            email = '{0}@fnal.gov'.format(m.group(1))
        else:
            email = ""
            # Non-CILogon Certs (note - this matches what we did before, but
            # we might need to change it in the future
            m = non_cilogon_match.match(cn)
        user = m.group(1)
        return email, user

    def dump(self):
        print "{0:>10}, {1:>20}, {2:>20}, {3}, {4}".format(
            self.vo,
            self.facility,
            self.user,
            int(self.hours),
            round(self.eff, 2))


class Efficiency(Reporter):
    def __init__(self, config, start, end, vo, verbose, hour_limit, eff_limit,
                 is_test, no_email):
        Reporter.__init__(self, config, start, end, verbose = False)
        self.no_email = no_email
        self.hour_limit = hour_limit
        self.logfile = logfile
        self.logger = self.setupgenLogger('efficiencypervo')
        self.vo = vo
        self.eff_limit = eff_limit
        self.is_test = is_test
        self.verbose = verbose
        self.text = ''
        self.fn = "{0}-efficiency.{1}".format(self.vo.lower(),
                                         self.start_time.replace("/", "-"))

    def query(self, client):
        """Method to query Elasticsearch cluster for EfficiencyReport
        information"""
        # Gather parameters, format them for the query
        starttimeq = self.dateparse_to_iso(self.start_time)
        endtimeq = self.dateparse_to_iso(self.end_time)
        wildcardVOq = '*' + self.vo.lower() + '*'
        wildcardProbeNameq = 'condor:fifebatch?.fnal.gov'

        # Elasticsearch query and aggregations
        s = Search(using = client, index = self.indexpattern)\
                   .query("wildcard", VOName=wildcardVOq)\
                   .query("wildcard", ProbeName=wildcardProbeNameq)\
                   .filter("range", EndTime={"gte" : starttimeq, "lt" : endtimeq})\
                   .filter(Q({"range" : {"WallDuration": {"gt": 0}}}))\
                   .filter(Q({"term": {"Host_description" : "GPGrid"}}))\
                   .filter(Q({"term" : {"ResourceType" : "Payload"}}))[0:0]
                    # Size 0 to return only aggregations

        Bucket = s.aggs.bucket('group_VOname', 'terms', field='ReportableVOName')\
                .bucket('group_HostDescription', 'terms', field='Host_description')\
                .bucket('group_commonName', 'terms', field='CommonName')

        Metric = Bucket.metric('Process_times_WallDur', 'sum',
                               script="(doc['WallDuration'].value*doc['Processors'].value)")\
                .metric('WallHours', 'sum',
                        script="(doc['WallDuration'].value*doc['Processors'].value)/3600")\
                .metric('CPUDuration', 'sum', field='CpuDuration')

        return s

    def query_to_csv(self):
        """Returns a csv file with aggregated data from query to
        Elasticsearch"""
        outfile = 'efficiency.csv'

        # Initialize the elasticsearch client
        client = self.establish_client()
        s = self.query(client)

        if self.verbose:
            t = s.to_dict()
            self.logger.info(json.dumps(t, sort_keys=True, indent=4))

        response = s.execute()
        resultset = response.aggregations

        if not response.success():
            self.logger.exception('Error accessing ElasticSearch')
            raise
        else:
            self.logger.info('Ran elasticsearch query successfully')

        if self.verbose:
            print json.dumps(response.to_dict(), sort_keys=True, indent=4)


        # Header for file
        header = '{0}\t{1}\t{2}\t{3}\t{4}\n'.format('VO',
                                             'Host Description',
                                             'Common Name',
                                             'Wall Hours',
                                             'Efficiency')

        # Write everything to the outfile
        with open(outfile, 'w') as f:
            f.write(header)
            for per_vo in resultset.group_VOname.buckets:
                for per_hostdesc in per_vo.group_HostDescription.buckets:
                    for per_CN in per_hostdesc.group_commonName.buckets:
                        outstring = '{0},{1},{2},{3},{4}\n'.format(self.vo,
                                                                  per_hostdesc.key,
                                                                  per_CN.key,
                                                                  per_CN.WallHours.value,
                                                                  (per_CN.CPUDuration.value / 3600) / per_CN.WallHours.value)
                        f.write(outstring)

        return outfile

    def reportVO(self, users, facility):
        """Method to generate report for VO from users dictionary"""
        if self.vo == "FIFE":
            records = [rec for rec in users.values()]
        else:
            records = users[self.vo.lower()]
        info = [rec for rec in records
                if ((rec.hours > self.hour_limit and rec.eff < self.eff_limit)
                    and (facility == "all" or rec.facility == facility))
                ]
        return sorted(info, key=lambda user: user.eff)

    def generate_report_file(self, report):
        if len(report) == 0:
            self.no_email = True
            self.logger.info("Report empty")
            return

        epoch_stamps = self.get_epoch_stamps_for_grafana()
        elist = [elt for elt in epoch_stamps]

        table = ""
        for u in report:
            elist_vo = [elt for elt in elist]
            elist_vo.append(u.vo.lower())
            vo_link = 'https://fifemon.fnal.gov/monitor/dashboard/db/' \
                      'experiment-efficiency-details?' \
                      'from={0}&to={1}' \
                      '&var-experiment={2}'.format(*elist_vo)

            vo_html = '<a href="{0}">{1}</a>'.format(vo_link, self.vo)

            elist.append(u.user)
            user_link = "https://fifemon.fnal.gov/monitor/dashboard/db/" \
                        "user-efficiency-details?" \
                        "from={0}&to={1}" \
                        "&var-user={2}".format(*elist)

            user_html = '<a href="{0}">{1}</a>'.format(user_link, u.user)

            table += '<tr><td align="left">{0}</td>' \
                     '<td align="left">{1}</td>'.format(vo_html,
                                                        u.facility) \
                     + '<td align="left">{0}</td>' \
                       '<td align="right">{1}</td>' \
                       '<td align="right">{2}</td></tr>'.format(
                user_html,
                NiceNum.niceNum(u.hours),
                u.eff)
            elist.pop()

        self.text = "".join(open("template_efficiency.html").readlines())
        self.text = self.text.replace("$START", self.start_time)
        self.text = self.text.replace("$END", self.end_time)
        self.text = self.text.replace("$TABLE", table)
        self.text = self.text.replace("$VO", self.vo)

        if self.verbose:
            with open(self.fn, 'w') as f:
                f.write(self.text)

        return

    def send_report(self):
        """Generate HTML from report and send the email"""
        if self.no_email:
            self.logger.info("Not sending report")
            return

        if self.is_test:
            emails = re.split('[; ,]', self.config.get("email", "test_to"))
        else:
            emails = re.split('[; ,]', self.config.get(self.vo.lower(), "email")) + \
                     re.split('[; ,]', self.config.get("email", "test_to"))
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

        if self.verbose:
            os.remove(self.fn)
            self.logger.info("Report sent")

        return


if __name__ == "__main__":
    opts, args = Reporter.parse_opts()
    try:
        # Set up the configuration
        config = Configuration.Configuration()
        config.configure(opts.config)
        # Grab VO
        vo = opts.vo
        # Grab the limits
        eff = config.config.get(opts.vo.lower(), "efficiency")
        min_hours = config.config.get(opts.vo.lower(), "min_hours")

        # Create an Efficiency object, create a report for the VO, and send it
        e = Efficiency(config,
                       opts.start,
                       opts.end,
                       vo,
                       opts.verbose,
                       int(min_hours),
                       float(eff),
                       opts.is_test,
                       opts.no_email)
        # Run our elasticsearch query, get results as CSV
        resultfile = e.query_to_csv()

        # For each line returned, create a User object, and add the User and
        # their vo to the users dict
        with open(resultfile, 'r') as file:
            f = file.readlines()
        users = {}
        for line in f[1:]:
            u = User(line)
            if u.vo not in users:
                users[u.vo] = []
            users[u.vo].append(u)

        # Generate the VO report, send it
        if vo == "FIFE" or vo.lower() in users:
            r = e.reportVO(users, opts.facility)
            e.generate_report_file(r)
            e.send_report()
    except Exception as e:
        with open(logfile, 'a') as f:
            f.write(traceback.format_exc())
        print >> sys.stderr, traceback.format_exc()
        runerror(config, e, traceback.format_exc())
        sys.exit(1)
    sys.exit(0)
