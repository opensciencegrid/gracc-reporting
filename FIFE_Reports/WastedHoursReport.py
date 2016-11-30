#!/usr/bin/python

import sys
import os
import inspect
import traceback
import json
import logging
from elasticsearch_dsl import A, Search

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

import NiceNum
import Configuration
from Reporter import Reporter, runerror
import TextUtils


class User:
    def __init__(self, user_name):
        self.user = user_name
        self.success = [0, 0]
        self.failure = [0, 0]

    def add_success(self, njobs, wall_duration):
        """

        :param njobs:
        :param wall_duration:
        :return:
        """
        self.success = [ex + new for ex, new in
                        zip(self.success, [njobs, wall_duration])]

    def add_failure(self, njobs, wall_duration):
        """

        :param njobs:
        :param wall_duration:
        :return:
        """
        self.failure = [ex + new for ex, new in
                        zip(self.failure, [njobs, wall_duration])]

    def get_failure_rate(self):
        """

        :return:
        """
        failure_rate = 0
        if self.success[0] + self.failure[0] > 0:
            failure_rate = self.failure[0] * 100. /\
                           (self.success[0] + self.failure[0])
        return failure_rate

    def get_waste_per(self):
        """

        :return:
        """
        waste_per = 0
        if self.success[1] + self.failure[1] > 0:
            waste_per = self.failure[1] * 100. /\
                        (self.success[1] + self.failure[1])
        return waste_per


class Experiment:
    def __init__(self, exp_name):
        """

        :param exp_name:
        :return:
        """
        self.experiment = exp_name
        self.success = [0, 0]
        self.failure = [0, 0]
        self.users = {}

    def add_user(self, user_name, user):
        self.users[user_name] = user

    def add_success(self, njobs, wall_duration):
        """

        :param njobs:
        :param wall_duration:
        :return:
        """
        self.success = [ex + new for ex, new in
                        zip(self.success, [njobs, wall_duration])]

    def add_failure(self, njobs, wall_duration):
        """

        :param njobs:
        :param wall_duration:
        :return:
        """
        self.failure = [ex + new for ex, new in
                        zip(self.failure, [njobs, wall_duration])]

    def get_failure_rate(self):
        """

        :return:
        """
        failure_rate = 0
        if self.success[0] + self.failure[0] > 0:
            failure_rate = self.failure[0] * 100. /\
                           (self.success[0] + self.failure[0])
        return failure_rate

    def get_waste_per(self):
        """

        :return:
        """
        waste_per = 0
        if self.success[1] + self.failure[1] > 0:
            waste_per = self.failure[1] * 100. /\
                        (self.success[1] + self.failure[1])
        return waste_per


class WastedHoursReport(Reporter):
    """

    """
    def __init__(self, config_file, start, end, is_test=True, verbose=False):
        """
        :param config_file:
        :param start:
        :param end:
        :param is_test:
        :param verbose:
        :return:
        """
        Reporter.__init__(self, config_file, start, end, verbose, raw=False)
        self.is_test = is_test
        self.experiments = {}
        self.connect_str = None
        self.text = ''
        self.fn = "user_wasted_hours_report.{0}".format(
            self.end_time.split(" ")[0].replace("/", "-"))

    def query(self, client):
        """Query method to grab wasted hours info, return query object"""

        wildcardProbeNameq = 'condor:fifebatch?.fnal.gov'

        starttimeq = self.dateparse_to_iso(self.start_time)
        endtimeq = self.dateparse_to_iso(self.end_time)

        s = Search(using=client, index=self.indexpattern) \
            .query("wildcard", ProbeName=wildcardProbeNameq) \
            .filter("range", EndTime={"gt": starttimeq, "lt": endtimeq})

        # Aggregations
        a1 = A('filters', filters={
            'Success': {'bool': {'must': {'term': {'Resource_ExitCode': 0}}}},
            'Failure': {
                'bool': {'must_not': {'term': {'Resource_ExitCode': 0}}}}})
        a2 = A('terms', field='VOName', size=1000000000) # large size to
                                                        # bring in all records
        a3 = A('terms', field='CommonName', size=1000000000)

        Buckets = s.aggs.bucket('group_status', a1)\
            .bucket('group_VO', a2) \
            .bucket('group_CommonName', a3)

        # Metrics
        Metric = Buckets.metric('numJobs', 'sum', field='Count')\
            .metric('WallHours', 'sum',
                    script="(doc['WallDuration'].value*doc['Processors'].value"
                           "/3600)")

        if self.verbose:
            print s.to_dict()

        return s

    def generate(self):
        """

        :return:
        """
        client = self.establish_client()
        resultquery = self.query(client)

        response = resultquery.execute()
        return_code_success = response.success()
        if not return_code_success:
            raise Exception('Error querying elasticsearch')

        results = response.aggregations

        if self.verbose:
            print json.dumps(response.to_dict(), sort_keys=True, indent=4)

        table_results = []
        for status in results.group_status.buckets:
            for VO in results.group_status.buckets[status].group_VO.buckets:
                for CommonName in VO['group_CommonName'].buckets:
                    table_results.append([CommonName.key, VO.key, status,
                                          CommonName['numJobs'].value,
                                          CommonName['WallHours'].value])

        if len(table_results) == 1 and len(table_results[0].strip()) == 0:
            print >> sys.stdout, "Nothing to report"
            return

        for line in table_results:
            if self.verbose:
                print line
            name = line[0]
            expname = line[1]
            status = line[2]
            count = int(line[3])
            hours = float(line[4])
            if expname not in self.experiments:
                exp = Experiment(expname)
                self.experiments[expname] = exp
            else:
                exp = self.experiments[expname]
            if name not in exp.users:
                user = User(name)
                exp.add_user(name, user)
            else:
                user = exp.users[name]
            if status == 'Success':
                user.add_success(count, hours)
            else:
                user.add_failure(count, hours)

        return

    def generate_report_file(self):
        if len(self.experiments) == 0:
            print "No experiments"
            return
        total_hrs = 0
        total_jobs = 0
        table = ""
        for key, exp in self.experiments.items():
            for uname, user in exp.users.items():
                failure_rate = round(user.get_failure_rate(), 1)
                waste_per = round(user.get_waste_per(), 2)
                table += '\n<tr><td align="left">{0:s}</td>' \
                         '<td align="left">{1:s}</td>' \
                         '<td align="right">{2:s}</td>' \
                         '<td align="right">{3:s}</td>' \
                         '<td align="right">{4:.1f}</td>' \
                         '<td align="right">{5:s}</td>' \
                         '<td align="right">{6:s}</td>' \
                         '<td align="right">{7:.1f}</td></tr>'.format(
                    key,
                    uname,
                    NiceNum.niceNum(user.success[0] + user.failure[0]),
                    NiceNum.niceNum(user.failure[0]),
                    failure_rate,
                    NiceNum.niceNum(user.success[1] + user.failure[1],1),
                    NiceNum.niceNum(user.failure[1], 1),
                    waste_per
                )
                if self.verbose:
                    total_hrs += (user.success[1] + user.failure[1])
                    total_jobs += (user.success[0] + user.failure[0])
        self.text = "".join(open("template_wasted_hours.html").readlines())
        self.text = self.text.replace("$START", self.start_time)
        self.text = self.text.replace("$END", self.end_time)
        self.text = self.text.replace("$TABLE", table)
        print "Writing file"
        f = open(self.fn, "w")
        f.write(self.text)
        f.close()
        if self.verbose:
            print total_jobs, total_hrs
        return

    def send_report(self):
        emails = ""
        if self.is_test:
            emails = self.config.get("email", "test_to").split(", ")
        else:
            pass
            emails = self.config.get("email", "{0}_email".format(self.vo.lower())).split(",")\
                     + self.config.get("email", "test_to").split(",")

        TextUtils.sendEmail(([], emails),
                            "{0:s} Wasted Hours on the GPGrid ({1:s} - {2:s})"\
                            .format("FIFE", self.start_time, self.end_time),
                            {"html": self.text},
                            ("GRACC Operations", "sbhat@fnal.gov"),
                            "smtp.fnal.gov")
        os.unlink(self.fn)


if __name__ == "__main__":
    args = Reporter.parse_opts()

    config = Configuration.Configuration()
    config.configure(args.config)

    try:
        report = WastedHoursReport(config, args.start, args.end, args.is_test, args.verbose)
        report.generate()
        report.generate_report_file()
        report.send_report()
    except Exception as e:
        print >> sys.stderr, traceback.format_exc()
        runerror(config, e, traceback.format_exc())
        sys.exit(1)
    sys.exit(0)
