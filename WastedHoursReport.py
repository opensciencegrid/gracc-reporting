import sys
import optparse
import traceback
import json
import certifi
import logging
import re
from datetime import datetime, date
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Q, A, Search

import NiceNum
import Configuration
import TextUtils
# from MySQLUtils import MySQLUtils
from Reporter import Reporter
# from indexpattern import indexpattern_generate

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
#        self.success = [int(njobs), float(wall_duration)]
        self.success = [ex + new for ex, new in zip(self.success, [njobs, wall_duration])]


    def add_failure(self, njobs, wall_duration):
        """

        :param njobs:
        :param wall_duration:
        :return:
        """
#        self.failure = [int(njobs), float(wall_duration)]
        self.failure = [ex + new for ex, new in zip(self.failure, [njobs, wall_duration])]


    def get_failure_rate(self):
        """

        :return:
        """
        failure_rate = 0
        if self.success[0] + self.failure[0] > 0:
            failure_rate = self.failure[0] * 100. / (self.success[0] + self.failure[0])
        return failure_rate

    def get_waste_per(self):
        """

        :return:
        """
        waste_per = 0
        if self.success[1] + self.failure[1] > 0:
            waste_per = self.failure[1] * 100. / (self.success[1] + self.failure[1])
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
        self.success = [ex + new for ex, new in zip(self.success, [njobs, wall_duration])]
        # self.success[0] += int(njobs)
        # self.success[1] += float(wall_duration)

    def add_failure(self, njobs, wall_duration):
        """

        :param njobs:
        :param wall_duration:
        :return:
        """
        # self.failure = [int(njobs), float(wall_duration)]
        self.failure = [ex + new for ex, new in zip(self.failure, [njobs, wall_duration])]


    def get_failure_rate(self):
        """

        :return:
        """
        failure_rate = 0
        if self.success[0] + self.failure[0] > 0:
            failure_rate = self.failure[0] * 100. / (self.success[0] + self.failure[0])
        return failure_rate

    def get_waste_per(self):
        """

        :return:
        """
        waste_per = 0
        if self.success[1] + self.failure[1] > 0:
            waste_per = self.failure[1] * 100. / (self.success[1] + self.failure[1])
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

    def query_test(self, client):
        """Query method to grab wasted hours, return query object"""

        wildcardProbeNameq = 'condor:fifebatch?.fnal.gov'

        starttimeq = self.dateparse_to_iso(self.start_time)
        endtimeq = self.dateparse_to_iso(self.end_time)

        s = Search(using=client, index=self.indexpattern)\
                   .query("wildcard", ProbeName=wildcardProbeNameq)\
               .filter("range", EndTime={"gte" : starttimeq, "lt" : endtimeq})
        #
        # Aggregations
        a1 = A('filters', filters = {'Success' : {'bool' : {'must' : {'term' : {'Resource_ExitCode' : 0}}}},
             'Failure': {'bool' : {'must_not' : {'term' : {'Resource_ExitCode' : 0}}}}})
        a2 = A('terms', field = 'VOName', size=1000000)
        a3 = A('terms', field = 'CommonName', size=1000000)
        # #
        Buckets = s.aggs.bucket('group_status', a1) \
            .bucket('group_VO', a2)\
           .bucket('group_CommonName', a3)

        # Buckets = s.aggs.bucket('blah', a3)

        # #
        # # Metrics
        # # FIGURE OUT HOW TO TOTAL JOBS
        # Metric = Buckets.metric('numJobs', 'sum', field = 'Count')\
        #      .metric('WallHours', 'sum', script="(doc['WallDuration'].value*doc['Processors'].value/3600)")

        if self.verbose:
            print s.to_dict()

        return s

    def query(self, client):
        """Query method to grab wasted hours, return query object"""

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
        a2 = A('terms', field='VOName', size=100000000)
        a3 = A('terms', field='CommonName', size=100000000)

        Buckets = s.aggs.bucket('group_status', a1) \
            .bucket('group_VO', a2) \
            .bucket('group_CommonName', a3)

        # Metrics
        # FIGURE OUT HOW TO TOTAL JOBS
        Metric = Buckets.metric('numJobs', 'sum', field='Count') \
            .metric('WallHours', 'sum',
                    script="(doc['WallDuration'].value*doc['Processors'].value/3600)")

        if self.verbose:
            print s.to_dict()

        return s

    def generate_test(self):
        """

        :return:
        """
#        mysql_client_cfg = MySQLUtils.createClientConfig("main_db", self.config)
#        self.connect_str = MySQLUtils.getDbConnection("main_db", mysql_client_cfg, self.config)
#        select = "select VO.VOName,CommonName,if (ApplicationExitCode=0,'Success','Failure') as Status,sum(NJobs) as NJobs," + \
#               "round(sum(WallDuration*Cores/3600.),1) as WallHours from MasterSummaryData m " + \
#               "JOIN VONameCorrection VC ON (VC.corrid=m.VOcorrid) JOIN VO on (VC.void = VO.void) " + \
#               "where EndTime>'"+self.start_time + "' AND EndTime < '"+self.end_time + \
#               "' and probename like 'condor:fifebatch%.fnal.gov' group by status,VOName,CommonName order by VO.VOName,CommonName,status;"

        client = self.establish_client()
        resultquery = self.query_test(client)

        response = resultquery.execute()
        return_code_success = response.success()

        # for hit in resultquery.scan():
        #     if hit['VOName'] == 'cdf' and 'dbox' in hit['DN']:
        #         print hit['DN'], hit['Resource_ExitCode'], hit['VOName'], hit['Count']

        results = response.aggregations
        # print results
        # for item in results.group_status.buckets:
        #     print item

        for status in results.group_status.buckets:
           print status, results.group_status.buckets[status]
           for VO in results.group_status.buckets[status].group_VO.buckets:
                print VO.key, status, VO
                if status == 'Success' and VO.key == 'cdf':
                   for item in VO['group_CommonName'].buckets:
                       print item
                       if VO['key'] == 'cdf':
                           print VO['group_CommonName']
                           for CN in VO['group_CommonName'].buckets:
                               if 'dbox' in CN.key:
                                   print CN.key, CN, status


                #
# #        print json.dumps(response.to_dict(),sort_keys=True,indent=4)
#
#
#         table_results = []
#         for status in results.group_status.buckets:
#             for VO in results.group_status.buckets[status].group_VO.buckets:
#                 for CommonName in VO['group_CommonName'].buckets:
#                     if CommonName.key == '/CN=Dennis Box/CN=UID:dbox':
#                         print [CommonName.key, VO.key, status, CommonName['numJobs'].value, CommonName['WallHours'].value]
#                     table_results.append([CommonName.key, VO.key, status, CommonName['numJobs'].value, CommonName['WallHours'].value])

        # Figure out how to translate all of this from the old query to the new.
#
#         if not return_code_success:
#             raise Exception('Error querying elasticsearch')
#
#         if len(table_results) == 1 and len(table_results[0].strip()) == 0:
#            print >> sys.stdout, "Nothing to report"
#            return
#
#         for line in table_results:
#             if self.verbose:
#                 print line
#             name = line[0]
#             expname = line[1]
#             status = line[2]
#             count = int(line[3])
#             hours = float(line[4])
#             if expname not in self.experiments:
#                 exp = Experiment(expname)
#                 self.experiments[expname] = exp
#             else:
#                 exp = self.experiments[expname]
#             if name not in exp.users:
#                 user = User(name)
#                 exp.add_user(name, user)
# #                print name, user, user.success
#             else:
#                 user = exp.users[name]
#             if status == 'Success':
#                 user.add_success(count, hours)
#             #    exp.add_success(count, hours)
#             else:
#                 user.add_failure(count, hours)
#                 #print user.failure
#              #   exp.add_failure(count, hours)
#         # MySQLUtils.removeClientConfig(mysql_client_cfg)

    def generate(self):
        """

        :return:
        """
        #        mysql_client_cfg = MySQLUtils.createClientConfig("main_db", self.config)
        #        self.connect_str = MySQLUtils.getDbConnection("main_db", mysql_client_cfg, self.config)
        #        select = "select VO.VOName,CommonName,if (ApplicationExitCode=0,'Success','Failure') as Status,sum(NJobs) as NJobs," + \
        #               "round(sum(WallDuration*Cores/3600.),1) as WallHours from MasterSummaryData m " + \
        #               "JOIN VONameCorrection VC ON (VC.corrid=m.VOcorrid) JOIN VO on (VC.void = VO.void) " + \
        #               "where EndTime>'"+self.start_time + "' AND EndTime < '"+self.end_time + \
        #               "' and probename like 'condor:fifebatch%.fnal.gov' group by status,VOName,CommonName order by VO.VOName,CommonName,status;"

        client = self.establish_client()
        resultquery = self.query(client)

        response = resultquery.execute()
        return_code_success = response.success()
        results = response.aggregations

        print json.dumps(response.to_dict(), sort_keys=True, indent=4)

        table_results = []
        for status in results.group_status.buckets:
            for VO in results.group_status.buckets[
                status].group_VO.buckets:
                for CommonName in VO['group_CommonName'].buckets:
                    # if CommonName.key == '/CN=Dennis Box/CN=UID:dbox':
                    #     item = CommonName
                    #     print item
                    table_results.append([CommonName.key, VO.key, status,
                                          CommonName['numJobs'].value,
                                          CommonName['WallHours'].value])

        # Figure out how to translate all of this from the old query to the new.

        if not return_code_success:
            raise Exception('Error querying elasticsearch')

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
                #                print name, user, user.success
            else:
                user = exp.users[name]
            if status == 'Success':
                user.add_success(count, hours)
            # exp.add_success(count, hours)
            else:
                user.add_failure(count, hours)
                # print user.failure
                #   exp.add_failure(count, hours)
                # MySQLUtils.removeClientConfig(mysql_client_cfg)

    def send_report(self):
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
                table += '\n<tr><td align="left">%s</td><td align="left">%s</td><td align="right">%s</td><td align="right">%s</td><td align="right">%s</td><td align="right">%s</td><td align="right">%s</td><td align="right">%s</td></tr>' %\
                      (key, uname, NiceNum.niceNum(user.success[0] + user.failure[0]),  NiceNum.niceNum(user.failure[0]), failure_rate, NiceNum.niceNum(user.success[1] + user.failure[1], 1), NiceNum.niceNum(user.failure[1], 1), waste_per)
                total_hrs += (user.success[1] + user.failure[1])
                total_jobs += (user.success[0] + user.failure[0])
        text = "".join(open("template_wasted_hours.html").readlines())
        text = text.replace("$START", self.start_time)
        text = text.replace("$END", self.end_time)
        text = text.replace("$TABLE", table)
        print "Writing file"
        fn = "user_wasted_hours_report.%s" % (self.end_time.split(" ")[0].replace("/", "-"))
        f = open(fn, "w")
        f.write(text)
        f.close()
        emails = ""
        if self.is_test:
            emails = self.config.get("email", "test_to").split(", ")
        else:
            pass
        print total_jobs, total_hrs
            # emails=self.config.get("email", "%s_email" % (self.vo.lower())).split(",")+self.config.get("email", "test_to").split(",")
#        TextUtils.sendEmail(([], emails), "%s Wasted Hours on the GPGrid (%s - %s)" % ("FIFE", self.start_time, self.end_time), {"html": text},  ("Gratia Operation", "sbhat@fnal.gov"), "smtp.fnal.gov")
        # os.unlink(fn)



def parse_opts():
        """Parses command line options"""

        usage = "Usage: %prog [options]"
        parser = optparse.OptionParser(usage)
        parser.add_option("-c", "--config", dest="config", type="string", help="report configuration file (required)")
        parser.add_option("-v", "--verbose", action="store_true", dest="verbose", default=False,
                          help="print debug messages to stdout")
        parser.add_option("-s", "--start", type="string", dest="start",
                          help="report start date YYYY/MM/DD HH:MM:DD (required)")
        parser.add_option("-e", "--end", type="string", dest="end", help="report end date YYYY/MM/DD")
        parser.add_option("-d", "--dryrun", action="store_true", dest="is_test", default=False,
                          help="send emails only to testers")

        opts, args = parser.parse_args()
        Configuration.checkRequiredArguments(opts, parser)
        return opts, args


if __name__ == "__main__":
    opts, args = parse_opts()
    try:
        config = Configuration.Configuration()
        config.configure(opts.config)
        report = WastedHoursReport(config, opts.start, opts.end, opts.is_test, opts.verbose)
        report.generate()
        #report.generate_test()

        report.send_report()
    except:
        print >> sys.stderr, traceback.format_exc()
        sys.exit(1)
    sys.exit(0)
