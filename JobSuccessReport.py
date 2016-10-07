import sys
import os
import re
import logging
from time import sleep
import traceback

from elasticsearch_dsl import Search, Q

import TextUtils
import Configuration
from Reporter import Reporter, runerror


class Jobs:
    def __init__(self):
        self.jobs = {}

    def add_job(self, site, job):
        if site not in self.jobs:
            self.jobs[site] = []

        self.jobs[job.site].append(job)


class Job:
    def __init__(self, end_time, start_time, jobid, site, host, exit__code):
        self.end_time = end_time
        self.start_time = start_time
        self.jobid = jobid
        self.site = site
        self.host = host
        self.exit_code = exit__code


class JobSuccessRateReporter(Reporter):
    def __init__(self, configuration, start, end, vo, template, is_test, verbose, no_email):
        Reporter.__init__(self, configuration, start, end, verbose)
        self.no_email = no_email
        self.is_test = is_test
        self.vo = vo
        self.template = template
        self.title = "Production Jobs Success Rate {0} - {1}".format(self.start_time, self.end_time)
        self.run = Jobs()
        self.clusters = {}
        self.connectStr = None
        self.usermatch_CILogon = re.compile('.+CN=UID:(\w+)')
        self.usermatch_FNAL = re.compile('.+/(\w+\.fnal\.gov)')
        self.globaljobparts = re.compile('\w+\.(fifebatch\d\.fnal\.gov)#(\d+\.\d+)#.+')
        self.realhost_pattern = re.compile('\s\(primary\)')
        self.jobpattern = re.compile('(\d+).\d+@(fifebatch\d\.fnal\.gov)')

    def query(self, client):
        """Method that actually queries elasticsearch"""
        # Set up our search parameters
        voq = self.config.get("query", "{0}_voname".format(self.vo.lower()))
        productioncheck = '*Role=Production*'

        starttimeq = self.dateparse_to_iso(self.start_time)
        endtimeq = self.dateparse_to_iso(self.end_time)

        if self.verbose:
            print >> sys.stdout, self.indexpattern
            sleep(3)

        # Elasticsearch query
        resultset = Search(using=client, index=self.indexpattern) \
            .query("wildcard", VOName=productioncheck) \
            .filter(Q({"term": {"VOName": voq}})) \
            .filter("range", EndTime={"gte": starttimeq, "lt": endtimeq}) \
            .filter(Q({"term": {"ResourceType": "Payload"}}))

        if self.verbose:
            print resultset.to_dict()

        return resultset

    def get_job_parts_from_jobid(self, jobid):
        return self.jobpattern.match(jobid).groups()

    def generate_result_array(self, resultset):
        # Compile results into array
        results = []
        for hit in resultset.scan():
            try:
                # Parse userid
                try:
                    # Grabs the first parenthesized subgroup in the hit['CommonName'] string, where that subgroup comes
                    # after "CN=UID:"
                    userid = self.usermatch_CILogon.match(hit['CommonName']).\
                        group(1)
                except AttributeError:
                    try:
                        userid = self.usermatch_FNAL.match(hit['CommonName']).\
                            group(1)  # If this doesn't match CILogon standard, just grab the *.fnal.gov string at the end
                    except AttributeError:
                        userid = hit['CommonName']  # Just print the CN string, move on
                # Parse jobid
                try:
                    # Parse the GlobalJobId string to grab the cluster number and schedd
                    jobparts = self.globaljobparts.match(hit['GlobalJobId']).group(2,1)
                    # Put these together to create the jobid (e.g. 123.0@fifebatch1.fnal.gov)
                    jobid = '{0}@{1}'.format(*jobparts)
                except AttributeError:
                    jobid = hit[
                        'GlobalJobId']  # If for some reason a probe gives us a bad jobid string, just keep going
                realhost = self.realhost_pattern.sub('', hit['Host'])  # Parse to get the real hostname

                outstr = '{starttime}\t{endtime}\t{CN}\t{JobID}\t{hostdescription}\t{host}\t{exitcode}'.format(
                    starttime=hit['StartTime'],
                    endtime=hit['EndTime'],
                    CN=userid,
                    JobID=jobid,
                    hostdescription=hit['Host_description'],
                    host=realhost,
                    exitcode=hit['Resource_ExitCode']
                )
                results.append(outstr)
                if self.verbose:
                    print >> sys.stdout, outstr
            except KeyError:
                # We want to ignore records where one of the above keys isn't listed in the ES document.
                # This is consistent with how the old MySQL report behaved.
                pass
        return results

    def add_to_clusters(self, results):
        # Grab each line in results, instantiate Job class for each one, and add to clusters
        for line in results:
            tmp = line.split('\t')
            start_time = tmp[0].strip().replace('T', ' ').replace('Z', '')
            end_time = tmp[1].strip().replace('T', ' ').replace('Z', '')
            userid = tmp[2].strip()
            jobid = tmp[3].strip()
            site = tmp[4].strip()
            if site == "NULL":
                continue
            host = tmp[5].strip()
            status = int(tmp[6].strip())
            job = Job(end_time, start_time, jobid, site, host, status)
            self.run.add_job(site, job)
            clusterid = jobid.split(".")[0]
            if clusterid not in self.clusters:
                self.clusters[clusterid] = {'userid': userid, 'jobs': []}
            self.clusters[clusterid]['jobs'].append(job)
        return

    def generate(self):
        client = self.establish_client()
        resultset = self.query(client)  # Generate Search object for ES
        response = resultset.execute()  # Execute that Search
        return_code_success = response.success()  # True if the elasticsearch query completed without errors
        results = self.generate_result_array(resultset)  # Format our resultset into an array we use later

        if not return_code_success:
            raise Exception('Error accessing ElasticSearch')
        if len(results) == 1 and len(results[0].strip()) == 0:
            print >> sys.stdout, "Nothing to report"
            return

        self.add_to_clusters(results)  # Parse our results and create clusters objects for each
        return

    def send_report(self):
        table = ""
        total_failed = 0
        if len(self.run.jobs) == 0:
            return
        table_summary = ""
        job_table = ""

        job_table_cl_count = 0
        # Look in clusters, figure out whether job failed or succeded, categorize appropriately,
        # and generate HTML line for total jobs failed by cluster
        for cid, cdict in self.clusters.iteritems():
            total_jobs = len(cdict['jobs'])
            failures = []
            total_jobs_failed = 0
            for job in cdict['jobs']:
                if job.exit_code == 0:
                    continue
                total_jobs_failed += 1
                failures.append(job)
            if total_jobs_failed == 0:
                continue
            if job_table_cl_count < 100:  # Limit number of clusters shown in report to 100.
                job_table += '\n<tr><td align = "left">{0}</td><td align = "right">{1}</td><td align = "right">{2}'\
                             '</td><td align = "right">{3}</td><td></td><td></td><td></td><td></td><td></td>'\
                             '<td></td></tr>'.format(
                                                cid,
                                                cdict['userid'],
                                                total_jobs,
                                                total_jobs_failed)
                # Generate HTML line for each failed job
                for job in failures:

                    job_link_parts = [elt for elt in
                                      self.get_job_parts_from_jobid(job.jobid)]
                    timestamps_exact = self.get_epoch_stamps_for_grafana(
                        start_time=job.start_time, end_time=job.end_time)
                    padding = 300000        # milliseconds
                    timestamps_padded = (timestamps_exact[0]-padding,
                                         timestamps_exact[1]+padding)
                    job_link_parts.extend(timestamps_padded)
                    job_link = 'https://fifemon.fnal.gov/monitor/dashboard/db' \
                               '/job-cluster-summary?var-cluster={0}' \
                               '&var-schedd={1}&from={2}&to={3}'.format(
                        *job_link_parts)
                    job_html = '<a href="{0}">{1}</a>'.format(job_link,
                                                              job.jobid)

                    job_table += '\n<tr><td></td><td></td><td></td><td></td><td align = "left">{0}</td>'\
                                 '<td align = "left">{1}</td><td align = "left">{2}</td><td align = "right">{3}</td>'\
                                 '<td align = "right">{4}</td><td align = "right">{5}</td></tr>'.format(
                                                                                                    job_html,
                                                                                                    job.start_time,
                                                                                                    job.end_time,
                                                                                                    job.site,
                                                                                                    job.host,
                                                                                                    job.exit_code)
                job_table_cl_count += 1

        total_jobs = 0

        # Compile count of failed jobs, calculate job success rate
        for key, jobs in self.run.jobs.items():
            failed = 0
            total = len(jobs)
            failures = {}
            for job in jobs:
                if job.exit_code != 0:
                    failed += 1
                    if job.host not in failures:
                        failures[job.host] = {}
                    if job.exit_code not in failures[job.host]:
                        failures[job.host][job.exit_code] = 0
                    failures[job.host][job.exit_code] += 1
            total_jobs += total
            total_failed += failed
            table_summary += '\n<tr><td align = "left">{0}</td><td align = "right">{1}</td><td align = "right">{2}</td>'\
                             '<td align = "right">{3}</td></tr>'.format(
                                                                    key,
                                                                    total,
                                                                    failed,
                                                                    round((total - failed) * 100. / total, 1))
            table += '\n<tr><td align = "left">{0}</td><td align = "right">{1}</td><td align = "right">{2}</td>'\
                     '<td align = "right">{3}</td><td></td><td></td><td></td></tr>'.format(
                                                                                    key,
                                                                                    total,
                                                                                    failed,
                                                                                    round((total - failed) * 100. / total, 1))
            for host, errors in failures.items():
#                print host, len(failures)
                for code, count in errors.items():
                    table += '\n<tr><td></td><td></td><td></td><td></td><td align = "left">{0}</td>'\
                             '<td align = "right">{1}</td><td align = "right">{2}</td></tr>'.format(
                                                                                            host,
                                                                                            code,
                                                                                            count)

        table += '\n<tr><td align = "left">Total</td><td align = "right">{0}</td><td align = "right">{1}</td>'\
                 '<td align = "right">{2}</td><td></td><td></td><td></td></tr>'.format(
                                                                                total_jobs,
                                                                                total_failed,
                                                                                round((total_jobs - total_failed) * 100. / total_jobs, 1))
        table_summary += '\n<tr><td align = "left">Total</td><td align = "right">{0}</td><td align = "right">{1}</td>'\
                         '<td align = "right">{2}</td></td></tr>'.format(
                                                                    total_jobs,
                                                                    total_failed,
                                                                    round((total_jobs - total_failed) * 100. / total_jobs, 1))

        epoch_stamps = self.get_epoch_stamps_for_grafana()
        elist = [elt for elt in epoch_stamps]
        elist.append('{0}pro'.format(self.vo.lower()))
        fifemon_link_raw = 'https://fifemon.fnal.gov/monitor/dashboard/db/' \
                       'user-batch-history?from={0}&to={1}&' \
                       'var-user={2}'.format(*elist)
        fifemon_link = '<a href="{0}">Fifemon</a>'.format(fifemon_link_raw)

        if total_failed == 0:
            divopen = '\n<div style="display:none">'
            divclose = '\n</div>'
        else:
            divopen = ''
            divclose = ''

        # Grab HTML template, replace variables shown
        text = "".join(open(self.template).readlines())
        text = text.replace("$START", self.start_time)
        text = text.replace("$END", self.end_time)
        text = text.replace("$TABLE_SUMMARY", table_summary)
        text = text.replace("$DIVOPEN", divopen)
        text = text.replace("$TABLE_JOBS", job_table)
        text = text.replace("$DIVCLOSE", divclose)
        text = text.replace("$TABLE", table)
        text = text.replace("$FIFEMON_LINK", fifemon_link)
        text = text.replace("$VO", self.vo)

        # Generate HTML file to send
        fn = "{0}-jobrate.{1}".format(self.vo.lower(),
                                    self.start_time.replace("/", "-"))

        with open(fn, 'w') as f:
            f.write(text)

        # The part that actually emails people.
        if self.no_email:
            print "Not sending email"
            return

        if self.is_test:
            emails = re.split('[; ,]', self.config.get("email", "test_to"))
        else:
            emails = re.split('[; ,]', self.config.get("email", "{0}_email".format(self.vo.lower()))) + \
                     re.split('[: ,]', self.config.get("email", "test_to"))

        TextUtils.sendEmail(([], emails),
                            "{0} Production Jobs Success Rate on the OSG Sites ({1} - {2})".format(self.vo,
                                                                                                self.start_time,
                                                                                                self.end_time),
                            {"html": text},
                            ("GRACC Operations", "sbhat@fnal.gov"),
                            "smtp.fnal.gov")

        os.unlink(fn)  # Delete HTML file


if __name__ == "__main__":
    opts, args = Reporter.parse_opts()

    if opts.debug:
        logging.basicConfig(filename='jobsuccessreport.log', level=logging.DEBUG)
    else:
        logging.basicConfig(filename='jobsuccessreport.log', level=logging.ERROR)
        logging.getLogger('elasticsearch.trace').addHandler(logging.StreamHandler())

    config = Configuration.Configuration()
    config.configure(opts.config)

    try:
        r = JobSuccessRateReporter(config, opts.start, opts.end, opts.vo, opts.template, opts.is_test, opts.verbose,
                                   opts.no_email)
        r.generate()
        r.send_report()
        print "Sent Report"
    except Exception as e:
        print >> sys.stderr, traceback.format_exc()
        runerror(config, e, traceback.format_exc())
        sys.exit(1)
    sys.exit(0)
