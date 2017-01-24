#!/usr/bin/python

import sys
import os
import re
import ast
from time import sleep
import traceback
import inspect
import datetime
from ConfigParser import NoOptionError

from elasticsearch_dsl import Search, Q

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
from Reporter import Reporter, runerror

logfile = 'jobsuccessratereport.log'


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
    def __init__(self, configuration, start, end, vo, template, is_test,
                 verbose, no_email):
        report = 'JobSuccessRate'
        Reporter.__init__(self, report, configuration, start, end, verbose,
                          is_test=is_test, no_email=no_email, logfile=logfile)
        self.vo = vo
        self.template = template
        self.title = "Production Jobs Success Rate {0} - {1}".format(
            self.start_time, self.end_time)
        self.run = Jobs()
        self.clusters = {}
        self.connectStr = None
        self.usermatch_CILogon = re.compile('.+CN=UID:(\w+)')
        self.usermatch_FNAL = re.compile('.+/(\w+\.fnal\.gov)')
        self.globaljobparts = re.compile('\w+\.(fifebatch\d\.fnal\.gov)#(\d+\.\d+)#.+')
        self.realhost_pattern = re.compile('\s\(primary\)')
        self.jobpattern = re.compile('(\d+).\d+@(fifebatch\d\.fnal\.gov)')
        self.text = ''
        self.fn = "{0}-jobrate.{1}".format(self.vo.lower(),
                                           self.start_time.replace("/", "-"))
        try:
            self.limit_sites = ast.literal_eval(
                self.config.get(self.vo.lower(), 'limit_sites'))
        except NoOptionError:
            self.limit_sites = False

    def query(self):
        """Method that actually queries elasticsearch"""
        # Set up our search parameters
        voq = self.config.get(self.vo.lower(), "voname".format(self.vo.lower()))
        productioncheck = '*Role=Production*'

        starttimeq = self.dateparse_to_iso(self.start_time)
        endtimeq = self.dateparse_to_iso(self.end_time)

        self.logger.info(self.indexpattern)
        if self.verbose:
            sleep(3)

        # Elasticsearch query
        resultset = Search(using=self.client, index=self.indexpattern) \
            .filter("range", EndTime={"gte": starttimeq, "lt": endtimeq}) \
            .filter("term", ResourceType="Payload")

        if self.vo in re.split(',', self.config.get('noproduction', 'list')):
            resultset = resultset.filter("wildcard", VOName=voq)
        else:
            resultset = resultset.filter("wildcard", VOName=productioncheck)\
                .filter("term", VOName=voq)

        if self.verbose:
            print resultset.to_dict()

        return resultset

    def get_job_parts_from_jobid(self, jobid):
        return self.jobpattern.match(jobid).groups()

    def generate_result_array(self, resultset):
        """Generator.  Compiles results from resultset into array.  Yields each
        line."""
        for hit in resultset.scan():
            try:
                # Parse userid
                try:
                    # Grabs the first parenthesized subgroup in the
                    # hit['CommonName'] string, where that subgroup comes
                    # after "CN=UID:"
                    userid = self.usermatch_CILogon.match(hit['CommonName']).\
                        group(1)
                except AttributeError:
                    # If this doesn't match CILogon standard, see if it
                    # matches *.fnal.gov string at the end.  If so,
                    # it's a managed proxy most likely, so give the localuserid
                    if self.usermatch_FNAL.match(hit['CommonName']) and 'LocalUserId' in hit:
                            userid = hit['LocalUserId']
                    else:
                        userid = hit['CommonName']  # Just print the CN string, move on
                # Parse jobid
                try:
                    # Parse the GlobalJobId string to grab the cluster number and schedd
                    jobparts = self.globaljobparts.match(hit['GlobalJobId']).group(2,1)
                    # Put these together to create the jobid (e.g. 123.0@fifebatch1.fnal.gov)
                    jobid = '{0}@{1}'.format(*jobparts)
                except AttributeError:
                    jobid = hit['GlobalJobId']  # If for some reason a probe
                                                # gives us a bad jobid string,
                                                # just keep going
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
                if self.verbose:
                    print >> sys.stdout, outstr
                yield outstr
            except KeyError:
                # We want to ignore records where one of the above keys isn't
                # listed in the ES document.  This is consistent with how the
                # old MySQL report behaved.
                pass

    def add_to_clusters(self):
        """Generator function/coroutine.  For each line fed in, will
        instantiate Job class for each one, add to Jobs class dictionary,
        and add to clusters.  Then waits for next line"""
        while True:
            line = yield
            tmp = line.split('\t')
            start_time = tmp[0].strip().replace('T', ' ').replace('Z', '')
            end_time = tmp[1].strip().replace('T', ' ').replace('Z', '')
            userid = tmp[2].strip()
            jobid = tmp[3].strip()
            site = tmp[4].strip()
            if site == "NULL":
                pass
            else:
                host = tmp[5].strip()
                status = int(tmp[6].strip())
                job = Job(end_time, start_time, jobid, site, host, status)
                self.run.add_job(site, job)
                clusterid = jobid.split(".")[0]
                if clusterid not in self.clusters:
                    self.clusters[clusterid] = {'userid': userid, 'jobs': []}
                self.clusters[clusterid]['jobs'].append(job)

    def generate(self):
        """Main driver of activity in report.  Runs the ES query, checks for
        success, and then runs routines to generate the results lines, parse
        those lines to add to Job, Jobs, and clusters structures."""
        resultset = self.query()  # Generate Search object for ES
        response = resultset.execute()  # Execute that Search
        return_code_success = response.success()  # True if the elasticsearch
                                                  # query completed without errors

        if not return_code_success:
            self.logger.exception('Error accessing ElasticSearch')
            raise Exception('Error accessing ElasticSearch')

        # Add all of our results to the clusters dictionary
        resultscount = 0
        add_clusters = self.add_to_clusters()
        add_clusters.send(None)
        for line in self.generate_result_array(resultset):
            if resultscount == 0:
                if len(line.strip()) == 0:
                    self.logger.info("Nothing to report")
                    break
            add_clusters.send(line)
            resultscount += 1

        return

    def generate_report_file(self, report=None):
        """This is the function that creates the report HTML file"""
        total_failed = 0
        if len(self.run.jobs) == 0:
            self.logger.info("no_email flag triggered - no jobs to report on")
            self.no_email = True
            return
        table_summary = ""
        job_table = ""


        job_table_cl_count = 0

        # Grab config values
        try:
            num_clusters = int(self.config.get(self.vo.lower(), 'num_clusters'))
        except NoOptionError:
            num_clusters = 100

        try:
            jobs_per_cluster = int(
                self.config.get(self.vo.lower(), 'jobs_per_cluster'))
        except NoOptionError:
            jobs_per_cluster = 1e6

        # Look in clusters, figure out whether job failed or succeeded,
        # categorize appropriately, and generate HTML line for total jobs
        # failed by cluster

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
            if job_table_cl_count < num_clusters:  # Limit number of clusters
                                                   # shown in report based on config file
                job_table += '\n<tr><td align = "left">{0}</td>' \
                             '<td align = "right">{1}</td>' \
                             '<td align = "right">{2}</td>' \
                             '<td align = "right">{3}</td>' \
                             '<td></td><td></td><td></td><td></td><td></td>'\
                             '<td></td></tr>'.format(
                                                cid,
                                                cdict['userid'],
                                                total_jobs,
                                                total_jobs_failed)
                # Generate HTML line for each failed job
                jcount = 0

                for job in failures:
                    if jcount < jobs_per_cluster:
                        # Generate link for each job for certain number of jobs
                        try:
                            job_link_parts = \
                                [elt for elt in
                                 self.get_job_parts_from_jobid(job.jobid)]

                            timestamps_exact = self.get_epoch_stamps_for_grafana(
                                start_time=job.start_time,
                                end_time=job.end_time)
                            padding = 300000  # milliseconds
                            timestamps_padded = (timestamps_exact[0] - padding,
                                                 timestamps_exact[1] + padding)
                            job_link_parts.extend(timestamps_padded)
                            job_link = 'https://fifemon.fnal.gov/monitor/dashboard/db' \
                                       '/job-cluster-summary?var-cluster={0}' \
                                       '&var-schedd={1}&from={2}&to={3}'.format(
                                        *job_link_parts)
                        except AttributeError:
                            # If jobID doesn't match the pattern
                            job_link = 'https://fifemon.fnal.gov/monitor/dashboard/db/' \
                                        'experiment-overview?var-experiment={0}'.format(self.vo)

                        job_html = '<a href="{0}">{1}</a>'.format(job_link,
                                                                  job.jobid)

                        job_table += '\n<tr><td></td><td></td><td></td><td></td>' \
                                     '<td align = "left">{0}</td>'\
                                     '<td align = "left">{1}</td>' \
                                     '<td align = "left">{2}</td>' \
                                     '<td align = "right">{3}</td>'\
                                     '<td align = "right">{4}</td>' \
                                     '<td align = "right">{5}</td></tr>'.format(
                                        job_html,
                                        job.start_time,
                                        job.end_time,
                                        job.site,
                                        job.host,
                                        job.exit_code)
                        jcount += 1
                    else:
                        break
                job_table_cl_count += 1

        total_jobs = 0

        site_failed_dict = {}
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
            table_summary += '\n<tr><td align = "left">{0}</td>' \
                             '<td align = "right">{1}</td>' \
                             '<td align = "right">{2}</td>'\
                             '<td align = "right">{3}</td></tr>'.format(
                                                                    key,
                                                                    total,
                                                                    failed,
                                                                    round((total - failed) * 100. / total, 1))
            site_failed_dict[key] = {}
            site_failed_dict[key]['FailedJobs'] = failed
            if 'HTMLLines' not in site_failed_dict[key]:
                site_failed_dict[key]['HTMLLines'] = \
                    '\n<tr><td align = "left">{0}</td>' \
                    '<td align = "right">{1}</td>' \
                    '<td align = "right">{2}</td>'\
                    '<td align = "right">{3}</td>' \
                    '<td></td><td></td><td></td></tr>'.format(
                        key,
                        total,
                        failed,
                        round((total - failed) * 100. / total, 1))

            for host, errors in failures.items():
                for code, count in errors.items():
                    site_failed_dict[key]['HTMLLines'] += \
                        '\n<tr><td></td><td></td><td></td><td></td>' \
                        '<td align = "left">{0}</td>'\
                        '<td align = "right">{1}</td>' \
                        '<td align = "right">{2}</td></tr>'.format(
                            host,
                            code,
                            count)

        # If gratia-main-osg ever upgrades python to 2.7+, replace the next
        # three uncommented lines with the following line:
        # faildict = {key: item['FailedJobs'] for key, item in site_failed_dict.iteritems()}

        faildict = {}
        for key, item in site_failed_dict.iteritems():
            faildict[key] = item['FailedJobs']

        if self.limit_sites:
            try:
                # Take top ten failed sites in descending order
                failkeys = (site[0] for site in sorted(faildict.iteritems())[:-11:-1])
            except IndexError:
                # Take all sites in descending order
                failkeys = (site[0] for site in sorted(faildict.iteritems())[::-1])
        else:
            failkeys = (site[0] for site in sorted(faildict.iteritems())[::-1])

        table = ''.join(str(site_failed_dict[site]['HTMLLines']) for site in failkeys)

        table += '\n<tr><td align = "left">Total</td>' \
                 '<td align = "right">{0}</td>' \
                 '<td align = "right">{1}</td>'\
                 '<td align = "right">{2}</td>' \
                 '<td></td><td></td><td></td></tr>'.format(
                    total_jobs,
                    total_failed,
                    round((total_jobs - total_failed) * 100. / total_jobs, 1))
        table_summary += '\n<tr><td align = "left">Total</td>' \
                         '<td align = "right">{0}</td>' \
                         '<td align = "right">{1}</td>'\
                         '<td align = "right">{2}</td></td></tr>'.format(
                                                                    total_jobs,
                                                                    total_failed,
                                                                    round((total_jobs - total_failed) * 100. / total_jobs, 1))

        # Generate Grafana link for User Batch Details
        epoch_stamps = self.get_epoch_stamps_for_grafana()
        elist = [elt for elt in epoch_stamps]
        elist.append('{0}pro'.format(self.vo.lower()))
        fifemon_link_raw = 'https://fifemon.fnal.gov/monitor/dashboard/db/' \
                           'user-batch-history?from={0}&to={1}&' \
                           'var-user={2}'.format(*elist)
        fifemon_link = '<a href="{0}">Fifemon</a>'.format(fifemon_link_raw)

        # Hide failed jobs table if no failed jobs
        if total_failed == 0:
            divopen = '\n<div style="display:none">'
            divclose = '\n</div>'
        else:
            divopen = ''
            divclose = ''

        if jobs_per_cluster < 1000:
            numclusterheader = 'Failed Job Details ({0} clusters shown here,' \
                         ' {1} per cluster)'.format(num_clusters,
                                                    jobs_per_cluster)
        else:
            numclusterheader = 'Failed Job Details (100 clusters shown here)'

        # Grab HTML template, replace variables shown
        self.text = "".join(open(self.template).readlines())
        self.text = self.text.replace("$START", self.start_time)
        self.text = self.text.replace("$END", self.end_time)
        self.text = self.text.replace("$TABLE_SUMMARY", table_summary)
        self.text = self.text.replace("$DIVOPEN", divopen)
        self.text = self.text.replace("$NUMCLUSTERHEADER", numclusterheader)
        self.text = self.text.replace("$TABLE_JOBS", job_table)
        self.text = self.text.replace("$DIVCLOSE", divclose)
        self.text = self.text.replace("$TABLE", table)
        self.text = self.text.replace("$FIFEMON_LINK", fifemon_link)
        self.text = self.text.replace("$VO", self.vo)

        # Generate HTML file to send

        with open(self.fn, 'w') as f:
            f.write(self.text)

        return

    def send_report(self, report_type=None):
        """Method to send emails of report file to intended recipients."""

        if self.is_test:
            emails = re.split('[; ,]', self.config.get("email", "test_to"))
        else:
            emails = re.split('[; ,]', self.config.get(self.vo.lower(), "email")
                              + ',' + self.config.get("email", "test_to"))

        if self.test_no_email(emails):
            if os.path.exists(self.fn):
                os.unlink(self.fn)  # Delete HTML file
            return

        TextUtils.sendEmail(([], emails),
                            "{0} Production Jobs Success Rate on the OSG Sites ({1} - {2})".format(
                                self.vo,
                                self.start_time,
                                self.end_time),
                            {"html": self.text},
                            ("GRACC Operations", "sbhat@fnal.gov"),
                            "smtp.fnal.gov")
        if os.path.exists(self.fn):
            os.unlink(self.fn)  # Delete HTML file

        self.logger.info("Sent Report for {0}".format(self.vo))
        return

    def run_report(self):
        """Method that runs all of the applicable actions in this class."""
        self.generate()
        self.generate_report_file()
        self.send_report()


if __name__ == "__main__":
    args = Reporter.parse_opts()

    config = Configuration.Configuration()
    config.configure(args.config)

    try:
        r = JobSuccessRateReporter(config, 
                                   args.start,
                                   args.end, 
                                   args.vo, 
                                   args.template, 
                                   args.is_test, 
                                   args.verbose,
                                   args.no_email)
        r.run_report()
        print "Job Success Report Execution finished"
    except Exception as e:
        errstring = '{0}: Error running Job Success Rate Report for {1}. ' \
                    '{2}'.format(datetime.datetime.now(),
                                 args.vo,
                                 traceback.format_exc())
        with open(logfile, 'a') as f:
            f.write(errstring)
        print >> sys.stderr, errstring
        runerror(config, e, errstring)
        sys.exit(1)
    sys.exit(0)
