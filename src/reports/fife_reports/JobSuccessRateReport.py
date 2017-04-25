import sys
import re
from time import sleep
import traceback
import datetime
from ConfigParser import NoOptionError

from elasticsearch_dsl import Search

from . import Reporter, runerror, get_configfile, get_template, Configuration
import reports.TextUtils as TextUtils

# Various config values and their default values
config_vals = {'num_clusters': 100, 'jobs_per_cluster': 1e6,
               'num_hosts_per_site': 1000, 'errors_per_host': 1000,
               'num_failed_sites': 1000}
logfile = 'jobsuccessratereport.log'
default_templatefile = 'template_jobrate.html'


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
    parser.add_argument("-E", "--experiment", dest="vo",
                        help="experiment name", default=None, required=True)


def sum_errors(dic):
    """Helper function to sum up number of failed jobs per host.
    Assumes that dic is in the form

    :param dict dic: {"error_code1":count1, "error_code2":count2, etc.}
    :return int: Sum of all values in dic
    """
    return sum(value for key, value in dic.iteritems())


class Jobs:
    """Class to assign jobs to sites"""
    def __init__(self):
        self.jobs = {}

    def add_job(self, site, job):
        """
        Adds job to self.jobs dict

        :param site: OSG site where job ran
        :param job: Job object that contains info about a job
        :return: None
        """
        if site not in self.jobs:
            self.jobs[site] = []

        self.jobs[job.site].append(job)


class Job:
    """Class that holds job information
    :param str end_time: End time of job
    :param str start_time: Start time of job
    :param str jobid: JobID of job
    :param str site: Site where job ran
    :param str host: Host where job ran
    :param str exit__code: Exit code of job
    """
    def __init__(self, end_time, start_time, jobid, site, host, exit__code):
        self.end_time = end_time
        self.start_time = start_time
        self.jobid = jobid
        self.site = site
        self.host = host
        self.exit_code = exit__code


class JobSuccessRateReporter(Reporter):
    """
    Class to hold information about and run Job Success Rate report.  To
    execute, instantiate the class and then use the run_report() method.

    :param Configuration.Configuration config: Report Configuration object
    :param str start: Start time of report range
    :param str end: End time of report range
    :param str vo: Experiment to run report on
    :param str template: Filename of HTML template to generate report
    :param bool is_test: Whether or not this is a test run.
    :param bool verbose: Verbose flag
    :param bool no_email: If true, don't actually send the email
    """
    def __init__(self, config, start, end, vo, template, is_test,
                 verbose, no_email, ov_logfile=None):
        report = 'JobSuccessRate'
        self.vo = vo

        if ov_logfile:
            rlogfile = ov_logfile
            logfile_override = True
        else:
            rlogfile = logfile
            logfile_override = False

        Reporter.__init__(self, report, config, start, end, verbose,
                          raw=True, is_test=is_test, no_email=no_email, logfile=rlogfile,
                          logfile_override=logfile_override)
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
        self.limit_sites = self._limit_site_check()
        self.title = "{0} Production Jobs Success Rate on the OSG Sites " \
                     "({1} - {2})".format(
                                self.vo,
                                self.start_time,
                                self.end_time)

    def run_report(self):
        """Method that runs all of the applicable actions in this class."""
        self.generate()
        self.generate_report_file()
        self.send_report()

    def query(self):
        """
        Method to query Elasticsearch cluster for EfficiencyReport information

        :return elasticsearch_dsl.Search: Search object containing ES query
        """
        # Set up our search parameters
        voq = self.config.get(self.vo.lower(), "voname".format(self.vo.lower()))
        productioncheck = '*Role=Production*'

        starttimeq = self.dateparse_to_iso(self.start_time)
        endtimeq = self.dateparse_to_iso(self.end_time)

        self.logger.info(self.indexpattern)
        if self.verbose:
            sleep(3)

        # Elasticsearch query
        s = Search(using=self.client, index=self.indexpattern) \
            .filter("range", EndTime={"gte": starttimeq, "lt": endtimeq}) \
            .filter("term", ResourceType="Payload")

        if self.vo in re.split(',', self.config.get('noproduction', 'list')):
            s = s.filter("wildcard", VOName=voq)
        else:
            s = s.filter("wildcard", VOName=productioncheck)\
                .filter("term", VOName=voq)

        if self.verbose:
            print s.to_dict()

        return s

    def generate(self):
        """Main driver of activity in report.  Runs the ES query, checks for
        success, and then runs routines to generate the results lines, parse
        those lines to add to Job, Jobs, and clusters structures."""
        resultset = self.run_query()

        # Add all of our results to the clusters dictionary
        resultscount = 0
        add_clusters = self._add_to_clusters()
        add_clusters.send(None)
        for line in self._generate_result_array(resultset):
            if resultscount == 0:
                if len(line.strip()) == 0:
                    self.logger.info("Nothing to report")
                    break
            add_clusters.send(line)
            resultscount += 1

        return

    def _add_to_clusters(self):
        """Coroutine: For each line fed in, will
        instantiate Job class for each one, add to Jobs class dictionary,
        and add to clusters.  Then waits for next line"""
        while True:
            line = yield
            tmp = line.split('\t')
            start_time, end_time, userid, jobid, site = (item.strip() for item in tmp[:5])
            start_time, end_time = (t.replace('T', ' ').replace('Z', '') for t in (start_time, end_time))
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

    def _generate_result_array(self, resultset):
        """Generator.  Compiles results from resultset into array.  Yields each
        line.

        :param elastisearch_dsl.Search resultset: ES Search object containing
        query
        """
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

    def generate_report_file(self):
        """This is the function that parses the clusters data and
        creates the report HTML file"""
        total_failed = 0
        if len(self.run.jobs) == 0:
            self.logger.info("no_email flag triggered - no jobs to report on")
            self.no_email = True
            return

        # Grab config values.  If they don't exist, keep defaults
        for key in config_vals:
            try:
                config_vals[key] = int(self.config.get(self.vo.lower(), key))
            except NoOptionError:
                pass

        table_summary = ""
        job_table = ""
        job_table_cl_count = 0

        def tdalign(info, align):
            """HTML generator to wrap a table cell with alignment"""
            return '<td align="{0}">{1}</td>'.format(align, info)

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
            if job_table_cl_count < config_vals['num_clusters']:  # Limit number of clusters
                                                   # shown in report based on config file

                linemap = ((cid, 'left'), (cdict['userid'], 'right'),
                           (total_jobs, 'right'), (total_jobs_failed, 'right'))

                job_table += '\n<tr>' + \
                             ''.join((tdalign(key, al) for key, al in linemap)) + \
                             '<td></td>' * 6 + '</tr>'

            # Generate HTML line for each failed job
                jcount = 0

                for job in failures:
                    if jcount < config_vals['jobs_per_cluster']:
                        # Generate link for each job for certain number of jobs
                        try:
                            job_link_parts = \
                                [elt for elt in
                                 self._get_job_parts_from_jobid(job.jobid)]

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


                        linemap = ((job_html, 'left'), (job.start_time, 'left'),
                                   (job.end_time, 'left'), (job.site, 'right'),
                                   (job.host, 'right'), (job.exit_code, 'right'))
                        job_table += '\n<tr>' + '<td></td>' * 4 + ''.join((tdalign(key, al) for key, al in linemap)) + '</tr>'
                        jcount += 1
                    else:
                        break
                job_table_cl_count += 1

        total_jobs = 0

        site_failed_dict = {}
        # Compile count of failed jobs, calculate job success rate
        for site, jobs in self.run.jobs.iteritems():
            failed = 0
            total = len(jobs)
            failures = {}
            # failures structure:
            # {host: {exit_code1:count, exit_code2:count},
            # host2: {exit_code1:count, exit_code2: count}, etc.}
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
                                                                    site,
                                                                    total,
                                                                    failed,
                                                                    round((total - failed) * 100. / total, 1))
            site_failed_dict[site] = {}
            site_failed_dict[site]['FailedJobs'] = failed
            if 'HTMLLines' not in site_failed_dict[site]:
                site_failed_dict[site]['HTMLLines'] = \
                    '\n<tr><td align = "left">{0}</td>' \
                    '<td align = "right">{1}</td>' \
                    '<td align = "right">{2}</td>'\
                    '<td align = "right">{3}</td>' \
                    '<td></td><td></td><td></td></tr>'.format(
                        site,
                        total,
                        failed,
                        round((total - failed) * 100. / total, 1))

            hostcount = 0

            for host, errors in sorted(failures.iteritems(),
                                       key=lambda x: sum_errors(x[1]),
                                       reverse=True):
                # Sort hosts by total error count in reverse order
                if hostcount < config_vals['num_hosts_per_site']:
                    errcount = 0
                    for code, count in sorted(errors.iteritems(),
                                              key=lambda x: x[1],
                                              reverse=True):
                        # Sort error codes for each host by count in
                        # reverse order
                        if errcount < config_vals['errors_per_host']:
                            site_failed_dict[site]['HTMLLines'] += \
                                '\n<tr><td></td><td></td><td></td><td></td>' \
                                '<td align = "left">{0}</td>'\
                                '<td align = "right">{1}</td>' \
                                '<td align = "right">{2}</td></tr>'.format(
                                    host,
                                    code,
                                    count)
                            errcount += 1
                        else:
                            break
                    hostcount += 1
                else:
                    break

        faildict = {site: item['FailedJobs']
                    for site, item in site_failed_dict.iteritems()}

        if self.limit_sites:
            # If a VO wants to limit the number of sites
            try:
                # Take top ten failed sites in descending order
                failkeys = (site[0] for site in sorted(faildict.iteritems(),
                                                       key=lambda x: x[1],
                                                       reverse=True)[0:config_vals['num_failed_sites']])
            except IndexError:
                # Take all sites in descending order
                failkeys = (site[0] for site in sorted(faildict.iteritems(),
                                                       key=lambda x: x[1],
                                                       reverse=True))
            finally:
                sitetabletitle_add = " (Top {0} sites shown here, " \
                                 "top {1} hosts per site)".format(config_vals['num_failed_sites'],
                                                                  config_vals['num_hosts_per_site'])
        else:
            failkeys = (site[0] for site in
                        sorted(faildict.iteritems(), key=lambda x: x[1],
                               reverse=True))
            sitetabletitle_add = ""

        sitetabletitle = "Site Details{0}".format(sitetabletitle_add)

        table = ''.join(str(site_failed_dict[site]['HTMLLines']) for site in failkeys)

        linemap = (('Total', 'left'), (total_jobs, 'right'),
                   (total_failed, 'right'),
                   (round((total_jobs - total_failed) * 100. / total_jobs, 1), 'right'))
        table += '\n<tr>' + \
                 ''.join((tdalign(key, al) for key, al in linemap)) + \
                 '<td></td>' * 3 + '</tr>'


        linemap = (('Total', 'left'), (total_jobs, 'right'),
                   (total_failed, 'right'),
                   (round((total_jobs - total_failed) * 100. / total_jobs, 1), 'right'))
        table_summary += '\n<tr>' + ''.join((tdalign(key, al) for key, al in linemap)) + '</tr>'

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

        # Cosmetic reformatting of site failed jobs section
        if config_vals['jobs_per_cluster'] > 1000:
            string_per_cluster = ''
        else:
            string_per_cluster = ', {0} per cluster'.format(
                int(config_vals['jobs_per_cluster']))

        # Grab HTML template, replace variables shown
        numclustertabletitle = 'Failed Job Details ' \
                           '({0} clusters shown here{1})'.format(int(config_vals['num_clusters']),
                                                                 string_per_cluster)

        headerdict = {
            "summaryheader": ['Site', 'Total Jobs', 'Failed Jobs', 'Success Rate'],
            "clusterheader": ['Cluster', 'UserID', 'Total Jobs', 'Failed Jobs',
                              'JobID', 'Start Time', 'End Time', 'Site',
                              'Host', 'ExitCode'],
            "siteheader": ['Site', 'Total Jobs', 'Failed Jobs', 'Success Rate',
                           'Bad Host', 'Exit Code', 'Failed Jobs on Host'],
        }

        htmldict = dict(title=self.title, table_summary=table_summary,
                        table_jobs=job_table, table=table,
                        fifemon_link=fifemon_link, divopen=divopen,
                        divclose=divclose,
                        numclustertabletitle=numclustertabletitle,
                        sitetabletitle=sitetabletitle)

        for tag, l in headerdict.iteritems():
            htmldict[tag] = self._generate_header(l)

        self.text = "".join(open(self.template).readlines())
        self.text = self.text.format(**htmldict)

        return

    def _get_job_parts_from_jobid(self, jobid):
        """
        Parses the jobid string and grabs the relevant parts to generate
        Fifemon link

        :param str jobid: GlobalJobId field of a GRACC record
        :return tuple: Tuple of jobid, schedd
        """
        return self.jobpattern.match(jobid).groups()

    @staticmethod
    def _generate_header(headerlist):
        """Method that creates HTML for table headers

        :param list headerlist: List of header columns
        :return str: HTML for header
        """
        htmlheader = '<th style="text-align:center">' + \
            '</th><th>'.join(headerlist) + '</th>'
        return htmlheader

    def send_report(self):
        """Method to send emails of report file to intended recipients."""
        if self.test_no_email(self.email_info["to_emails"]):
            return

        TextUtils.sendEmail((self.email_info["to_names"],
                             self.email_info["to_emails"]),
                            self.title,
                            {"html": self.text},
                            (self.email_info["from_name"],
                             self.email_info["from_email"]),
                            self.email_info["smtphost"])

        self.logger.info("Sent Report for {0}".format(self.vo))
        return

    def _limit_site_check(self):
        """Check to see if the num_failed_sites option is set in the config
        file for the VO"""
        return self.config.has_option(self.vo.lower(), 'num_failed_sites')


def main():
    args = parse_opts()

    # Set up the configuration
    config = Configuration.Configuration()
    config.configure(get_configfile(override=args.config, flag='fife'))

    templatefile = get_template(override=args.template, deffile=default_templatefile)

    try:
        r = JobSuccessRateReporter(config,
                                   args.start,
                                   args.end,
                                   args.vo,
                                   templatefile,
                                   args.is_test,
                                   args.verbose,
                                   args.no_email,
                                   ov_logfile=args.logfile)
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


if __name__ == "__main__":
    main()