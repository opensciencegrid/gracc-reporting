import sys
import re
from time import sleep

import traceback
import datetime
from collections import defaultdict, namedtuple

from elasticsearch_dsl import Search

from . import Reporter, runerror, get_configfile, get_template

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
                        help="experiment name", type=unicode, required=True)


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
        self.jobs = defaultdict(list)

    def add_job(self, job):

        """
        Adds job to self.jobs dict

        :param site: OSG site where job ran
        :param job: Job object that contains info about a job
        :return: None
        """
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

    :param config: Report Configuration file
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

        logfile_fname = ov_logfile if ov_logfile is not None else logfile
        logfile_override = True if ov_logfile is not None else False

        self.title = "{0} Production Jobs Success Rate on the OSG Sites " \
                     "({1} - {2})".format(self.vo, start, end)

        super(JobSuccessRateReporter, self).__init__(report, config, start,
                                                     end, verbose,
                                                     is_test=is_test,
                                                     no_email=no_email,
                                                     logfile=logfile_fname,
                                                     logfile_override=logfile_override,
                                                     check_vo=True)
        self.template = template
        self.run = Jobs()
        self.clusters = defaultdict(lambda: {'userid': None, 'jobs': []})
        self.connectStr = None

        # Patch while we fix gratia probes to include CommonName Field
        self.dnusermatch_CILogon = re.compile('.+CN=UID:(\w+)')
        self.dnusermatch_FNAL = None
        # End patch

        self.usermatch_CILogon = re.compile('.+CN=UID:(\w+)')
        self.usermatch_FNAL = re.compile('.+/(\w+\.fnal\.gov)')
        self.globaljobparts = re.compile('\w+\.(.+\.fnal\.gov)#(\d+\.\d+)#.+')
        self.realhost_pattern = re.compile('\s\(primary\)')
        self.jobpattern = re.compile('(\d+).\d+@(.+\.fnal\.gov)')
        self.text = ''
        self.limit_sites = self._limit_site_check()

    def run_report(self):
        """Method that runs all of the applicable actions in this class."""
        self.generate()
        self.generate_report_file()
        smsg = "Report sent for {0} to {1}".format(
            self.vo,", ".join(self.email_info['to']['email']))
        self.send_report(successmessage=smsg)

    def query(self):
        """
        Method to query Elasticsearch cluster for EfficiencyReport information

        :return elasticsearch_dsl.Search: Search object containing ES query
        """
        # Set up our search parameters
        rep_config = self.config[self.report_type.lower()][self.vo.lower()]
        voq = rep_config['voname']  # Using a specific string to check for VO
        productioncheck = '*Role=Production*'

        starttimeq = self.start_time.isoformat()
        endtimeq = self.end_time.isoformat()

        self.logger.info(self.indexpattern)
        if self.verbose:
            sleep(3)

        # Elasticsearch query
        s = Search(using=self.client, index=self.indexpattern) \
            .filter("range", EndTime={"gte": starttimeq, "lt": endtimeq}) \
            .filter("term", ResourceType="Payload")

        if 'no_production' in rep_config and rep_config['no_production']:
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
        add_clusters = self._add_to_clusters()
        add_clusters.send(None)
        for resultscount, line in enumerate(self._generate_result_array(resultset)):
            if resultscount == 0 and len(line) == 0:
                    self.logger.info("Nothing to report")
                    return
            else:
                add_clusters.send(line)

    def _add_to_clusters(self):
        """Coroutine: For each line fed in, will
        instantiate Job class for each one, add to Jobs class dictionary,
        and add to clusters.  Then waits for next line"""
        while True:
            line = yield

            if line['hostdescription'] != "NULL":  # Not NULL site
                line['exitcode'] = int(line['exitcode'])
                job = Job(line['endtime'],
                          line['starttime'],
                          line['jobid'],
                          line['hostdescription'],
                          line['host'],
                          line['exitcode'])
                self.run.add_job(job)
                clusterid = line['jobid'].split(".")[0]
                self.clusters[clusterid]['userid'] = line['userid']
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

                    # Patch while we fix gratia probes to include CommonName Field
                    userid = self.usermatch_CILogon.match(hit['DN']).\
                        group(1)
                    # userid = self.usermatch_CILogon.match(hit['CommonName']).\
                    #     group(1)    # Original
                    # End patch

                except AttributeError:
                    # If this doesn't match CILogon standard, see if it
                    # matches *.fnal.gov string at the end.  If so,
                    # it's a managed proxy most likely, so give the localuserid

                    # Patch while we fix gratia probes to include CommonName Field
                    if self.usermatch_FNAL.match(hit['DN']) and 'LocalUserId' in hit:
                            userid = hit['LocalUserId']
                    else:
                        userid = hit['DN']  # Just print the CN string, move on

                    # # Original
                    # if self.usermatch_FNAL.match(
                    #         hit['CommonName']) and 'LocalUserId' in hit:
                    #     userid = hit['LocalUserId']
                    # else:
                    #     userid = hit[
                    #         'CommonName']  # Just print the CN string, move on
                    # End patch

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

                line = dict((
                    ('starttime', hit['StartTime']),
                    ('endtime', hit['EndTime']),
                    ('userid', userid),
                    ('jobid', jobid),
                    ('hostdescription', hit['Host_description']),
                    ('host', realhost),
                    ('exitcode', hit['Resource_ExitCode'])
                                    ))

                for key in line.iterkeys():
                    line[key] = line[key].strip()

                if self.verbose:
                    print '\t'.join(line.itervalues())
                yield line
            except KeyError:
                """ We want to ignore records where one of the above keys isn't
                 listed in the ES document.  This is consistent with how the
                 old MySQL report behaved."""
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
                config_vals[key] = int(self.config[self.report_type.lower()]
                                       [self.vo.lower()][key])
            except KeyError:
                pass

        table_summary = ""
        job_table = ""

        def tdalign(info, align):
            """HTML generator to wrap a table cell with alignment"""
            return '<td align="{0}">{1}</td>'.format(align, info)

        # Look in clusters, figure out whether job failed or succeeded,
        # categorize appropriately, and generate HTML line for total jobs
        # failed by cluster (Job Details table)
        for job_table_cl_count, (cid, cdict) in enumerate(
                self.clusters.iteritems()):
            total_jobs = len(cdict['jobs'])

            failures = [job for job in cdict['jobs'] if job.exit_code != 0]
            if len(failures) == 0: continue

            # Generate HTML lines for each cluster
            if job_table_cl_count < config_vals['num_clusters']:  # Limit number of clusters
                                                   # shown in report based on config file

                linemap = ((cid, 'left'), (cdict['userid'], 'right'),
                           (total_jobs, 'right'), (len(failures), 'right'))
                job_table += '\n<tr>' + \
                             ''.join((tdalign(key, al) for key, al in linemap)) + \
                             '<td></td>' * 6 + '</tr>'

            # Generate HTML line for each failed job
                for jcount, job in enumerate(failures):
                    if jcount < config_vals['jobs_per_cluster']:
                        linemap = self._generate_job_linemap(job)
                        job_table += '\n<tr>' + '<td></td>' * 4 + \
                                     ''.join((tdalign(key, al) for key, al in linemap)) + \
                                     '</tr>'
                    else:
                        break

        total_jobs = 0

        site_failed_dict = defaultdict(dict)
        # Compile count of failed jobs, calculate job success rate
        # For Site Details Table and Summary Table
        for site, jobs in self.run.jobs.iteritems():
            failed = 0
            total = len(jobs)
            failures = defaultdict(lambda: defaultdict(int))
            # failures structure:
            # {host1: {exit_code1: count, exit_code2: count},
            # host2: {exit_code1: count, exit_code2: count}, etc.}
            for job in jobs:
                if job.exit_code != 0:
                    failed += 1
                    failures[job.host][job.exit_code] += 1

            total_jobs += total
            total_failed += failed

            jsrate = round((total - failed) * 100. / total, 1)

            table_summary += self._new_table_summary_line(site, total, failed,
                                                          jsrate)

            site_failed_dict[site] = self._init_site_failed_dict(site, total,
                                                                 failed, jsrate)

            for hostcount, (host, errors) in enumerate(
                    sorted(failures.iteritems(),
                           key=lambda x: sum_errors(x[1]),
                           reverse=True)):
                # Sort hosts by total error count in reverse order
                if hostcount < config_vals['num_hosts_per_site']:
                    for errcount, (code, count) in enumerate(
                            sorted(errors.iteritems(),
                                   key=lambda x: x[1],
                                   reverse=True)):
                        # Sort error codes for each host by count in
                        # reverse order
                        if errcount < config_vals['errors_per_host']:
                            site_failed_dict[site]['HTMLLines'] += self._generate_site_detail_line(host, code, count)
                        else:
                            break
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

        # Generate Total line for Site Details Section
        table = ''.join(str(site_failed_dict[site]['HTMLLines']) for site in failkeys)

        linemap = (('Total', 'left'), (total_jobs, 'right'),
                   (total_failed, 'right'),
                   (round((total_jobs - total_failed) * 100. / total_jobs, 1), 'right'))
        table += '\n<tr>' + \
                 ''.join((tdalign(key, al) for key, al in linemap)) + \
                 '<td></td>' * 3 + '</tr>'

        # Generate Total line for Summary Table
        linemap = (('Total', 'left'), (total_jobs, 'right'),
                   (total_failed, 'right'),
                   (round((total_jobs - total_failed) * 100. / total_jobs, 1), 'right'))
        table_summary += '\n<tr>' + ''.join((tdalign(key, al) for key, al in linemap)) + '</tr>'

        # Hide failed jobs table if no failed jobs
        divopen = '\n<div style="display:none">' if total_failed == 0 else ''
        divclose = '\n</div>' if total_failed == 0 else ''

        fifemon_link = self._generate_fifemon_link()

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

        with open(self.template, 'r') as f:
            self.text = f.read()

        self.text = self.text.format(**htmldict)

        return

    def _generate_job_linemap(self, job):
        """
        Generates the HTML alignment "linemap" for the generate_report_file
        method to create an HTML line

        :param job: Instance of Job class
        :return tuple: Line Map (tuple) of (item, alignment) tuples
        """
        jobtimes = namedtuple('jobtimes', ['start', 'end'])

        try:
            job_link_parts = \
                [elt for elt in
                 self._get_job_parts_from_jobid(job.jobid)]
            jt = jobtimes(*(self.parse_datetime(dt, utc=True)
                            for dt in
                            (job.start_time, job.end_time)))

            timestamps_exact = self.get_epoch_stamps_for_grafana(*jt)
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

        try:
            j_out = jobtimes(*(datetime.datetime.strftime(t, "%Y-%m-%d %H:%M:%S")
                           for t in jt))
        except NameError:
            jt = jobtimes(*(self.parse_datetime(dt, utc=True)
                       for dt in
                       (job.start_time, job.end_time)))
            j_out = jobtimes(*(datetime.datetime.strftime(t, "%Y-%m-%d %H:%M:%S")
                           for t in jt))

        linemap = ((job_html, 'left'), (j_out.start, 'left'),
                   (j_out.end, 'left'), (job.site, 'right'),
                   (job.host, 'right'), (job.exit_code, 'right'))

        return linemap

    @staticmethod
    def _new_table_summary_line(site, total, failed, jsrate):
        """
        Creates HTML line that's formed for the Summary Table

        :param str site: Site where jobs ran
        :param int total: Total number of jobs
        :param int failed: Total number of failed jobs
        :param float jsrate: Job success rate for the site
        :return str: HTML line for Summary Table
        """
        return '\n<tr><td align = "left">{0}</td>' \
                             '<td align = "right">{1}</td>' \
                             '<td align = "right">{2}</td>'\
                             '<td align = "right">{3}</td></tr>'\
            .format(site, total, failed, jsrate)

    @staticmethod
    def _init_site_failed_dict(site, total, failed, jsrate):
        """
        Puts the already-generated data into dict form for insertion into
        site_failed_dict

        :param str site: Site where jobs ran
        :param int total: Total number of jobs
        :param int failed: Total number of failed jobs
        :param float jsrate: Job success rate for the site
        :return dict: Dictionary with keys 'FailedJobs', 'HTMLLines'.  The
        latter is an HTML line that will get read out later
        """
        return {'FailedJobs': failed,
                'HTMLLines':
                    '\n<tr><td align = "left">{0}</td>'
                    '<td align = "right">{1}</td>'
                    '<td align = "right">{2}</td>'
                    '<td align = "right">{3}</td>'
                    '<td></td><td></td><td></td></tr>'
                        .format(site, total, failed, jsrate)
                }

    @staticmethod
    def _generate_site_detail_line(host, code, count):
        """
        Creates HTML line representing a site detail line

        :param str host: Which host failed jobs ran on
        :param int code: What the error code was
        :param int count: How many jobs failed with this error code
        :return str: HTML line summarizing this info
        """
        return '\n<tr><td></td><td></td><td></td><td></td>' \
        '<td align = "left">{0}</td>' \
        '<td align = "right">{1}</td>' \
        '<td align = "right">{2}</td></tr>'.format(host, code, count)

    def _generate_fifemon_link(self):
        """Generate fifemon link for User Batch Details page"""
        epoch_stamps = self.get_epoch_stamps_for_grafana()
        elist = [elt for elt in epoch_stamps]
        elist.append('{0}pro'.format(self.vo.lower()))
        fifemon_link_raw = 'https://fifemon.fnal.gov/monitor/dashboard/db/' \
                           'user-batch-history?from={0}&to={1}&' \
                           'var-user={2}'.format(*elist)
        return '<a href="{0}">Fifemon</a>'.format(fifemon_link_raw)

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
        return'<th style="text-align:center">' + \
            '</th><th>'.join(headerlist) + '</th>'

    def _limit_site_check(self):
        """Check to see if the num_failed_sites option is set in the config
        file for the VO"""
        return 'num_failed_sites' in \
               self.config[self.report_type.lower()][self.vo.lower()]


def main():
    args = parse_opts()

    # Set up the configuration
    config = get_configfile(flag='fife', override=args.config)

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
        runerror(config, e, errstring, logfile)
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()