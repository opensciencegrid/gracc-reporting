import traceback
import sys
import datetime

from elasticsearch_dsl import Search

from . import Reporter, runerror, get_configfile, get_template, Configuration

logfile = 'osgpersitereport.log'
default_templatefile = 'template_persite.html'
opp_vos = ['glow', 'gluex', 'hcc', 'osg', 'sbgrid']


# Helper Functions
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


def monthrange(date):
    """
    Takes a start date and finds out the start and end of the month that that
    input date is in

    :param list date: Pass in date list (as returned by TimeUtils.dateparse) or
    datetime.datetime object.
    :return tuple:datetime.datetime objects that span the month
    (2016-12-05 --> 2016-12-01, 2016-12-31)
    """
    if isinstance(date, list):
        startdate = datetime.datetime(*date)
    elif isinstance(date, datetime.datetime) or \
            isinstance(date, datetime.date):
        startdate = date
    else:
        print "monthrange will only work with a datelist or a " \
              "datetime.datetime object"
        sys.exit(1)
    start = startdate.replace(day=1)
    nextmonth_first = (startdate.replace(day=28) +
                       datetime.timedelta(days=4)).replace(day=1)
    end = nextmonth_first - datetime.timedelta(days=1)
    return start, end


def prev_month_shift(date):
    """
    Take the date passed in, go back a month, and return the range of that
    previous month
    (2016-12-05 --> 2016-11-01, 2016-11-30)

    :param datetime.datetime date: datetime.datetime object
    :return tuple: Consists of two datetime.datetime
    objects that span the range of the _previous_ month
    """
    sd = date.replace(day=1)-datetime.timedelta(days=1)
    return monthrange(sd)


def perc(num, den):
    """
    Converts fraction to percent

    :param float num: Numerator of fraction
    :param float den: Denominator of fraction
    :return float: Returns fraction converted to percentage
    """
    try:
        return float(num/den) * 100.
    except ZeroDivisionError:
        if num == 0:
            return 0.
        else:
            raise("Error: Tried to do division of {0}/{1}".format(num, den))


def perc_change(old, new):
    """Calculates the percentage change between two numbers

    :param float old: Baseline number
    :param float new: Changed number
    :return float: Percentage change from old to new
    """
    try:
        return perc(new - old, old)
    except:
        if new != 0:
            return 100.     # If we have something like (10-0) / 0, return 100%


@Reporter.init_reporter_parser
def parse_opts(parser):
    """
    Don't need to add any options to Reporter.parse_opts
    """
    pass


class VO(object):
    """
    Class to hold VO information

    :param str voname: Name of VO
    """
    def __init__(self, voname):
        self.name = voname
        self.sites = {}
        self.current = True
        self.pos = 0
        self.total = [0,0]  # Position 0 = Current, Position 1 = Past
        self.totalcalc = self.running_totalhours()

    def add_site(self, sitename, corehours):
        """Add a new site data to the VO.  Also updates the core hours of
         sitename and the total for the VO

         :param str sitename: Name of site a VO's job set ran on
         :param float corehours: Core hours in the summary record
         """
        if not self.current: self.pos = 1

        if sitename in self.sites:
            self.sites[sitename][self.pos] += corehours
        else:   # Initialize the site
            self.sites[sitename] = [0, 0]
            self.sites[sitename][self.pos] = corehours

        self.totalcalc.send(corehours)
        return

    def get_cur_sitehours(self, sitename):
        """Get the current value of core hours for a particular sitename for
        the VO"""
        return self.sites[sitename][0]

    def get_cur_totalhours(self):
        """Get the current value of total core hours for the VO"""
        return self.total[0]

    def get_old_sitehours(self, sitename):
        """Get the previous month's value of core hours for a particular
        sitename for the VO"""
        if sitename in self.sites:
            return self.sites[sitename][1]
        else:
            return 0

    def get_old_totalhours(self):
        """Get the previous month's value of total core hours for the VO"""
        return self.total[1]

    @coroutine
    def running_totalhours(self):
        """Keeps running total of core hours for the VO"""
        while True:
            newhrs = yield
            self.total[self.pos] += newhrs


class OSGPerSiteReporter(Reporter):
    """Class to store information and perform actions for the OSG Per Site
    Report

    :param Configuration.Configuration config: Report Configuration object
    :param str start: Start time of report range
    :param str end: End time of report range
    :param str template: Filename of HTML template to generate report
    :param bool verbose: Verbose flag
    :param bool is_test: Whether or not this is a test run.
    :param bool no_email: If true, don't actually send the email
    """
    def __init__(self, config, start, end, template=False, verbose=False,
                 is_test=False, no_email=False, ov_logfile=None):
        report = 'siteusage'

        if ov_logfile:
            rlogfile = ov_logfile
            logfile_override = True
        else:
            rlogfile = logfile
            logfile_override = False

        Reporter.__init__(self, report, config, start, end=end,
                          verbose=verbose, is_test=is_test, no_email=no_email,
                          template=template, logfile=rlogfile,
                          logfile_override=logfile_override)
        self.header = ["Site", "Total", "Opportunistic Total",
                       "Percent Opportunistic", "Prev. Month Opp. Total",
                       "Percentage Change Month-Month"]
        self.start_time, self.end_time = \
            monthrange(self.dateparse(self.start_time))
        self.title = 'VOs Usage of OSG Sites: {0} - {1}'.format(
            self.start_time, self.end_time)
        self.current = True
        self.vodict = {}
        self.sitelist = []

    def run_report(self):
        """Method to run the OSG per site report"""
        self.generate()
        self.send_report(title=self.title)
        return

    def query(self):
        """Method to query Elasticsearch cluster for Flocking Report
        information

        :return elasticsearch_dsl.Search: Search object containing ES query
        """
        startdate = self.dateparse_to_iso(self.start_time)
        enddate = self.dateparse_to_iso(self.end_time)

        if self.verbose:
            self.logger.info(self.indexpattern)

        s = Search(using=self.client, index=self.indexpattern) \
            .filter("range", EndTime={"gte": startdate, "lt": enddate})\
            .filter('term', ResourceType="Batch")

        # Note:  Using ?: operator in painless language to coalesce the
        # 'OIM_Site' and 'SiteName' fields.
        s.aggs.bucket('vo_bucket', 'terms', field='VOName', size=2**31-1) \
            .bucket('site_bucket', 'terms',
                    script={"inline": "doc['OIM_Site'].value ?: doc['SiteName'].value", "lang": "painless"},
                    size=2**31-1) \
            .metric('sum_core_hours', 'sum', field='CoreHours')

        return s

    def generate(self):
        """Higher-level method to run other methods to
        generate the raw data for the report."""
        consumer = self._create_vo_objects()

        # Run our query twice - once for this month, once for last month
        for self.start_time, self.end_time in (
                monthrange(self.start_time),
                prev_month_shift(self.start_time)):
            results = self.run_query()
            self._parse_results(results, consumer)
            self.current = False

        return

    @coroutine
    def _create_vo_objects(self):
        """Coroutine to create the VO objects and store the information
        in them"""
        while True:
            vo, site, wallhrs = yield
            if self.current:
                if vo not in self.vodict:
                    V = VO(vo)
                    self.vodict[vo] = V
                V.add_site(site, wallhrs)

                if site not in self.sitelist:
                    self.sitelist.append(site)
            else:
                if vo not in self.vodict or site not in self.sitelist:
                    continue

                V = self.vodict[vo]
                V.current = self.current
                V.add_site(site, wallhrs)

    @staticmethod
    def _parse_results(results, consumer):
        """Method that parses the result and passes the values to the
        consumer coroutine
        :param Response.aggregations results: ES Response object from ES query
        :param function consumer: Consumer function to pass data to
        """
        for vo_bucket in results.vo_bucket.buckets:
            vo = vo_bucket['key'].lower()
            for site_bucket in vo_bucket.site_bucket.buckets:
                site = site_bucket['key']
                wallhrs = site_bucket['sum_core_hours']['value']
                consumer.send((vo, site, wallhrs))
        return

    def format_report(self):
        """Report formatter.  Returns a dictionary called report containing the
        columns of the report.

        Note:  Each column here is a VO (or an aggregating column like Total)

        :return dict: Constructed dict of report information for
        Reporter.send_report to send report from
        """
        report = {}
        sitelist = sorted(self.sitelist)

        # Populate Site column
        report["Site"] = [site for site in sitelist]

        # Add VO Data to the report
        inspos = 2
        for vo, vo_object in sorted(self.vodict.iteritems()):
            if vo not in self.header:
                if vo in opp_vos:
                    # Insert the opportunistic VOs into the header
                    # in order after the Site and Total columns
                    self.header.insert(inspos, vo)
                    inspos += 1
                else:
                    # Tack the other VO columns to the end
                    self.header.append(vo)

            report[vo] = [vo_object.get_cur_sitehours(site)
                          if site in vo_object.sites else 0
                          for site in sitelist]
            report[vo].append(vo_object.get_cur_totalhours())

        # Column for Previous Month Opportunistic Total
        report["Prev. Month Opp. Total"] = [
            sum((self.vodict[col].get_old_sitehours(site)
                 for col in report if col in opp_vos))
            for site in sitelist]
        stagecol = report["Prev. Month Opp. Total"]
        stagecol.append(sum(stagecol))  # Append the total for this column

        report["Site"].append("Total")  # Add "Total" line at bottom of report

        # This is the per-site total column, not the same "Total" as just above
        # Add all of the data for every vo; do this for each site
        report["Total"] = [sum((report[col][pos] for col in report
                                if col not in
                                ("Site", "Total", "Percentage Change Month-Month",
                                 "Prev. Month Opp. Total")))
                           for pos in range(len(report["Site"]))]

        # Do the same as above, but for only the opp. VOs in the report
        report["Opportunistic Total"] = [sum((report[col][pos]
                                              for col in report
                                              if col in opp_vos))
                                         for pos in range(len(report["Site"]))]

        # Calculate the percent opportunistic usage from the above two columns
        report["Percent Opportunistic"] = [perc(report["Opportunistic Total"][pos],
                                                report["Total"][pos])
                                           for pos in
                                           range(len(report["Site"]))]

        # Percent Change Month-Month for opportunistic VOs
        report["Percentage Change Month-Month"] = [
            perc_change(report["Prev. Month Opp. Total"][pos],
                        report["Opportunistic Total"][pos])
            for pos in range(len(report["Site"]))]

        # In any modifications, the order of the next four sections must be
        # preserved.  They all have to do with handling the previous month's
        # data

        # Previous month totals and percent change by VO
        for col, values in report.iteritems():
            if col == "Site":
                values.append("Prev. Month Total")
                values.append("Percent Change over Prev. Month")
            elif col in self.vodict:
                values.append(self.vodict[col].get_old_totalhours())    # Total
                values.append(perc_change(values[-1], values[-2]))    # Percent change

        # Handle opportunistic total column for the previous month
        stagecol = report['Opportunistic Total']
        stagecol.append(sum((report[vo][-2] for vo in report
                             if vo in opp_vos)))
        stagecol.append(perc_change(stagecol[-1], stagecol[-2]))

        # Handle total column for the previous month
        stagecol = report['Total']
        stagecol.append(sum((report[vo][-2] for vo in self.vodict)))
        stagecol.append(perc_change(stagecol[-1], stagecol[-2]))

        # Handle percent opportunistic overall for previous month
        report['Percent Opportunistic'].extend(
            ((report["Opportunistic Total"][-2] /
              report["Total"][-2] * 100), 'N/A'))

        # Fill in the rest of the report
        report["Prev. Month Opp. Total"].extend(('N/A', 'N/A'))
        report["Percentage Change Month-Month"].extend(('N/A', 'N/A'))

        # Insert a blank line
        for values in report.itervalues():
            values.insert(-3, '')

        return report


def main():
    args = parse_opts()

    # Set up the configuration
    config = Configuration.Configuration()
    config.configure(get_configfile(override=args.config))

    templatefile = get_template(override=args.template, deffile=default_templatefile)

    try:
        osgreport = OSGPerSiteReporter(config,
                                       args.start,
                                       args.end,
                                       template=templatefile,
                                       verbose=args.verbose,
                                       is_test=args.is_test,
                                       no_email=args.no_email,
                                       ov_logfile=args.logfile)

        osgreport.run_report()
        print 'OSG Per Site Report Execution finished'
        sys.exit(0)
    except Exception as e:
        with open(logfile, 'a') as f:
            f.write(traceback.format_exc())
        print >> sys.stderr, traceback.format_exc()
        runerror(config, e, traceback.format_exc())
        sys.exit(1)


if __name__ == '__main__':
    main()
