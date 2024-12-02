"""Sample gracc report"""

import traceback
from collections import defaultdict
import sys

from opensearchpy  import Search

from gracc_reporting import ReportUtils 

MAXINT = 2**31 - 1

class SampleReport(ReportUtils.Reporter):
    """Sample report"""

    def __init__(self, config_file, start, end, **kwargs):
        """Initialize class that subclasses ReportUtils.Reporter"""
        report = 'sample'
        super(SampleReport, self).__init__(report_type=report,
                                           config_file=config_file,
                                           start=start,
                                           end=end,
                                           **kwargs)
        self.title = "Sample report"
        self.header = ["OIM_Site", "Core Hours"]

    def run_report(self):
        """What to run when we run the report"""
        self.send_report()

    def query(self):
        """Our query.  We're keeping it simple.  Just get the total number of
        CoreHours for each Site (OIM_Site) from the Payload records over our 
        time range"""

        # GRACC likes iso-formatted dates
        starttimeq = self.start_time.isoformat()
        endtimeq = self.end_time.isoformat()

        if self.verbose:
            print "This is a verbose statement"

        # Query to ES.  Get all records within our time range that are 
        # ResourceType Payload
        s = Search(using=self.client, index=self.indexpattern)\
                .filter("range", EndTime={"gte": starttimeq, "lt": endtimeq}) \
                .filter("term", ResourceType="Payload")[0:0]

        # Create buckets of those records by the OIM_Site
        Bucket = s.aggs.bucket("OIM_Site", "terms", field="OIM_Site", size=MAXINT) 

        # And calculate the sum of the "CoreHours" field for each bucket
        # and display that as "CoreHours"
        Bucket.metric("CoreHours", "sum", field="CoreHours")
        
        return s

    def generate(self):
        """Mine raw data so python can do stuff"""
        results = self.run_query()

        # The results look like this, kind of:
        #  {"OIM_Site": 
        #    {"buckets": [
        #      {"key": "My_site", "CoreHours": {"value": 12345}}, 
        #      {"key": "My_site_2", "CoreHours": {"value":67890}
        #    ]}
        #  }

        for site in results.OIM_Site.buckets:
            yield (site.key, site.CoreHours.value)
            
    def format_report(self):
        """Format report into columns.  We're using this method so we won't 
        need a separate template"""
        report = defaultdict(list)

        for result_tuple in self.generate():
            # Why not
            if self.verbose:
                print result_tuple 

            # There are much more elegant and pythonic ways to do this, but 
            # let's keep it extremely simple.  Each list becomes a column
            report['OIM_Site'].append(result_tuple[0])
            report['Core Hours'].append(result_tuple[1])


        # Let's add a total row to the bottom of the report
        total = sum(report['Core Hours'])
        report['OIM_Site'].append("Total")
        report['Core Hours'].append(total)

        return report


def main():
    args = ReportUtils.get_report_parser().parse_args()

    try:
        # We'll throw a few extra args in here just because
        s = SampleReport(config_file=args.config,
                         start=args.start,
                         end=args.end,
                         verbose=args.verbose,
                         is_test=args.is_test)
        s.run_report()
        print "Yay, it worked!"
        sys.exit(0)
    except Exception as e:
        ReportUtils.runerror(args.config, e, traceback.format_exc())
        sys.exit(1)

if __name__ == '__main__':
    main()
