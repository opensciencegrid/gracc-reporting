import json
import optparse
# import Configuration
import subprocess

from . import Configuration

class Job:

    def __init__(self,jtype):
        self.jtype = jtype
        self.totals = []

    def add_job_count(self, njobs):
        if njobs:
            self.totals.append(int(njobs))

    def get_average_jobs(self):
        sum = 0.0
        for eff in self.totals:
            sum = sum+eff
        return int(sum/len(self.totals))

class CurrentJobs:
    def __init__(self,config,template):
        self.access_fifemon = True
        self.jobs = {}
        self.add_job("current",json.loads(self.query_graphite(config.get("jobs","current_jobs_curl")))[0])
        self.add_job("idle",json.loads(self.query_graphite(config.get("jobs","idle_jobs_curl")))[0])
        self.add_job("held",json.loads(self.query_graphite(config.get("jobs","held_jobs_curl")))[0])
        self.limit = int(config.get("jobs","slot_quota"))
        self.held_limit = int(config.get("jobs","held_limit"))
        self.template = template
        self.should_check_held = False
        self.should_check_running = False

    def add_job(self,jtype,item):
        job = Job(jtype)
        for count in item["datapoints"]:
            job.add_job_count(count[0])
        if len(job.totals):
            self.jobs[jtype] = job

    def query_graphite(self,url):
        cmd = "curl -k \'%s\'" % (url)
        proc = subprocess.Popen(cmd,shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        # Reads from pipes, avoides blocking
        result, error = proc.communicate()
        return_code = proc.wait()
        if return_code != 0:
            self.access_fifemon = False
            return None
        return result

    def update_template(self):
        color_red = "<font color=\"red\">"
        color_black = "<font color=\"black\">"
        end_color = "</font>"
        table_content = ""
        self.template = self.template.replace("$CURRENT_JOBS_FIFEMON_ACCESS", "" if self.access_fifemon else
                                              "FIFEMON_ACCESS: %sDOWN%s" % (color_red, end_color) )
        self.template = self.template.replace("$CURRENT_JOBS_LEVEL_1_STARTS", "" if self.access_fifemon else "<!-- ")
        if self.access_fifemon:
            current = self.jobs['current'].get_average_jobs()
            idle = self.jobs['idle'].get_average_jobs()
            held = self.jobs['held'].get_average_jobs()
            if current < self.limit*0.75 and current < idle/2.:
                color_used = color_red
            else:
                color_used = color_black
            if held > self.held_limit:
                color_held = color_red
            else:
                color_held = color_black
            current = self.jobs['current'].get_average_jobs()
            idle = self.jobs['idle'].get_average_jobs()
            held = self.jobs['held'].get_average_jobs()
            table_content = "%s<tr><td align=\"right\">%s%s%s</td><td align=\"right\">" \
                            "%s%s%s</td><td align=\"right\">%s%s%s</td></tr>" % \
                            (table_content, color_used, current, end_color, color_used, idle, end_color,
                             color_held, held, end_color)
            self.template = self.template.replace("$TABLE_CURRENT_JOBS", table_content)
        self.template = self.template.replace("$CURRENT_JOBS_LEVEL_1_ENDS", "" if self.access_fifemon else "-->")
        return self.template





def parse_opts():
        """Parses command line options"""

        usage = "Usage: %prog [options]"
        parser = optparse.OptionParser(usage)
        parser.add_option("-c", "--config", dest="config", type="string",
                  help="report configuration file (required)")
        opts, args = parser.parse_args()
        return opts, args

if __name__ == '__main__':
    opts, args = parse_opts()
    print opts
    config = Configuration.Configuration()
    config.configure(opts.config)
    template = "".join(open(config.config.get("common", "template")).readlines())
    cjobs = CurrentJobs(config.config, template)
    report = open("minerva_report.html", 'w')
    report.write(cjobs.update_template())
    report.close()
