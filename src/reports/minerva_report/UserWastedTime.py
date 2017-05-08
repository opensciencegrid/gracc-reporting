import json
import optparse
import time
# import Configuration
import subprocess
#import numpy as np

from . import Configuration

class User:

    def __init__(self,name):
        self.name = name
        self.wasted = []
        self.duration = []
    def add_data_point(self,cur_wasted,dt):
        if cur_wasted:
            self.wasted.append((float(cur_wasted)/60.,int(dt)))
    def get_worst_wasted(self):
        max_wasted = 0
	dt=time.time()
        for item in self.wasted:
            if max_wasted < item[0]:
                max_wasted = item[0]
                dt = item[1]
        return max_wasted,time.ctime(dt)

    def get_average_duration(self):
        #return np.mean(self.duration)
	if  len(self.duration):
            return  reduce(lambda x, y: x + y, self.duration) / len(self.duration)
        else:
            return 0

class UserWastedTime:

    def __init__(self,config,template):
        self.url = (config.get("wasted_time", "wasted_curl"))
        self.limit = int(config.get("wasted_time", "limit"))
        self.average_jobs_url = (config.get("wasted_time", "duration_curl"))
        cmd = "curl -k \'%s\'" % (self.url)
        proc = subprocess.Popen(cmd,shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        # Reads from pipes, avoides blocking
        result, error=proc.communicate()
        return_code = proc.wait()
        self.access_fifemon = True
        if return_code != 0:
            self.access_fifemon = False
            return
        wasted_results = json.loads(result.strip())
        self.users={}
        for item in wasted_results:
            name = item['target'].replace("fifebatch.jobs.experiments.minerva.users.","").replace(".running.totals.wastetime_avg","")
            user = User(name)
            for waste in item["datapoints"]:
                user.add_data_point(waste[0],waste[1])
            if len(user.wasted):
                user.duration = self.get_jobs_walltime(self.average_jobs_url.replace("$USER", name))
                self.users[name] = user
        self.template = template

    def get_jobs_walltime(self, url):
        cmd = "curl -k \'%s\'" % (url)
        proc = subprocess.Popen(cmd, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        # Reads from pipes, avoides blocking
        result, error=proc.communicate()
        return_code = proc.wait()
        self.access_fifemon = True
        if return_code != 0:
            self.access_fifemon = False
            return
        job_results  = json.loads(result.strip())[0]["datapoints"]
        cmd = "curl -k \'%s\'" % (url.replace("walltime", "count"))
        proc = subprocess.Popen(cmd, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        # Reads from pipes, avoides blocking
        result, error=proc.communicate()
        return_code = proc.wait()
        self.access_fifemon = True
        if return_code != 0:
            self.access_fifemon = False
            return
        count_results = json.loads(result.strip())[0]["datapoints"]
        duration = []
        i = 0
        for item in job_results:
            count=count_results[i][0]
            i += 1
            if item[0] <=0:
                continue
            duration.append(item[0]/count)
        return duration

    def update_template(self):
        color_red = "<font color=\"red\">"
        color_black = "<font color=\"black\">"
        end_color = "</font>"
        table_content = ""
        self.template = self.template.replace("$JOB_WASTEDTIME_FIFEMON_ACCESS", "" if self.access_fifemon else
                                              "FIFEMON_ACCESS: %sDOWN%s" % (color_red, end_color) )
        self.template = self.template.replace("$JOB_WASTEDTIME_LEVEL_1_STARTS", "" if self.access_fifemon else "<!-- ")

        if self.access_fifemon:
            for key, user in self.users.items():
                wasted, dt = user.get_worst_wasted()
                if wasted > self.limit:
                    color_used = color_red
                else:
                    color_used = color_black
                table_content = "%s<tr><td align=\"left\">%s%s%s</td><td align=\"right\">" \
                            "%s%s%s</td><td align=\"right\">%s%s%s</td><td align=\"right\">%s%s%s</td></tr>\n" % \
                            (table_content, color_used, key, end_color, color_used, round(wasted, 1), end_color,
                             color_used, dt, end_color,color_used, round(user.get_average_duration()/3600.,1), end_color)
            self.template = self.template.replace("$TABLE_JOB_WASTEDTIME", table_content)
        self.template = self.template.replace("$JOB_WASTEDTIME_LEVEL_1_ENDS", "" if self.access_fifemon else "-->")
        return self.template





def parse_opts():
        """Parses command line options"""

        usage="Usage: %prog [options]"
        parser = optparse.OptionParser(usage)
        parser.add_option("-c", "--config", dest="config", type="string",
                  help="report configuration file (required)")
        opts, args = parser.parse_args()
        return opts, args

if __name__ == '__main__':
    opts, args = parse_opts()
    config=Configuration.Configuration()
    config.configure(opts.config)
    template = "".join(open(config.config.get("common", "template")).readlines())
    ejobs = UserWastedTime(config.config, template)
    report = open("minerva_report.html", 'w')
    report.write(ejobs.update_template())
    report.close()
