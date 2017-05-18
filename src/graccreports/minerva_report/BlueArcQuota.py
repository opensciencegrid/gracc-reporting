"""
This module is dealing with BlueArc partition usage by an experiment. It compares the usage vs the limit defined in
configuration file and updated the relevant section of a report in html format. If the usage exceeds the limit, it would
displayed in red. The information is pulled from graphite.
"""

import json
import optparse
import subprocess

from . import Configuration


class BlueArcQuota:
    def __init__(self, config, template):
        self.url = (config.get("blue_arc", "curl"))
        cmd = "curl -k \'%s\'" % (self.url,)
        proc = subprocess.Popen(cmd, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        result, error = proc.communicate()
        return_code = proc.wait()
        self.access_fifemon = True
        if return_code != 0:
            self.access_fifemon = False
            return
        quota_results = json.loads(result.strip())
        self.quotas = {}
        for quota in quota_results:
            target = quota['target'].replace("-p-g-", "").replace("fs.bluearc.exp.minerva.", "")
            if not quota["datapoints"][0][0]:
                quota["datapoints"][0][0] = "0"
            self.quotas[target] = int(quota["datapoints"][0][0])
        self.template = template
        self.limit = int(config.get("blue_arc", "limit"))
        self.problem = False


    def update_template(self):
        color_red = "<font color=\"red\">"
        color_black = "<font color=\"black\">"
        end_color = "</font>"
        table_content = ""
        self.template = self.template.replace("$BLUEARC_FIFEMON_ACCESS",
                                              "" if self.access_fifemon else "FIFEMON_ACCESS: %sDOWN%s" % (
                                                  color_red, end_color))
        self.template = self.template.replace("$BLUEARC_LEVEL_1_STARTS", "" if self.access_fifemon else "<!-- ")
        if self.access_fifemon:
            for key, value in self.quotas.items():
                if value >= self.limit:
                    color_used = color_red
                    self.problem = True 
                else:
                    color_used = color_black
                    self.problem = False 
                table_content = "%s<tr><td align=\"left\">%s%s%s</td><td align=\"right\">%s%s%s</td></tr>" % \
                                (table_content, color_used, key, end_color, color_used, value, end_color)
            self.template = self.template.replace("$TABLE_BLUEARC", table_content)
        self.template = self.template.replace("$BLUEARC_LEVEL_1_ENDS", "" if self.access_fifemon else "-->")
        return self.template

    def has_problem(self):
        return self.problem



def parse_opts():
    """Parses command line options"""

    usage = "Usage: %prog [options]"
    parser = optparse.OptionParser(usage)
    parser.add_option("-c", "--config", dest="config", type="string", help="report configuration file (required)")
    opts, args = parser.parse_args()
    return opts, args


if __name__ == '__main__':
    opts, args = parse_opts()
    config = Configuration.Configuration()
    config.configure(opts.config)
    template = "".join(open(config.config.get("common", "template")).readlines())
    crt = BlueArcQuota(config.config, template)
    report = open(config.config.get("common", "report"), 'w')
    report.write(crt.update_template())
    report.close()
