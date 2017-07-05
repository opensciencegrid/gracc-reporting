import json
import optparse
import subprocess


class CPNLocks:
    def __init__(self,config,template):
        self.url, self.limit = config['cpn_locks']['curl'], config['cpn_locks']['limit']
        cmd = "curl -k \'%s\'" % (self.url)
        proc = subprocess.Popen(cmd,shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        # Reads from pipes, avoides blocking
        result, error=proc.communicate()
        return_code = proc.wait()
        self.access_fifemon = True
        if return_code != 0:
            self.access_fifemon = False
            return
        cpn_results = json.loads(result.strip())
        self.datapoints=cpn_results[0]['datapoints']
        try:
            self.num_locks = int(self.datapoints[-1][0])
        except:
            self.num_locks = 0
        self.should_check = False
        self.duration = 5
        if self.num_locks >= self.limit:
            self.should_check = True
            for entry in reversed(self.datapoints[-1:]):
                if entry[0]:
                    num_locks=int(entry[0])
                    if num_locks < self.limit:
                        self.should_check = False
                        break
                    self.duration = self.duration + 5
        self.template = template


    def update_template(self):
        color_red="<font color=\"red\">"
        color_black="<font color=\"black\">"
        end_color="</font>"
        table_content=""
        self.template = self.template.replace("$CPNLOCKS_FIFEMON_ACCESS", "" if self.access_fifemon else
                                              "FIFEMON_ACCESS: %sDOWN%s" % (color_red,end_color) )
        self.template = self.template.replace("$CPNLOCKS_LEVEL_1_STARTS", "" if self.access_fifemon else "<!-- ")
        if self.access_fifemon:
            if self.should_check:
                color_used = color_red
            else:
                color_used = color_black
            table_content = "%s<tr><td align=\"left\">all</td><td align=\"right\">%s%s%s</td><td align=\"right\">%s%s%s</td></tr>" % \
                                    (table_content,color_used,self.num_locks,end_color,color_used,self.duration,end_color)
            self.template = self.template.replace("$TABLE_CPNLOCKS",table_content)
        self.template = self.template.replace("$CPNLOCKS_LEVEL_1_ENDS", "" if self.access_fifemon else "-->")
        return self.template





def parse_opts():
        """Parses command line options"""

        usage="Usage: %prog [options]"
        parser = optparse.OptionParser(usage)
        parser.add_option("-c", "--config", dest="config",type="string",
                  help="report configuration file (required)")
        opts, args = parser.parse_args()
        return opts, args

if __name__ == '__main__':
    opts, args = parse_opts()
    config = opts.config
    template = "".join(open(config['common']['template']).readlines())
    crt = CPNLocks(config,template)
    report=open("minerva_report.html",'w')
    report.write(crt.update_template())
    report.close()