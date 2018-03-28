import json
import optparse
import subprocess


class CheckReconstructedTransfers:
    def __init__(self, config, template):
        self.url = config['reconstructed_transfers']['curl']
        cmd = "curl %s" % (self. url,)
        proc = subprocess.Popen(cmd, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        # Reads from pipes, avoides blocking
        result, error = proc.communicate()
        return_code = proc.wait()
        self.access_web = True
        if return_code != 0:
            self.access_web = False
            return
        fts_results = json.loads(result.strip())
        self.fts_status = fts_results['status']['fts']
        self.sam_status = fts_results['status']['sam']
        self.completed = fts_results['counts']['ncompleted']
        self.failed = fts_results['counts']['nfailedtransfer']
        self.errors = fts_results['counts']['nerror']
        self.errors_list = fts_results['errorstates']
        self.failure_list = fts_results['failedtransferstates']
        self.template = template
        self.problem = False

    def dump(self):
        if self.access_web:
            print "The reconstruction transfers results"
            print "Completed: %s, Failed: %s, Errors: %s" % (self.completed, self. failed, self.errors)
            for item in self.errors_list:
                print "FileName: %s, Type: %s" % (item['name'], item['type'])
                print "   Reason: %s" % (item['msg'])
        else:
            print "Failed access web: %s" % self.url

    def update_template(self):
        color_red = "<font color=\"red\"\n>"
        color_green = "<font color=\"green\"\n>"
        color_used = "<font color=\"green\"\n>"
        end_color = "</font>"
        self.template = self.template.replace("$FTS_STATUS", self.access_web and "%sOK%s" %
                                              (color_green, end_color) or "%sDOWN%s" % (color_red, end_color))
        self.template = self.template.replace("$FTS_LEVEL_1_STARTS",  "" if self.access_web else "<!-- ")
        if self.access_web:
            self.template = self.template.replace("$FTS_STATUS",  self.fts_status and "%sOK%s" %
                                                  (color_green, end_color) or "%sDOWN%s" % (color_red, end_color))
            self.template = self.template.replace("$SAM_STATUS", self.sam_status and "%sOK%s" %
                                                  (color_green, end_color) or "%sDOWN%s" % (color_red, end_color))

            self.template = self.template.replace("$FTS_COMPLETED", "%s%s%s"
                                                  % (color_green, self.completed, end_color))
            if self.failed:
                color_used = color_red
                self.problem = True
            self.template = self.template.replace("$FTS_FAILED", "%s%s%s" % (color_used, self.failed, end_color,))
            color_used = color_green
            if self.errors:
                color_used = color_red
                self.problem = True
            self.template = self.template.replace("$FTS_ERROR", "%s%s%s" % (color_used, self.errors, end_color,))
            self.template = self.template.replace("$FTS_LEVEL_2_STARTS", "" if (self.errors or self.failed) else "<!--")
            table_content = ""
            if self.failed:
                print self.failure_list
                for item in self.failure_list:
                    table_content = "%s<tr>\n<td align=\"right\">Failure</td>\n<td align=\"right\">%s</td>\n<td align=\"right\">%s</td>\n<td align=\"right\">%s</td>\n</tr>\n" % \
                                    (table_content, item['name'], item['type'], item['msg'])
            if self.errors:
                for item in self.errors_list:
                    table_content = "%s<tr>\n<td align=\"right\">Error</td>\n<td align=\"right\">%s</td>\n<td align=\"right\">%s</td>\n<td align=\"right\">%s</td>\n</tr>\n" % \
                                    (table_content, item['name'], item['type'], item['msg'])
            self.template = self.template.replace("$TABLE_FTS", table_content)
            self.template = self.template.replace("$FTS_LEVEL_2_ENDS", "" if (self.errors or self.failed) else "-->")
        self.template = self.template.replace("$FTS_LEVEL_1_ENDS", "" if self.access_web else "-->")
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
    config = opts.config
    template = "".join(open(config['common']['template']).readlines())
    crt = CheckReconstructedTransfers(config, template)
    report = open("minerva_report.html", 'w')
    report.write(crt.update_template())
    report.close()
