"""
MinervaReport is a report generated for MINERvA off-line production group.
The daily report is sent to mailing list specified in configuration. It provides information about current, idle, and
pending jobs, inefficient users, number of cpn locks, usage of blue arc area and problem with transfers to dcache
"""

import optparse
import time
import sys
import traceback

from . import Configuration, TextUtils

from CheckReconstructedTransfers import CheckReconstructedTransfers
from BlueArcQuota import BlueArcQuota
from CPNLocks import CPNLocks
from CurrentJobs import CurrentJobs
from UserWastedTime import UserWastedTime


def parse_opts():
    """Parses command line options"""

    usage = "Usage: %prog [options]"
    parser = optparse.OptionParser(usage)
    parser.add_option("-c", "--config", dest="config", type="string", help="report configuration file (required)")
    parser.add_option("-d", "--dryrun", action="store_true", dest="is_test", default=False,
                      help="send emails only to _testers")
    parser.add_option("-a", "--alarm", action="store_true", dest="is_alarm", default=False,
                      help="send emails only about bluearc or transfer problems")
    opts, args = parser.parse_args()
    return opts, args


def main():
    opts, args = parse_opts()

    if opts.is_test:
        print "Running in test mode"

    try:
        config = Configuration.Configuration()
        config.configure(opts.config)
        template = "".join(
            open(config.config.get("common", "template")).readlines())
        template = template.replace("$START", time.ctime())
        cjobs = CurrentJobs(config.config, template)
        template = cjobs.update_template()
        ejobs = UserWastedTime(config.config, template)
        template = ejobs.update_template()
        cpn = CPNLocks(config.config, template)
        template = cpn.update_template()
        crt = CheckReconstructedTransfers(config.config, template)
        template = crt.update_template()
        bluearc = BlueArcQuota(config.config, template)
        template = bluearc.update_template()
        found = False
        if not opts.is_alarm:
            found = True
            template = template.replace("$IGNORE1_STARTS", "")
            template = template.replace("$IGNORE3_STARTS", "")
            template = template.replace("$IGNORE1_ENDS", "")
            template = template.replace("$IGNORE3_ENDS", "")
            template = template.replace("$IGNORE2_STARTS", "")
            template = template.replace("$IGNORE2_ENDS", "")
        else:
            template = template.replace("$IGNORE1_STARTS", "<!--")
            template = template.replace("$IGNORE1_ENDS", "-->")
            if not bluearc.has_problem():
                template = template.replace("$IGNORE2_STARTS", "<!--")
                template = template.replace("$IGNORE2_ENDS", "-->")
            else:
                template = template.replace("$IGNORE2_STARTS", "")
                template = template.replace("$IGNORE2_ENDS", "")
                found = True

            if not crt.has_problem():
                template = template.replace("$IGNORE3_STARTS", "<!--")
                template = template.replace("$IGNORE3_ENDS", "")
            else:
                template = template.replace("$IGNORE3_STARTS", "")
                template = template.replace("$IGNORE3_ENDS", "")
                found = True
        if found:
            if opts.is_test:
                emails = config.config.get("email", "test_to").split(", ")
            else:
                emails = config.config.get("email", "minerva_email").split(
                    ", ") + \
                         config.config.get("email", "test_to").split(", ")
            TextUtils.sendEmail(([], emails), "MINERvA Report %s" %
                                (time.ctime()), {"html": template},
                                ("Gratia Operation", "tlevshin@fnal.gov"),
                                "smtp.fnal.gov")
        print "Minerva Report run successful"
    except:
        print >> sys.stderr, traceback.format_exc()
        sys.exit(1)
    sys.exit(0)


if __name__ == '__main__':
    main()
