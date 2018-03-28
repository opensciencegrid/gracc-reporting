"""
MinervaReport is a report generated for MINERvA off-line production group.
The daily report is sent to mailing list specified in configuration. It provides information about current, idle, and
pending jobs, inefficient users, number of cpn locks, usage of blue arc area and problem with transfers to dcache
"""

import optparse
import time
import sys
import traceback

from . import TextUtils, Reporter

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
        config = Reporter._parse_config(opts.config)
        template = open(config['common']['template']).read()
        template = template.replace("$START", time.ctime())
        cjobs = CurrentJobs(config, template)
        template = cjobs.update_template()
        ejobs = UserWastedTime(config, template)
        template = ejobs.update_template()
        cpn = CPNLocks(config, template)
        template = cpn.update_template()
        crt = CheckReconstructedTransfers(config, template)
        template = crt.update_template()
        bluearc = BlueArcQuota(config, template)
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
            emails = config['email']['test_to']
            if not opts.is_test:
                emails.extend(config['email']['minerva_email'])
            print emails
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
