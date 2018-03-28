""" This module is handling XD project and access OIM and XD accounting database"""

import traceback
import sys
import optparse
from copy import deepcopy

import psycopg2

from ProjectName import ProjectName


__author__ = "Tanya Levshina"
__email__ = "tlevshin@fnal.gov"

# This is how project related information looks in OIM
#                             query_to_xml                              
# ------------------------------------------------------------------------
# <table xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">         +
#                                                                       +
# <row>                                                                 +
#   <person_id>6386</person_id>                                         +
#   <first_name>Lillian</first_name>                                    +
#   <last_name>Chong</last_name>										+
#   <email_address>blah</email_address>                                 +
#   <organization_name>University of Pittsburgh</organization_name>     +
#   <department>Chemistry</department>                                  +
#   <field_of_science_desc>Molecular Biosciences</field_of_science_desc>+
#   <abstract_body>blah blah</abstract_body>							+
# </row>                                                                +
# </table>                                                              


class XDProject(ProjectName):
    """Handles XD Projects

        Queries XD postgres database for a specified ProjectName
        Sets PI, email, Institution, Department, Field of Science and Abstract
        Path psql should be set and hostname, port, password and schema should be correctly defined in configuration 
        file

        """
    
    def __init__(self, name, config, verbose=True):
        """Constructs XDProject
        Args:
            name(str) - name of the project
            config(Configuration) - configuration object
            verbose(boolean) - controls debug messages
        """

        ProjectName.__init__(self, name)
        self.config = config
        self.verbose = verbose

    def get_connection_string(self):
        """Creates a connection string for accessing xd database, set temporarily file with db password."""
        return deepcopy(self.config['xd_db'])

    def execute_query(self, url_type='project'):
        """

        :param url_type:
        :return:
        """


        try:
            print "Trying to run query to XD DB"
            connection = psycopg2.connect(**self.get_connection_string())
            print "Connected to DB"
            cursor = connection.cursor()
            abstract = "n/a"
            cursor.execute("""select distinct p.person_id,first_name,last_name,email_address,organization_name,
            department, f1.field_of_science_desc,abstract_body from acct.people p, acct.allocation_users u,
            acct.accounts  a, acct.principal_investigators pi,acct.organizations o, acct.requests r,
            acct.fields_of_science f ,acct.fields_of_science f1,acct.abstracts_requests abr, acct.abstracts ab,
            acct.email_addresses e  where pi.person_id=p.person_id and p.person_id=u.person_id and
            u.account_id=a.account_id and r.account_id=u.account_id and a.charge_number = %s and
            o.organization_id=p.organization_id and r.request_id=pi.request_id  and
            r.primary_fos_id=f.field_of_science_id and f1.field_of_science_id=f.parent_field_of_science_id and
            r.request_id=abr.request_id and abr.abstract_id=ab.abstract_id and  e.person_id=pi.person_id and
            ab.abstract_body not like %s""" ,(self.name.strip(),abstract))

            rows = cursor.fetchall()
            if len(rows):
                self.set_first_name(rows[0][1])
                self.set_last_name(rows[0][2])
                self.set_pi("%s %s" % (rows[0][1], rows[0][2]))
                self.set_email(rows[0][3])
                self.set_institution(rows[0][4])
                self.set_department(rows[0][5])
                self.set_fos(rows[0][6])
                self.set_abstract(rows[0][7])
                return True
        except:
            print >> sys.stderr, "Failed to extract information from XD database",traceback.print_stack()
            return False

def parse_opts():
        """Parses command line options"""

        usage = "Usage: %prog [options] ProjectName"
        parser = optparse.OptionParser(usage)
        parser.add_option("-c", "--config", dest="config", help="report configuration file (required)")
        parser.add_option("-r", "--report", dest="report_type", help="report type: XD,OSG or OSG-Connect",
                          choices=["XD", "OSG", "OSG-Connect"], default="OSG")
        parser.add_option("-v", "--verbose", action="store_true", dest="verbose", default=False,
                          help="print debug messages to stdout")
        opts, args = parser.parse_args()
        return opts, args


if __name__ == "__main__":
    opts, args = parse_opts()
    xd = XDProject(args[0], args.config)
    xd.set_info_for_projectname()
    print "%s, %s, %s, %s, %s, %s, %s" % (xd.get_pi(), xd.get_email(), xd.get_institution(), xd.get_department(),
                                          xd.get_project_name(), xd.get_fos(), xd.get_abstract())
