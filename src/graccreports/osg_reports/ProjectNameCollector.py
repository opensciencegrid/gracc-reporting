"""This module collects project information for various sources OIM, XD DB"""

import sys
import optparse

from XDProject import XDProject

__author__ = "Tanya Levshina"
__email__ = "tlevshin@fnal.gov"


class ProjectNameCollector:
    """Collects and create container of Projects from various sources"""

    def __init__(self, config, verbose=False):
        """ Collects ProjectName information from all available sources (OIM, XD DB).
        Args:
            config(dict)
            verbose(boolean) - debug messages flag
        """
        self.projects = {}
        self.config = config
        self.verbose = verbose
    #     Known projects are stored in a file, name of the file is under project_name csv in configuration
    #     self.cache = self.config.config.get("project_name", "csv")
    #     self.parse()
    #
    # def parse(self):
    #     """Parses the cache file with project names, skip failures"""
    #
    #     try:
    #         lines = open(self.cache).readlines()
    #     except IOError, e:
    #         print >> sys.stderr, e
    #         sys.exit(1)
    #     for l in lines:
    #         # Abbott, Bill, Rutgers; the State University of New Jersey, TG-TRA120035, Training, OSG, 0, 0, 0
    #         try:
    #             tmp = l[:-1].split(",")
    #             p = ProjectName(tmp[3])
    #             p.set_first_name(tmp[1])
    #             p.set_last_name(tmp[0])
    #             p.set_pi("%s %s" % (tmp[0], tmp[1]))
    #             p.set_institution(tmp[2])
    #             p.set_fos(tmp[4])
    #             p.set_id(tmp[6])
    #             self.projects[tmp[3]] = p
    #         except:
    #             print >> sys.stderr, "Can not parse ", l
    #             continue

    def get_project(self, name, source):
        """
        Looks up project.  Currently, we only use it for XD projects, as every
        other project should be in OIM

        :param str name:  The name of the project we're looking up
        :param str source:  The type of lookup we're doing (XD, OSG,
        or OSG-Connect)

        :return: None or XDProject.XDProject object
        """
        """Finds project in XD Database or OIM if it is not in cache
        Args:
            name(str) - name of the project
            source(str) - XD,  OSG or OSG-Connect
        """
        if source == "XD":
            # print name
            if not name.startswith("TG-"):
                return None
        else:   # Redundant check
            if name.startswith("TG-"):
                return None
        # if self.projects.has_key(name):
        #     if self.verbose:
        #         print >> sys.stdout, "Project is in cache ", name
        #     return self.projects[name]

        # # if project is in OIM just get it from there
        # oim = OSGProject(name, self.config)
        # if not oim.execute_query(url_type='project'):
        #     self.projects[name] = oim
        #     # Add new project to cache
        #     self.add_to_cache(self.projects[name])
        #     return self.projects[name]

        # Now, project is not in OIM. If this is OSG project we could not do anything but send notification,
        # if this is XD project we could try XD database
        if source == "XD":
            xd = XDProject(name, self.config, self.verbose)
            if not xd.execute_query(url_type='project'):
                    # Email Mats about this project.  New method
                    print "This project is not in XD database ", name
            else:
                # Put the information in request file, will be sent later
                self.create_request_to_register_oim(name, source, xd)
                # We will still report this project
                self.projects[name] = xd
                return self.projects[name]
        else:
            print "This project %s is not registered " % (name,)
            # Put the information in request file, will be sent later
            self.create_request_to_register_oim(name, source)
        return None


    # def add_to_cache(self, p):
    #     """Adds project and relative info to cache csv file
    #     Args:
    #         p(Project)
    #     """
    #     try:
    #         if self.verbose:
    #                 print >> sys.stdout, "Trying to add project to cache ", p.name
    #         fd = open(self.cache, "a")
    #         fd.write("%s,%s,%s,%s,%s,OSG,%s,0,0\n" % (p.get_last_name().replace(",", ";"), p.get_first_name(),
    #                                                       p.get_institution().replace(", ", ";"),
    #                                                       p.get_project_name().replace(",", ";"),
    #                                                       p.get_fos().replace(",", ";"), p.get_id()))
    #         fd.close()
    #     except:
    #         print >>sys.stderr, "Failed add to cache", self.cache

    def create_request_to_register_oim(self, name, source, p=None, altfile=None):
        """Creates file with information related to project that will be sent later to OSG secretary
        Args:
            name(str) - project name
            source(str) - XD, OSG, or  OSG-Connect"
            p(Project) - project
            altfile(str) - alternative file to write to
        """
        if not altfile:
            filename = "OIM_Project_Name_Request_for_{0}".format(source)
        else:
            filename = altfile

        if source == "XD" and name.startswith("TG-"):
            with open(filename, 'a') as f:
                f.write("****************START****************\n")
                f.write(
                    "ProjectName: {0}\nPI: {1}\nEmail: {2}\nInstitution: {3}"
                    "\nDepartment: {4}\nField of Science: {5}\nDescription: {6}"
                    "\n".format(name, p.get_pi(), p.get_email(),
                                p.get_institution(), p.get_department(),
                                p.get_fos(),p.get_abstract()))
                f.write("****************END****************\n")
        # elif self.no_name(name):
            # Send email to OSG support with record info. Maybe this needs to be in MissingProject.py
            # pass
        else:
            with open(filename, 'a') as f:
                f.write("Project names that are reported from {0} but not "
                        "registered in OIM\n".format(source))
                f.write("ProjectName: {0}\n".format(name))

        return filename

    @staticmethod
    def no_name(name):
        return name == 'N/A' or name.upper() == "UNKNOWN"


def parse_opts():
    """Parses command line options"""

    usage = "Usage: %prog [options] ProjectName"
    parser = optparse.OptionParser(usage)
    parser.add_option("-c", "--config", dest="config", help="report configuration file (required)")
    parser.add_option("-r", "--report", dest="report_type", help="report type: XD,OSG or OSG-Connect",
                      choices=["XD", "OSG", "OSG-Connect", "Duke-Connect"], default="OSG")
    parser.add_option("-v", "--verbose", action="store_true", dest="verbose", default=False,
                      help="print debug messages to stdout")
    opts, args = parser.parse_args()
    if len(args) != 1:
        parser.print_usage()
        sys.exit(1)
    return opts, args

if __name__ == "__main__":
    opts, args = parse_opts()
    pnc = ProjectNameCollector(config, opts.verbose)
    p = pnc.get_project(args[0], opts.report_type)
    print p.__dict__
