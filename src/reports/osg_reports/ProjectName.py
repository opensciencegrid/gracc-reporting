"""This module defines ProjectName class"""

import subprocess
import sys


class ProjectName:
    """Base class for the OSG Projects"""

    def __init__(self, name, verbose=False):
        """Creates Project for the OSG Projects
        Args:
            name(str) - project name"""
        self.name = name
        self.last_name = None
        self.first_name = None
        self.pi = None
        self.fos = None
        self.institution = None
        self.department = None
        self.clusters = {}
        self.abstract = None
        self.email = None
        self.verbose = verbose
        self.oimid = 0
        self.dom = None

    def execute_cmd(self, cmd):
        """Executes shell command
        Args:
            cmd(str) - shell command
        """
        if self.verbose:
            print >> sys.stdout, cmd
        proc = subprocess.Popen(cmd, shell=True,  stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        # Reads from pipes, avoides blocking
        result, error = proc.communicate()
        return_code = proc.wait()
        if self.verbose:
            print >> sys.stdout, result
            print >> sys.stderr, error
            print >> sys.stdout, "command return code is %s" % (return_code, )
        return result.strip().split("\n"), return_code

    def execute_query(self, url_type=None):
        """
        Abstract class to execute query against some database
        :param url_type:
        :return:
        """
        pass

    @staticmethod
    def get_value(lst):
        """get Value from the node of DOM docs
        Args:
            lst(list of Node)
        """

        tmp = []
        for node in lst:
            if node.nodeType == node.TEXT_NODE:
                tmp.append(node.data)
        value = ' '.join(tmp)
        return value

    def get_pi(self):
        """getter PI"""

        return self.pi

    def get_institution(self):
        """getter Institution"""

        return self.institution

    def get_department(self):
        """getter Department"""

        return self.department

    def get_fos(self):
        """getter FOS"""

        return self.fos

    def get_abstract(self):
        """getter Abstract"""

        return self.abstract

    def get_project_name(self):
        """getter ProjectName"""

        return self.name

    def get_last_name(self):
        """getter LastName"""

        return self.last_name

    def get_first_name(self):
        """getter FirstName"""

        return self.first_name

    def get_email(self):
        """getter Email"""

        return self.email

    def get_id(self):
        """getter id"""
        return self.oimid

    def set_info_for_projectname(self):
        """Queries and parse project related info"""

        try:
            return self.execute_query()
        except:
            print >> sys.stderr, sys.exc_info()[0]
            return 1

    def set_pi(self, pi = None):
        self.pi = pi

    def set_institution(self, institution=None):
        self.institution = institution

    def set_fos(self, fos=None):
        self.fos = fos

    def set_last_name(self, ln):
        self.last_name = ln

    def set_first_name(self, nm):
        self.first_name = nm

    def set_usage(self, h, wh, jn):
        """setter Usage cpu,wall hours and num of jobs
        Args:
            h(float) - cpu hours
            wh(float) - wall hours
            jn(int) - number of jobs"""

        self.clusters[h] = [jn, jn, wh, wh]

    def set_department(self, department=None):
        self.department = department

    def set_abstract(self, abstract=None):
        self.absract = abstract

    def set_email(self, email=None):
        self.email = email

    def set_id(self, oimid=0):
        """setter ID
        Args:
            oimid(int) - OIM project number
        """
        self.oimid = oimid

    def set_delta_usage(self, h, wh, jn):
        """setter Delta
        Args:
                h(float) - cpu hours
                wh(float) - wall hours
                jn(int) - number of jobs"""

        if self.clusters.has_key(h):
            v = self.clusters[h]
        else:
            v = [0, 0, 0, 0]
        t = [v[0], v[0]-jn, v[2], v[2]-wh]
        self.clusters[h] = t

    def get_usage_total(self):
        """Calucalte and returns total usage in all the clusters"""

        usage = [0, 0, 0, 0]
        for v in self.clusters.values():
            usage[0] = usage[0]+v[0]
            usage[1] = usage[1]+v[1]
            usage[2] = usage[2]+v[2]
            usage[3] = usage[3]+v[3]
        return usage

    def get_clusters(self):
        """getter cluster"""
        return self.clusters
