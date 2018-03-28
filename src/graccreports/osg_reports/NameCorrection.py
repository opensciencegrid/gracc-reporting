from xml.etree import ElementTree as ET
import re

import requests

mwt2info = {}


class MWT2Correction(object):
    """
    Class to get and return Resource Group MWT2 information
    """
    mwt2filename = '/tmp/mwt2.xml'

    def __init__(self, config):
        if not mwt2info:
            self.mwt2url = config['namecorrection']['mwt2url']
            self._get_info_from_oim()
            self._parse_xml()

    def _get_info_from_oim(self):
        """
        Get the XML file from OIM
        """
        r = requests.get(self.mwt2url)
        if not r.status_code == requests.codes.ok:
            raise Exception("Unable to get MWT2 info from OIM")

        with open(self.mwt2filename, 'w') as f:
            f.write(r.text)

        return

    def _parse_xml(self):
        """
        Parse the XML file and store information into the mwt2info dict
        """
        tree = ET.parse(self.mwt2filename)
        root = tree.getroot()
        for elt in root.findall('./ResourceGroup/Resources/Resource'):
            mwt2info[elt.find('FQDN').text] = elt.find('Name').text
        return

    @staticmethod
    def get_info(fqdn):
        """
        Generates and returns the dict for an MWT2 fqdn

        :param str fqdn: FQDN from an ES query result
        :return dict: Dict with MWT2 info in the form of raw data entry in
        for use in TopOppUsageByFacility report
        """
        resource = mwt2info[fqdn]
        return {'OIM_Facility': 'University of Chicago',
                'OIM_ResourceGroup': 'MWT2',
                'OIM_Resource': resource}


class GPGridCorrection(object):
    @staticmethod
    def get_info():
        """Generates and returns the dict for GPGrid

        :return dict: Dict with GPGrid info in the form of raw data entry in
        for use in TopOppUsageByFacility report"""
        return {'OIM_Facility': 'Fermi National Accelerator Laboratory',
                'OIM_ResourceGroup': 'FNAL_FERMIGRID',
                'OIM_Resource': 'FNAL_GPGRID_4'}


class NameCorrection(object):
    """General Name Correction class to select correct info class (above)
    and return the relevant info

    :param str hd: Host Description from an ES query result
    """
    mwt2matchstring = re.compile('.+\@(.+)\/condor')
    gpgridmatchstring = 'GPGrid'

    def __init__(self, hd, config):
        self.args = []
        self.config = config
        # Select the correct class to instantiate
        if hd == self.gpgridmatchstring:
            self.cl = GPGridCorrection()
        elif self.mwt2matchstring.match(hd):
            self.cl = MWT2Correction(config=self.config)
            self.args = self.mwt2matchstring.match(hd).groups()

    def get_info(self):
        """
        Either returns the correct class' get_info method return value or None
        """
        try:
            return self.cl.get_info(*self.args)
        except AttributeError:
            return


if __name__ == '__main__':

    hd = 'GPGrid'
    n = NameCorrection(hd)
    print n.get_info()

    hd2 = 'ruc.ciconnect@uct2-gk.mwt2.org/condor'
    n2 = NameCorrection(hd2)
    print n2.get_info()
