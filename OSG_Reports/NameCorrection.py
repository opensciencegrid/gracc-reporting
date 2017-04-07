from xml.etree import ElementTree as ET
import os
import inspect
import requests
import re

parentdir = os.path.dirname(
    os.path.dirname(
        os.path.abspath(
            inspect.getfile(
                inspect.currentframe()
            )
        )
    )
)
os.sys.path.insert(0, parentdir)

import Configuration

mwt2info = {}
configfile = 'osg.config'
config = Configuration.Configuration()
config.configure(configfile)


class MWT2Correction(object):
    """

    """
    mwt2filename = '/tmp/mwt2.xml'
    mwt2url = config.config.get('namecorrection', 'mwt2url')

    def __init__(self):
        if not mwt2info:
            self._get_info_from_oim()
            self._parse_xml()

    def _get_info_from_oim(self):
        """

        :return:
        """
        r = requests.get(self.mwt2url)
        if not r.status_code == requests.codes.ok:
            raise Exception("Unable to get MWT2 info from OIM")

        with open(self.mwt2filename, 'w') as f:
            f.write(r.text)

        return

    def _parse_xml(self):
        """

        :return:
        """
        tree = ET.parse(self.mwt2filename)
        root = tree.getroot()
        for elt in root.findall('./ResourceGroup/Resources/Resource'):
            mwt2info[elt.find('FQDN').text] = elt.find('Name').text
        return

    @staticmethod
    def get_info(fqdn):
        """

        :param fqdn:
        :return:
        """
        resource = mwt2info[fqdn]
        return {'OIM_Facility': 'University of Chicago',
                'OIM_ResourceGroup': 'MWT2',
                'OIM_Resource': resource}


class GPGridCorrection(object):
    @staticmethod
    def get_info():
        return {'OIM_Facility': 'Fermi National Accelerator Laboratory',
                'OIM_ResourceGroup': 'FNAL_FERMIGRID',
                'OIM_Resource': 'FNAL_GPGRID_4'}


class NameCorrection(object):
    mwt2matchstring = re.compile('.+\@(.+)\/condor')
    gpgridmatchstring = 'GPGrid'

    def __init__(self, hd):
        self.args = []
        if hd == self.gpgridmatchstring:
            self.cl = GPGridCorrection()
        elif self.mwt2matchstring.match(hd):
            self.cl = MWT2Correction()
            self.args = self.mwt2matchstring.match(hd).groups()

    def get_info(self):
        """

        :return:
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
