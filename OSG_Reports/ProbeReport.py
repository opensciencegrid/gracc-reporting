import xml.etree.ElementTree as ET
from datetime import timedelta, date
import urllib2
import ast
import re

class OIMInfo(object):
    def __init__(self):
        self.e = None
        self.root = None
        self.resourcedict = {}

        self.xml_file = self.get_file_from_OIM()
        if self.xml_file:
            self.parse()

    @staticmethod
    def get_file_from_OIM():
        today = date.today()
        startdate = today - timedelta(days=7)
        rawdateslist = [startdate.month, startdate.day, startdate.year,
                        today.month, today.day, today.year]
        dateslist = ['0' + str(elt) if len(str(elt)) == 1 else str(elt)
                     for elt in rawdateslist]

        oim_url = 'http://myosg.grid.iu.edu/rgsummary/xml?summary_attrs_showhierarchy=on' \
                  '&summary_attrs_showwlcg=on&summary_attrs_showservice=on&summary_attrs_showfqdn=on&gip_status_attrs_showtestresults=on&downtime_attrs_showpast=&account_type=cumulative_hours&ce_account_type=gip_vo&se_account_type=vo_transfer_volume&bdiitree_type=total_jobs&bdii_object=service&bdii_server=is-osg&start_type=7daysago&start_date={0}%2F{1}%2F{2}&end_type=now&end_date={3}%2F{4}%2F{5}&all_resources=on&facility_sel%5B%5D=10009&gridtype=on&gridtype_1=on&service=on&service_sel%5B%5D=1&active=on&active_value=1&disable_value=1&has_wlcg=on'\
        .format(*dateslist)

        try:
            oim_xml = urllib2.urlopen(oim_url)
        except (urllib2.HTTPError, urllib2.URLError) as e:
            print e

        return oim_xml

    def parse(self):
        self.e = ET.parse(self.xml_file)
        self.root = self.e.getroot()
        print "Parsing File"

        for resourcename_elt in self.root.findall('./ResourceGroup/Resources/Resource'
                                             '/Name'):
            resourcename = resourcename_elt.text
            if resourcename not in self.resourcedict:
                resource_grouppath = './ResourceGroup/Resources/Resource/' \
                                     '[Name="{0}"]/../..'.format(resourcename)
                self.resourcedict[resourcename] = self.get_resource_information(resource_grouppath,
                                                                      resourcename)
        return

    def get_resource_information(self, rgpath, rname):
        """Uses parsed XML file and finds the relevant information based on the
         dictionary of XPaths.  Searches by resource.

         Arguments:
             resource_grouppath (string): XPath path to Resource Group
             Element to be parsed
             resourcename (string): Name of resource

         Returns dictionary that has relevant OIM information
         """

        # This could (and probably should) be moved to a config file
        rg_pathdictionary = {
            'Facility': './Facility/Name',
            'Site': './Site/Name',
            'ResourceGroup': './GroupName'}

        r_pathdictionary = {
            'Resource': './Name',
            'ID': './ID',
            'FQDN': './FQDN',
            'WLCGInteropAcct': './WLCGInformation/InteropAccounting'
        }

        returndict = {}

        # Resource group-specific info
        resource_group_elt = self.root.find(rgpath)
        for key, path in rg_pathdictionary.iteritems():
            try:
                returndict[key] = resource_group_elt.find(path).text
            except AttributeError:
                # Skip this.  It means there's no information for this key
                pass

        # Resource-specific info
        resource_elt = resource_group_elt.find(
            './Resources/Resource/[Name="{0}"]'.format(rname))
        for key, path in r_pathdictionary.iteritems():
            try:
                returndict[key] = resource_elt.find(path).text
            except AttributeError:
                # Skip this.  It means there's no information for this key
                pass

        return returndict

    def get_fqdns_for_probes(self):
        oim_probe_fqdns_list = []
        for resourcename, info in self.resourcedict.iteritems():
            if ast.literal_eval(info['WLCGInteropAcct']):
                oim_probe_fqdns_list.append(info['FQDN'])
        return set(oim_probe_fqdns_list)



def main():
    oiminfo = OIMInfo()
    oim_probe_fqdns = oiminfo.get_fqdns_for_probes()
    print oim_probe_fqdns


if __name__ == '__main__':
    main()