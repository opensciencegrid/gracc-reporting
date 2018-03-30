import unittest
import os
from copy import deepcopy

import toml

import gracc_reporting.ReportUtils as ReportUtils


config_file = 'test_config.toml'
bad_config_file = 'test_bad_config.toml'



class FakeVOReport(ReportUtils.Reporter):
    def __init__(self, cfg_file=config_file, vo=None):
        report = 'test'
        start = '2018-03-28 06:30'
        end = '2018-03-29 06:30'
        super(FakeVOReport, self).__init__(report=report, config=cfg_file,
                                         start=start, end=end, vo=vo)

    def query(self): pass
    def run_report(self): pass


class TestReportUtilsBase(unittest.TestCase):
    def setUp(self):
        self.r = FakeVOReport(vo='testVO')
        self.r_copy = FakeVOReport(cfg_file=bad_config_file)


class TestGetLogfilePath(TestReportUtilsBase):
    def test_override(self):
        """Return override logfile if that's passed in"""
        fn = "/tmp/override.log"
        self.assertEqual(self.r.get_logfile_path(fn), fn)

    def test_configfile(self):
        """Logfile should be set to configfile value"""
        answer = os.path.join(self.r.config["default_logdir"], 'gracc-reporting',
            'test.log')
        self.assertEqual(self.r.get_logfile_path(), answer)

    def test_fallback(self):
        """Set logdir to $HOME if no override and no configfile value"""
        answer = os.path.join(os.path.expanduser('~'), 'gracc-reporting', 
            'test.log')
        self.assertEqual(self.r_copy.get_logfile_path(), answer)

    def test_bad_configval(self):
        """Set logdir to $HOME if configfile value is invalid"""
        self.r_copy.config["default_logdir"] = '/'
        answer = os.path.join(os.path.expanduser('~'), 'gracc-reporting',
                              'test.log')
        self.assertEqual(self.r_copy.get_logfile_path(), answer)
        del self.r_copy.config["default_logdir"]

class TestParseConfig(TestReportUtilsBase):
    def test_parse_config_control(self):
        """Parse a normal config"""
        answer = {u'test': {
                    u'index_pattern': u'gracc.osg.raw-%Y.%m', 
                    u'testvo': {
                        u'min_hours': 1000, 
                        u'min_efficiency': 0.5, 
                        u'to_emails': [u'nobody@example.com']
                        }
                    }, 
                    u'configured_vos': [u'testvo'], 
                    u'elasticsearch': {
                        u'hostname': u'https://gracc.opensciencegrid.org/q'
                        }, 
                    u'email': {
                        u'test': {
                            u'names': [u'Test Recipient'], 
                            u'emails': [u'nobody@example.com']
                            }, 
                        u'from': {
                            u'name': u'GRACC Operations', 
                            u'email': u'nobody@example.com'
                        }, 
                        u'smtphost': u'smtp.example.com'
                    }, 
                    u'default_logdir': u'/tmp/gracc-test'
                }
        self.assertDictEqual(self.r._parse_config(config_file), answer)

    def test_invalid_config(self):
        """Raise toml.TomlDecodeError if we're parsing a bad config file"""
        bad_toml = "blahblah\""
        junk_file = "/tmp/junk.toml"

        with open(junk_file, 'w') as f:
            f.write(bad_toml)

        self.assertRaises(toml.TomlDecodeError, self.r._parse_config, junk_file)

