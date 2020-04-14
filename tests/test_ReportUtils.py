"""Unit tests for ReportUtils"""

import unittest
import os
from shutil import copyfile

import toml

import gracc_reporting.ReportUtils as ReportUtils

CONFIG_FILE = 'tests/test_config.toml'
BAD_CONFIG_FILE = 'tests/test_bad_config.toml'


# Reporter stuff
class FakeVOReport(ReportUtils.Reporter):
    """Fake Report class on top of ReportUtils.Reporter"""
    def __init__(self, cfg_file=CONFIG_FILE, vo=None, althost_key=None,
                 is_test=False):
        report = 'test'
        start = '2018-03-28 06:30'
        end = '2018-03-29 06:30'
        super(FakeVOReport, self).__init__(report_type=report, 
                                           config_file=cfg_file,
                                           start=start, 
                                           end=end, 
                                           vo=vo,
                                           althost_key=althost_key,
                                           is_test=is_test)

    # Defined to satisfy abstract class constraints
    def query(self): pass
    def run_report(self): pass


class TestReportUtilsBase(unittest.TestCase):
    """Base class for ReportUtils tests"""
    def setUp(self):
        self.r = FakeVOReport(vo='testVO')
        self.r_copy = FakeVOReport(cfg_file=BAD_CONFIG_FILE)


class TestGetLogfilePath(TestReportUtilsBase):
    """Tests for ReportUtils.Reporter.get_logfile_path"""
    def test_override(self):
        """Return override logfile if that's passed in"""
        fn = "/tmp/override.log"
        self.assertEqual(self.r.get_logfile_path(fn), fn)

    def test_configfile(self):
        """Logfile should be set to configfile value"""
        answer = os.path.join(self.r.config["default_logdir"],
                              'gracc-reporting', 'test.log')
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
    """Tests for ReportUtils.Reporter._parse_config"""
    def test_parse_config_control(self):
        """Parse a normal config"""
        answer = {'test': {
                    'index_pattern': 'gracc.osg.raw-%Y.%m', 
                    'to_emails': ['nobody2@example.com'],
                    'to_names' : ['test name'],
                    'testvo': {
                        'min_hours': 1000, 
                        'min_efficiency': 0.5, 
                        'to_emails': ['nobody3@example.com']
                        }
                    }, 
                    'configured_vos': ['testvo'], 
                    'elasticsearch': {
                        'ok_statuses': ['green', 'yellow'],
                        'hostname': 'https://gracc.opensciencegrid.org/q',
                        'secondary_host': 'https://gracc.opensciencegrid.org/q',
                        'bad_host': 'https://www.blah.badurl'
                        }, 
                    'email': {
                        'test': {
                            'names': ['Test Recipient'], 
                            'emails': ['nobody1@example.com']
                            }, 
                        'from': {
                            'name': 'GRACC Operations', 
                            'email': 'nobody@example.com'
                        }, 
                        'smtphost': 'smtp.example.com'
                    }, 
                    'default_logdir': '/tmp/gracc-test'
                }
        self.assertDictEqual(self.r._parse_config(CONFIG_FILE), answer)

    def test_invalid_config(self):
        """Raise toml.TomlDecodeError if we're parsing a bad config file"""
        bad_toml = "blahblah\""
        junk_file = "/tmp/junk.toml"

        with open(junk_file, 'w') as f:
            f.write(bad_toml)

        self.assertRaises(toml.TomlDecodeError, self.r._parse_config, junk_file)


class TestCheckVO(TestReportUtilsBase):
    """We're not going to actually test ReportUtils.Reporter.__check_vo
    directly. We're instead going to check for behavior if we pass a valid
    and invalid VO into the class instantiation"""

    def test_valid_vo(self):
        """Instantiate Reporter with a valid VO"""
        answer = "testVO"
        good_report_inst = FakeVOReport(vo=answer)
        self.assertEqual(good_report_inst.vo, answer)
        del good_report_inst

    def test_bad_vo(self):
        """Instantiate Reporter with an invalid VO"""
        self.assertRaises(KeyError, FakeVOReport, vo="thisshouldfail")


class TestEstablishClient(TestReportUtilsBase):
    """Test establishing of Elasticsearch client, by instantiating the
    ReportUtils.Reporter with different parameters and testing the behavior"""
    def test_default_es_client(self):
        """Automatically pass because all other tests must pass this to run"""
        pass

    def test_no_althostkey_no_config_entry(self):
        """host should return https://gracc.opensciencegrid.org/q if no
        althost is specified and there is no config file entry"""
        _cfg = BAD_CONFIG_FILE
        test_report = FakeVOReport(cfg_file=_cfg)
        self.assertEqual(test_report.client.transport.hosts[0]['host'],
                         'gracc.opensciencegrid.org')
        del test_report

    def test_althost_key(self):
        """Connect to host specified in secondary_host key"""
        test_report_ok = FakeVOReport(althost_key='secondary_host')
        self.assertEqual(test_report_ok.client.transport.hosts[0]['host'],
                         'gracc.opensciencegrid.org')
        del test_report_ok

    def test_althost_bad(self):
        """Raise SystemExit if connecting to a bad host"""
        #self.assertRaises(SystemExit, FakeVOReport, althost_key='bad_host')

    def test_althost_invalid(self):
        """Raise SystemExit if passing in an althost_key that's not in the 
        config file"""
        self.assertRaises(SystemExit, FakeVOReport, althost_key='invalid_key')

    def test_cluster_bad_status(self):
        """Raise SystemExit if the cluster is in a state that's not 
        in the config file as being 'OK'"""
        _cfg = '/tmp/junk.toml'
        text = """
[elasticsearch]
    ok_statuses = ['purple']
        """
        copyfile(BAD_CONFIG_FILE, _cfg)
        with open(_cfg, 'a') as f:
            f.write(text)
                
        self.assertRaises(SystemExit, FakeVOReport, cfg_file=_cfg)



class TestGetEmailInfo(TestReportUtilsBase):
    """Tests for ReportUtils.Reporter.__get_email_info"""
    def test_email_info_control_VO(self):
        """If we provide a VO, grab the recipient information from
        config[report_type][vo][to_emails]"""
        answer = {"to": {
                    "email": ["nobody1@example.com", "nobody3@example.com", ],
                    "name": []
                  },
                  "from": {
                      "email": "nobody@example.com", 
                      "name": "GRACC Operations"
                  },
                  "smtphost": "smtp.example.com"
                  }
        self.assertDictEqual(self.r.email_info, answer)

    def test_email_info_control_no_VO(self):
        """If we don't provide a VO, grab the recipient information from
        config[report_type][to_emails]"""
        answer = {"to": {
            "email": ["nobody1@example.com", "nobody2@example.com", ],
            "name": ['Test Recipient', 'test name']
        },
            "from": {
            "email": "nobody@example.com",
            "name": "GRACC Operations"
        },
            "smtphost": "smtp.example.com"
        }
        test_report_no_vo = FakeVOReport()
        self.assertDictEqual(test_report_no_vo.email_info, answer)
        del test_report_no_vo

    def test_email_info_test_mode(self):
        """If we're in test mode, only email recipients should be 
        from email.test"""
        recipients_dict = {
            "email": ["nobody1@example.com", ],
            "name": ['Test Recipient', ]}
        test_report_test = FakeVOReport(is_test=True)
        self.assertDictEqual(test_report_test.email_info['to'], recipients_dict)
        del test_report_test


# Everything besides Reporter
class TestUtilFuncs(unittest.TestCase):
    """Unit tests for ReportUtils module level functions"""
    def test_coroutine(self):
        """Use ReportUtils.coroutine decorator to create simple coroutine"""
        @ReportUtils.coroutine
        def test_func(*args, **kwargs):
            pass
            value = yield
            yield value

        f = test_func()
        self.assertEqual(f.send(1), 1)