"""Unit tests for IndexPattern.py"""

import unittest
from datetime import datetime

from gracc_reporting.IndexPattern import indexpattern_generate


date_dateend = datetime(2016, 0o6, 12)

date_datestart1 = datetime(2016, 0o6, 10)
date_datestart2 = datetime(2016, 5, 10)
date_datestart3 = datetime(2015, 0o5, 10)
date_break = '20160205'

gracc_summary_index = 'gracc.osg.summary'
gracc_all_raw = 'gracc.osg.raw-*'

class TestIndexPatternGenerateDefault(unittest.TestCase):
    """Tests for indexpattern_generate default behavior"""

    def test_default_behavior(self):
        """No arguments should return gracc.osg.summary"""
        self.assertEqual(indexpattern_generate(), gracc_summary_index)

    def test_default_with_dates(self):
        """Return gracc.osg.summary when passing in dates with no pattern"""
        self.assertEqual(
            indexpattern_generate(
                start=date_datestart1, end=date_dateend), gracc_summary_index)

    def test_default_with_start_date(self):
        """Return gracc.osg.summary when passing in one date with no
        pattern"""
        self.assertEqual(
            indexpattern_generate(start=date_datestart1), gracc_summary_index)


class TestIndexPatternGenerateDateIndep(unittest.TestCase):
    """Tests for indexpattern_generate when passed a date-independent 
    pattern"""

    def test_all_date_indep_pattern_runall(self):
        """Run date-independent tests in this class that should pass"""
        for p in (gracc_summary_index, gracc_all_raw):
            self.assertTrue(self.__test_indep_pattern(p))
            self.assertTrue(self.__test_indep_pattern_date(p))

    @staticmethod
    def __test_indep_pattern(pat):
        """Passing in date-independent pattern should return itself""" 
        try:
            assert indexpattern_generate(pattern=pat) == pat
        except AssertionError:
            return False
        return True
    
    @staticmethod
    def __test_indep_pattern_date(pat):
        """Passing in date-independent pattern and any date should return
        pattern"""
        try:
            assert indexpattern_generate(pattern=pat,
                                         start=date_datestart1) == pat
        except AssertionError:
            return False
        return True

    def test_bad_pattern_indep(self):
        """If we pass in a non-string pattern, we should get a TypeError"""
        pattern_bad = 4
        self.assertRaises(TypeError, indexpattern_generate,
                          pattern=pattern_bad, sdate=None)
        self.assertRaises(TypeError, indexpattern_generate,
                          pattern=pattern_bad, sdate=date_datestart1)


class TestIndexPatternGenerateDateDep(unittest.TestCase):
    """Unit tests for indexpattern_generate when passing in
    date-dependent patterns"""

    pattern_good = 'gracc.osg.raw-%Y.%m'

    def test_bad_start_date(self):
        """Raise AttributeError if we pass in a date-dependent pattern, an invalid
        start date, but a valid end date"""
        self.assertRaises(AttributeError, indexpattern_generate,
                          pattern=self.pattern_good,
                          start=date_break, end=date_dateend)

    def test_bad_end_date(self):
        """Raise AttributeError if we pass in a date-dependent pattern, a valid
        start date, but an invalid end date"""
        self.assertRaises(AttributeError, indexpattern_generate,
                          pattern=self.pattern_good,
                          start=date_datestart1, end=date_break)

    def test_good_datepattern_no_dates(self):
        """Raise AttributeError if we pass in a date-dependent pattern,
        and no dates"""
        self.assertRaises(AttributeError, indexpattern_generate,
                          pattern=self.pattern_good)

    def test_month_level(self):
        """Generate a month-level index pattern if dates are in the same
        month"""
        answer = 'gracc.osg.raw-2016.06'
        self.assertEqual(indexpattern_generate(
            pattern=self.pattern_good, start=date_datestart1, end=date_dateend),
                         answer)

    def test_year_level(self):
        """Generate a year-level index pattern if dates are in the same year, but different
        months"""
        answer = 'gracc.osg.raw-2016.0*'
        self.assertEqual(indexpattern_generate(
            pattern=self.pattern_good, start=date_datestart2, end=date_dateend),
                         answer)

    def test_whole_index(self):
        """Generate general index pattern if the dates are in different
        years"""
        answer = 'gracc.osg.raw-201*'
        self.assertEqual(indexpattern_generate(
            pattern=self.pattern_good, start=date_datestart3, end=date_dateend),
                         answer)
