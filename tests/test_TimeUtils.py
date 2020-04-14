"""Unit tests for TimeUtils"""

import unittest
from datetime import datetime, date

from dateutil import tz

import gracc_reporting.TimeUtils as TimeUtils

class TestParseDatetime(unittest.TestCase):
    """Tests for TimeUtils.parse_datetime"""
    fail_string = "this should not parse correctly"
    date_local = date(2018, 3, 27)
    datetime_local = datetime(2018, 3, 27, 16, 00, 00)
    datetime_utc = datetime(2018, 3, 27, 16, 00, 00).replace(tzinfo=tz.tzutc())

    def test_none(self):
        """If we pass in None, we should get None back"""
        self.assertIsNone(TimeUtils.parse_datetime(None))

    def test_utc_time_control(self):
        """This should just return itself"""
        answer = self.datetime_utc
        self.assertEqual(TimeUtils.parse_datetime(self.datetime_utc, utc=True), 
            answer)

    def test_local_time_control(self):
        """Take our local time, transform it to UTC"""
        answer = self.datetime_local.replace(tzinfo=tz.tzlocal()).astimezone(
            tz.tzutc())
        self.assertEqual(TimeUtils.parse_datetime(self.datetime_local), answer)
    
    def test_date_parse(self): 
        """Parse a datetime.date object into a datetime.datetime object"""
        answer = datetime(2018, 3, 27, 00, 00, 00).replace(
            tzinfo=tz.tzlocal()).astimezone(tz.tzutc())
        self.assertEqual(TimeUtils.parse_datetime(self.date_local), answer)

    def test_valid_datestring_parse(self):
        """If we pass in a date in a standard form, it should get parsed"""
        answer = self.datetime_utc
        in_dates = ("2018 Mar 27 16:00:00 UTC",
                    "2018-03-27 16:00:00 UTC", 
                    "Tue Mar 27 16:00:00 UTC 2018")
        for in_date in in_dates:
            self.assertEqual(TimeUtils.parse_datetime(
                in_date, utc=True), answer)

    def test_fail_parse(self):
        """Invalid time string should fail to parse""" 
        self.assertRaises(Exception, TimeUtils.parse_datetime, self.fail_string)


class TestEpochToDatetime(unittest.TestCase):
    """Test TimeUtils.epoch_to_datetime"""
    epoch_time = 1522253329
    datetime_time = datetime(2018, 3, 28, 11, 8, 49)

    def test_the_test(self):
        """Make sure our test constants are equivalent"""
        self.assertEqual(
            datetime.fromtimestamp(self.epoch_time), self.datetime_time)

    def test_return_none(self):
        """If we pass in None, we should get back None"""
        self.assertIsNone(TimeUtils.epoch_to_datetime(None))

    def test_control_epoch(self):
        """If we pass in self.epoch_time, we should get self.datetime_time"""
        answer = self.datetime_time.replace(
            tzinfo=tz.tzlocal()).astimezone(tz=tz.tzutc())
        self.assertEqual(TimeUtils.epoch_to_datetime(self.epoch_time), answer)

    def test_units(self):
        """If we specify a valid unit, the correct conversion
        should take place"""
        answer = self.datetime_time.replace(
            tzinfo=tz.tzlocal()).astimezone(tz=tz.tzutc())
        conversions = {'second': 1, 'millisecond': 1e3, 'microsecond': 1e6}
        units_inputs = {}

        for unit, factor in conversions.items():
            units_inputs[unit] = self.epoch_time * factor

        for unit_name, value in units_inputs.items():
            self.assertEqual(TimeUtils.epoch_to_datetime(value, unit=unit_name), answer)

    def test_unit_fail(self):
        """Raise InvalidUnitError if invalid unit is passed in"""
        self.assertRaises(TimeUtils.InvalidUnitError,
                          TimeUtils.epoch_to_datetime, self.epoch_time,
                          'hours')

class TestGetEpochTimeRangeUtcms(unittest.TestCase):
    """Test TimeUtils.get_epoch_time_range_utc_ms"""
    start = datetime(2018, 3, 27, 16, 8, 49)
    end = datetime(2018, 3, 28, 16, 8, 49)

    def test_invalid_args_type(self):
        """Raise generic Exception if we pass in invalid args"""
        args_list = ['hello', 'world']
        self.assertRaises(Exception,
                          TimeUtils.get_epoch_time_range_utc_ms, *args_list)

    def test_invalid_args_value(self):
        """Raise AssertionError if we pass in bad values.  Switching
        start and end should trip this"""
        self.assertRaises(AssertionError,
                          TimeUtils.get_epoch_time_range_utc_ms,
                          self.end, self.start)

    def test_control_epoch_range(self):
        """Return epoch time range in ms for valid input range"""
        answer = (1522166929000, 1522253329000)
        self.assertTupleEqual(TimeUtils.get_epoch_time_range_utc_ms(self.start, self.end), answer)


if __name__ == '__main__':
    unittest.main()
