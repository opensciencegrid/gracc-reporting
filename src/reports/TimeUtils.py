#!/usr/bin/python

import re
from datetime import datetime, date
import time


class TimeUtils(object):
    def __init__(self):
        self.start_time = None
        self.end_time = None
        self.local_time_offset = self.get_local_time_offset()

    @staticmethod
    def get_local_time_offset():
        """Returns local time offset from UTC"""
        delta = datetime.now() - datetime.utcnow()
        return int((delta.microseconds + (
            delta.seconds + delta.days * 24 * 3600) * 10 ** 6) / 10 ** 6)

    @staticmethod
    def handle_date_vs_datetime(time_in):
        """Tries to see if the timestamp is a in date-time format or just date
        Returns a time.struct_time object either way"""
        try:
            out_time = time.strptime(re.sub('-', '/', time_in),
                              '%Y/%m/%d %H:%M:%S')
        except ValueError:
            out_time = time.strptime(re.sub('-', '/', time_in), '%Y/%m/%d')
        return out_time

    @staticmethod
    def datetimecheck(test_date):
        """Checks to see if date is in the form yyyy/mm/dd HH:MM:SS or
        yyyy-mm-dd HH:MM:SS.  We return the match object if it is, or else None"""
        slashpattern_time = re.compile(
            '(\d{4})/(\d{2})/(\d{2})\s(\d{2}):(\d{2}):(\d{2})')
        dashpattern_time = re.compile(
            '(\d{4})-(\d{2})-(\d{2})\s(\d{2}):(\d{2}):(\d{2})')
        for pattern in [slashpattern_time, dashpattern_time]:
            match = pattern.match(test_date)
            if match:
                break
        return match

    @staticmethod
    def datecheck(test_date):
        """Checks to see if date is in the form yyyy/mm/dd or yyyy-mm-dd.
        We return the match object if it is, or else None"""
        slashpattern = re.compile('(\d{4})/(\d{2})/(\d{2})')
        dashpattern = re.compile('(\d{4})-(\d{2})-(\d{2})')
        for pattern in [slashpattern, dashpattern]:
            match = pattern.match(test_date)
            if match:
                break
        return match

    def dateparse(self, date_in):
        """Function to make sure that our date is either a list of form
        [yyyy, mm, dd], a datetime.datetime object or a date in the form of
        yyyy/mm/dd HH:MM:SS or yyyy-mm-dd HH:MM:SS

        Arguments:
            time (bool):  if True, then we are passing in a date/time, and want to
            return the date and time.  If False (default), we can pass in either
            a date or a date/time, but we only want to return a date

        Returns:
            List of date elements in [yyyy,mm,dd] form or list of datetime elements
            in [yyyy,mm,dd,HH,MM,SS] form
        """
        while True:
            if isinstance(date_in, datetime) or \
                    isinstance(date_in, date):
                    return [elt for elt in date_in.timetuple()[:6]]
            elif isinstance(date_in, time.struct_time):
                return [elt for elt in date_in[:6]]
            elif isinstance(date_in, list):
                return date_in
            else:
                try:
                    match = self.datetimecheck(date_in)
                    if not match:
                        match = self.datecheck(date_in)
                    if match:
                        date_in = self.handle_date_vs_datetime(date_in)
                    else:
                        raise
                    continue  # Pass back to beginning of loop so datetime.date clause returns the date string
                except:
                    raise TypeError(
                        "The date must be a datetime.date object, a list in the "
                        "form of [yyyy,mm,dd], or a date in the form of yyyy/mm/dd "
                        "or yyyy-mm-dd or datetime in the form yyyy/mm/dd HH:MM:SS"
                        " or yyyy-mm-dd HH:MM:SS")

    def dateparse_to_iso(self, date_time):
        """Parses date_time into iso format"""
        datelist = self.dateparse(date_time)
        return datetime(*[int(elt) for elt in datelist]).isoformat()

    def get_epoch_stamps_for_grafana(self, start_time=None, end_time=None):
        """Generates tuple of self.start_time, self.end_time in epoch time
        form
        """
        if not start_time:
            start_time = self.start_time
        if not end_time:
            end_time = self.end_time
        start = self.handle_date_vs_datetime(start_time)
        end = self.handle_date_vs_datetime(end_time)
        # Multiply each by 1000 to convert to milliseconds for grafana
        start_epoch = int((time.mktime(start) + self.local_time_offset) * 1000)
        end_epoch = int((time.mktime(end) + self.local_time_offset) * 1000)
        self.epochrange = (start_epoch, end_epoch)
        return self.epochrange