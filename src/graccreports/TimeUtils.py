#!/usr/bin/python

from datetime import datetime, date
from dateutil import tz, parser
from calendar import timegm
import time


class TimeUtils(object):
    """
    Class to hold Time/datetime manipulations for the gracc reports.
    """
    def __init__(self):
        self.start_time = None
        self.end_time = None

    @staticmethod
    def parse_datetime(timestamp, utc=False):
        """
        Parse datetime, return as UTC time datetime
        
        :param timestamp:  datetime.date, datetime.datetime, or str.  Timestamp
         to convert to datetime.datetime object
        :param bool utc:  True if timestamp is in UTC.  False if it's local time
        :return: datetime.datetime object in UTC timezone
        """
        if timestamp is None:
            return None

        if isinstance(timestamp, datetime):
            x = timestamp
        elif isinstance(timestamp, date):
            mn = datetime.min.time()
            x = datetime.combine(timestamp, mn)
        else:
            x = parser.parse(timestamp)

        if not utc:
            x = x.replace(tzinfo=tz.tzlocal())  # Assume time is local TZ
        else:
            x = x.replace(tzinfo=tz.tzutc())
        return x.astimezone(tz.tzutc())

    @staticmethod
    def epoch_to_datetime(timestamp):
        """
        Parse epoch timestamp, return as UTC time datetime

        :param timestamp:  string or int.  Timestamp to convert to datetime.datetime object
        """
        if timestamp is None:
            return None

        if isinstance(timestamp, str):
            timestamp = float(timestamp)
        
        now = time.time()
        # Check for milliseconds vs seconds epoch timestamp
        try:
            assert timestamp > now
        except AssertionError:    # We assume that the epoch time is in ms
            _timestamp = timestamp / 1000
            try:
                assert _timestamp > now
            except AssertionError:
                raise OverflowError("Timestamp {0} is too large to be an epoch time".format(timestamp))
            except Exception as e:
                raise
            else:
                timestamp = int(_timestamp)
        except Exception as e:
            raise
        
        dt_timestamp = datetime.fromtimestamp(timestamp)
        return TimeUtils.parse_datetime(dt_timestamp, utc=True)

    @staticmethod
    def check_date_datetime(item):
        """
        Check to make sure if item is instance of datetime.datetime or 
        datetime.date
        
        :param item: object to test
        :return: True if item is date or datetime instance
        """
        return isinstance(item, datetime) or isinstance(item, date)

    def get_epoch_time_range_utc(self, start_time=None, end_time=None):
        """Generates tuple of self.start_time, self.end_time in epoch time
        form
        
        :param start_time: datetime.datetime, datetime.date, or str timestamp
        representing start time.
        :param end_time: Same as above, but end time
        :return tuple: Timestamps representing milliseconds since epoch 
        """
        d = {"start_time": start_time, "end_time": end_time}
        for key in d:
            if d[key] is not None:
                if not self.check_date_datetime(d[key]):
                    d[key] = self.parse_datetime(d[key])  # Convert to datetime
            else:
                try:
                    val = getattr(self, key)
                    if val is not None and self.check_date_datetime(val):
                        d[key] = val
                    else:
                        raise AttributeError
                except AttributeError:
                    print "A value must be specified for variable {0}".format(key)
                    raise

            d[key] = timegm(d[key].timetuple()) * 1000  # Convert to Epoch milliseconds

        return d["start_time"], d["end_time"]
