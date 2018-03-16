"""
TimeUtils contains functions to manipulate and run validation tests on datetimes in
 the gracc-reporting suite.
"""

import time
from calendar import timegm

from datetime import datetime, date
from dateutil import tz, parser


# TODO:  Given that this has a number of static methods and ONE instance method,
# there is no reason to keep this a class 
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
            _timestamp = timestamp
        elif isinstance(timestamp, date):
            min_time = datetime.min.time()
            _timestamp = datetime.combine(timestamp, min_time)
        else:
            _timestamp = parser.parse(timestamp)

        if not utc:
            _timestamp = _timestamp.replace(tzinfo=tz.tzlocal())  # Assume time is local TZ
        else:
            _timestamp = _timestamp.replace(tzinfo=tz.tzutc())
        return _timestamp.astimezone(tz.tzutc())

    @staticmethod
    def epoch_to_datetime(timestamp):
        """
        Parse epoch timestamp, return as UTC time datetime

        :param timestamp:  string or int.  Timestamp to convert to datetime.datetime object
        :return:  datetime.datetime object in UTC time zone
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
                raise OverflowError("Timestamp {0} is too large to be an "
                                    "epoch time".format(timestamp))
            except Exception:
                raise
            else:
                timestamp = _timestamp
        except Exception:
            raise

        timestamp = int(timestamp)
        dt_timestamp = datetime.utcfromtimestamp(timestamp)
        return TimeUtils.parse_datetime(dt_timestamp, utc=True)

    @staticmethod
    def check_date_datetime(item):
        """
        Check to make sure if item is instance of datetime.datetime or 
        datetime.date
        
        :param item: object to test
        :return: True if item is date or datetime instance
        """
        return isinstance(item, (date, datetime))

    def get_epoch_time_range_utc(self, start_time=None, end_time=None):
        """Generates tuple of start_time, end_time  OR self.start_time, 
        self.end_time in epoch time form (the first two override the 
        second two)
        
        :param start_time: datetime.datetime, datetime.date, or str timestamp
        representing start time.
        :param end_time: Same as above, but end time
        :return tuple: Timestamps representing milliseconds since epoch 
        """
        return_dict = {"start_time": start_time, "end_time": end_time}
        for key in return_dict:
            if return_dict[key] is not None:
                if not self.check_date_datetime(return_dict[key]):
                    return_dict[key] = self.parse_datetime(return_dict[key])  # Convert to datetime
            else:
                try:
                    val = getattr(self, key)
                    if val is not None and self.check_date_datetime(val):
                        return_dict[key] = val
                    else:
                        raise AttributeError
                except AttributeError:
                    print "A value must be specified for variable {0}".format(key)
                    raise

            # Convert to Epoch milliseconds
            return_dict[key] = timegm(return_dict[key].timetuple()) * 1000  

        return return_dict["start_time"], return_dict["end_time"]
