"""TimeUtils is a library of helper functions, built heavily on datetime,
time, and dateutil, to help with the conversions of timestamps in gracc-
reporting.  Note that in this module, parse_datetime is the only function
that can accept non-UTC timestamps.  All other functions assume either epoch
time or UTC timestamps"""

from datetime import datetime, date
from calendar import timegm

from dateutil import tz, parser

class InvalidUnitError(ValueError):
    pass

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
        min_timestamp = datetime.min.time()
        _timestamp = datetime.combine(timestamp, min_timestamp)
    else:
        _timestamp = parser.parse(timestamp)

    if not utc:
        _timestamp = _timestamp.replace(tzinfo=tz.tzlocal())  # Assume time is local TZ
    else:
        _timestamp = _timestamp.replace(tzinfo=tz.tzutc())
    return _timestamp.astimezone(tz.tzutc())


def epoch_to_datetime(timestamp, unit='second'):    # Note that changes might affect JSR links
    """
    Parse epoch timestamp, return as UTC time datetime

    :param timestamp:  string or int.  Timestamp to convert to datetime.datetime object
    :return:  datetime.datetime object in UTC time zone
    """
    _accepted_units = {'second': 1, 'millisecond': 1e3, 'microsecond': 1e6}

    if timestamp is None:
        return None

    if not isinstance(timestamp, (float, int)):
        timestamp = float(timestamp)

    try:
        _timestamp = timestamp / _accepted_units[unit]
    except KeyError:
        raise InvalidUnitError("unit passed in was {0}. unit must be one "
                               "of {1}.".format(unit, ', '.join(
                                   _accepted_units)))

    timestamp = int(round(_timestamp))
    dt_timestamp = datetime.utcfromtimestamp(timestamp)
    return parse_datetime(dt_timestamp, utc=True)


def get_epoch_time_range_utc_ms(start_time, end_time):
    """Generates tuple of start_time, end_time in epoch time
    form from UTC datetime or date objects

    :param start_time: datetime.datetime, datetime.date, or str timestamp
    representing start time in UTC.
    :param end_time: Same as above, but end time (UTC)
    :return tuple: Timestamps representing milliseconds since epoch
    """
    assert start_time <= end_time
    return_dict = {"start_time": start_time, "end_time": end_time}

    for key in return_dict:
        # Make sure our timestamps are tz-aware, and are proper datetimes
        return_dict[key] = parse_datetime(return_dict[key], utc=True)
        return_dict[key] = timegm(return_dict[key].timetuple()) * 1000

    return return_dict["start_time"], return_dict["end_time"]
