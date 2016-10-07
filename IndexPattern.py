#!/usr/bin/python

import datetime
import re
from TimeUtils import TimeUtils


def datetimecheck(date):
    """Checks to see if date is in the form yyyy/mm/dd HH:MM:SS or
    yyyy-mm-dd HH:MM:SS.  We return the match object if it is, or else None"""
    slashpattern_time = re.compile(
        '(\d{4})/(\d{2})/(\d{2})\s(\d{2}):(\d{2}):(\d{2})')
    dashpattern_time = re.compile(
        '(\d{4})-(\d{2})-(\d{2})\s(\d{2}):(\d{2}):(\d{2})')
    for pattern in [slashpattern_time, dashpattern_time]:
        match = pattern.match(date)
        if match:
            break
    return match

def datecheck(date):
    """Checks to see if date is in the form yyyy/mm/dd or yyyy-mm-dd.
    We return the match object if it is, or else None"""
    slashpattern = re.compile('(\d{4})/(\d{2})/(\d{2})')
    dashpattern = re.compile('(\d{4})-(\d{2})-(\d{2})')
    for pattern in [slashpattern, dashpattern]:
        match = pattern.match(date)
        if match:
            break
    return match

def dateparse(date, time=False):
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
        if isinstance(date, datetime.datetime) or \
                isinstance(date, datetime.date):
            if time:
                return [date.year, date.month, date.day, date.hour, date.minute, date.second]
            if not time:
                return [date.year, date.month, date.day]
        elif isinstance(date, list):
            return date
        else:
            try:
                if time:
                    match = datetimecheck(date)
                    if not match:
                        match = datecheck(date)
                else:
                    match = datecheck(date)
                if match:
                    date = datetime.datetime(
                        *[int(elt) for elt in match.groups()])
                else:
                    raise
                continue        # Pass back to beginning of loop so datetime.date clause returns the date string
            except:
                raise TypeError(
                    "The date must be a datetime.date object, a list in the "
                    "form of [yyyy,mm,dd], or a date in the form of yyyy/mm/dd "
                    "or yyyy-mm-dd or datetime in the form yyyy/mm/dd HH:MM:SS"
                    " or yyyy-mm-dd HH:MM:SS")


def indexpattern_generate(start, end, raw=True):
    """Function to return the proper index pattern for queries to elasticsearch on gracc.opensciencegrid.org.  This improves performance by not just using a general index pattern unless absolutely necessary.
    This will especially help with reports, for example.

    This function assumes that the date being passed in has been split into a list with [yyyy,mm,dd] format.  This gets tested and cleaned up in the called dateparse function.
    """
    if not raw:
        return 'gracc.osg.summary*'

    t = TimeUtils()
    startdate = t.dateparse(start)
    enddate = t.dateparse(end)

    basepattern = 'gracc.osg.raw-'

    if startdate[0] == enddate[0]:                        # Check if year is the same
        basepattern += '{0}.'.format(str(startdate[0]))
        if startdate[1] == enddate[1]:                    # Check if month is the same
            if len(str(startdate[1])) == 1:               # Add leading zero if necessary
                add = '0{0}'.format(str(startdate[1]))
            else:
                add = str(startdate[1])
            basepattern += '{0}'.format(add)
        else:
            basepattern += '*'
    else:
        basepattern += '*'

    return basepattern


if __name__ == "__main__":
    # Meant for testing
    date_end = ['2016', '06', '12']
    date_start1 = ['2016', '06', '10']
    date_start2 = ['2016', '05', '10']
    date_start3 = ['2015', '06', '10']

    date_dateend = datetime.date(2016, 06, 12)
    date_datestart1 = datetime.datetime(2016, 06, 10)
    date_datestart2 = datetime.date(2016, 5, 10)
    date_datestart3 = datetime.date(2015, 05, 10)

    datestringslash = '2016/06/10'
    datestringdash = '2016-06-10'

    fulldate = '2016/06/10 12:34:00'

    datebreak = '20160205'

    # gracc.osg.raw-YYYY.MM

    assert indexpattern_generate(date_start1, date_end) == 'gracc.osg.raw-2016.06', "Assertion Error, {0}-{1} test failed".format(date_start1, date_end)
    assert indexpattern_generate(date_start2, date_end) == 'gracc.osg.raw-2016.*', "Assertion Error, {0}-{1} test failed".format(date_start2, date_end)
    assert indexpattern_generate(date_start3, date_end) == 'gracc.osg.raw-*', "Assertion Error, {0}-{1} test failed".format(date_start3, date_end)
    print "Passed date array tests"

    assert indexpattern_generate(date_datestart1, date_dateend) == 'gracc.osg.raw-2016.06', "Assertion Error, {0}-{1} test failed".format(date_datestart1, date_dateend)
    assert indexpattern_generate(date_datestart2, date_dateend) == 'gracc.osg.raw-2016.*', "Assertion Error, {0}-{1} test failed".format(date_datestart2, date_dateend)
    assert indexpattern_generate(date_datestart3, date_dateend) == 'gracc.osg.raw-*', "Assertion Error, {0}-{1} test failed".format(date_datestart3, date_dateend)
    print "Passed datetime.date tests"

    assert indexpattern_generate(datestringslash, date_dateend) == 'gracc.osg.raw-2016.06', "Assertion Error, {0}-{1} test failed".format(datestringslash, date_dateend)
    assert indexpattern_generate(datestringdash, date_dateend) == 'gracc.osg.raw-2016.06', "Assertion Error, {0}-{1} test failed".format(datestringslash, date_dateend)
    print "Passed date string tests (/ and -)"

    dateparse_fulldate = dateparse(fulldate,time=True)
    assert indexpattern_generate(dateparse_fulldate, date_dateend) == 'gracc.osg.raw-2016.06', "Assertion Error, {0}-{1} test failed".format(datestringslash, date_dateend)
    print "Passed full date time test"

    print "This next test should fail with a TypeError."
    try:
        indexpattern_generate(datebreak, date_dateend)
    except TypeError as e:
        print "A TypeError was raised.  The error was the following:"
        print e
