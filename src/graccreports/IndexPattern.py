#!/usr/bin/python

from TimeUtils import TimeUtils

def indexpattern_generate(start, end, raw=False, allraw=False):
    """Function to return the proper index pattern for queries to elasticsearch on gracc.opensciencegrid.org.  This improves performance by not just using a general index pattern unless absolutely necessary.
    This will especially help with reports, for example.

    This function assumes that the date being passed in has been split into a list with [yyyy,mm,dd] format.  This gets tested and cleaned up in the called dateparse function.
    """

    if allraw:
        return 'gracc.osg.raw-*'

    if not raw and not allraw:      # Default
        return 'gracc.osg.summary'

    # Raw is True, allraw is False

    t = TimeUtils()

    basepattern = 'gracc.osg.raw-'

    if start.year == end.year:
        basepattern += '{0}.'.format(str(start.year))
        if start.month == end.month:
            if len(str(start.month)) == 1:
                add = '0{0}'.format(str(start.month))
            else:
                add = str(start.month)
            basepattern += '{0}'.format(add)
        else:
            basepattern += '*'
    else:
        basepattern += '*'

    return basepattern


if __name__ == "__main__":
    import datetime

    date_dateend = datetime.date(2016, 06, 12)
    date_datestart1 = datetime.datetime(2016, 06, 10)
    date_datestart2 = datetime.date(2016, 5, 10)
    date_datestart3 = datetime.date(2015, 05, 10)

    datebreak = '20160205'

    # gracc.osg.raw-YYYY.MM

    assert indexpattern_generate(date_datestart1, date_dateend, raw=True) == 'gracc.osg.raw-2016.06', "Assertion Error, {0}-{1} test failed".format(date_datestart1, date_dateend)
    assert indexpattern_generate(date_datestart2, date_dateend, raw=True) == 'gracc.osg.raw-2016.*', "Assertion Error, {0}-{1} test failed".format(date_datestart2, date_dateend)
    assert indexpattern_generate(date_datestart3, date_dateend, raw=True) == 'gracc.osg.raw-*', "Assertion Error, {0}-{1} test failed".format(date_datestart3, date_dateend)
    print "Passed datetime.date tests"


    print "This next test should fail with a AttributeError."
    try:
        indexpattern_generate(datebreak, date_dateend, raw=True)
    except AttributeError as e:
        print "An AttributeError was raised.  The error was the following:"
        print e

    print "Testing allraw and summary indices"
    assert indexpattern_generate(date_datestart1, date_dateend, raw=False) == 'gracc.osg.summary'
    assert indexpattern_generate(date_datestart1, date_dateend, allraw=True) == 'gracc.osg.raw-*'
    print "Passed allraw and summary tests"
