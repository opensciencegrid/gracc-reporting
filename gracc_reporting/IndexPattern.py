#!/usr/bin/python

from datetime import datetime


# TODO:  Move tests to a unit test!

def indexpattern_generate(pattern=None, start=None, end=None):
    """Function to return the proper index pattern for queries to
    elasticsearch on gracc.opensciencegrid.org.  This improves performance by
    not just using a general index pattern unless absolutely necessary.
    This will especially help with reports, for example.

    Some examples of patterns are:
    'gracc.osg.raw-%Y.%m' - date-dependent
    'gracc.osg.raw-*' - date-independent (will be returned as-is)
    'gracc.osg.summary' - date-independent (will be returned as-is)

    :param str pattern: Index pattern to parse.  This will be passed through
        datetime.strftime, so if date-dependence is desired, it should follow
        python's time format conventions
    :param datetime start: Start time
    :param datetime end: End time
    :return str: Index Pattern to pass to Elasticsearch
    """
    if pattern is None:
        # Default
        return 'gracc.osg.summary'
    else:
        try:
            # No Date dependence, like 'gracc.osg.summary' or 'gracc.osg.raw-*'
            if pattern == datetime.now().strftime(pattern): return pattern
        except TypeError as e:
            # Pattern isn't a string
            errmsg = "pattern must be a string"
            raise TypeError('{0}\n{1}'.format(errmsg, e))

    # So now we're assuming date dependence in pattern
    try:
        test_indices = [d.strftime(pattern) for d in (start, end)]
    except AttributeError:
        # One of start or end is None or is not a datetime
        errmsg = "Start and end date for indexpattern_generate must be " \
                 "datetime objects if the pattern argument is date-dependent."
        print errmsg
        raise AttributeError(errmsg)

    if test_indices[0] == test_indices[1]:
        return test_indices[0]
    else:
        # Construct the index pattern by comparing the two test indices one
        # character at a time.  Stop when they don't match anymore
        index_pattern_common = ''
        for tup in zip(*test_indices):
            if tup[0] == tup[1]:
                index_pattern_common += tup[0]
            else:
                break
        return '{0}*'.format(index_pattern_common)


if __name__ == "__main__":
    # Testing
    date_dateend = datetime(2016, 06, 12)

    date_datestart1 = datetime(2016, 06, 10)
    date_datestart2 = datetime(2016, 5, 10)
    date_datestart3 = datetime(2015, 05, 10)

    datebreak = '20160205'

    def check_error(errortype, **kwargs):
        try:
            indexpattern_generate(**kwargs)
        except errortype as e:
            print "We expect to see a {0} error here".format(errortype.__name__)
            print e
        except Exception as e:
            raise AssertionError(
                "We should have seen a(n) {0}.  "
                "We saw some other error".format(errortype.__name__)
            )
        else:
            raise AssertionError(
                "Failed date-independent TypeError try-except test.  We "
                "got no errors and we should have")

    # No pattern
    testout= 'gracc.osg.summary'
    assert indexpattern_generate() == testout
    assert indexpattern_generate(start=date_datestart1, end=date_dateend) == testout
    assert indexpattern_generate(start=date_datestart1) == testout
    print "Passed no pattern tests"

    # Pattern is not date-dependent
    pattern1 = 'gracc.osg.summary'
    pattern2 = 'gracc.osg.raw-*'
    pattern_bad = 4
    for p in (pattern1, pattern2):
        assert indexpattern_generate(pattern=p) == p
        assert indexpattern_generate(pattern=p, start=date_datestart1) == p

    for sdate in (None, date_datestart1):
        check_error(TypeError, pattern=pattern_bad, start=sdate)

    print "Passed date-independent pattern tests"

    # Pattern is date-dependent
    pattern_good = 'gracc.osg.raw-%Y.%m'

    check_error(AttributeError, pattern=pattern_good, start=datebreak, end=date_dateend)
    check_error(AttributeError, pattern=pattern_good, start=date_datestart1, end=datebreak)
    check_error(AttributeError, pattern=pattern_good)

    assert indexpattern_generate(pattern=pattern_good, start=date_datestart1, end=date_dateend) == 'gracc.osg.raw-2016.06', \
        "Assertion Error, {0}-{1} test failed".format(date_datestart1, date_dateend)
    assert indexpattern_generate(pattern=pattern_good, start=date_datestart2, end=date_dateend) == 'gracc.osg.raw-2016.0*', \
        "Assertion Error, {0}-{1} test failed".format(date_datestart2, date_dateend)
    assert indexpattern_generate(pattern=pattern_good, start=date_datestart3, end=date_dateend) == 'gracc.osg.raw-201*', \
        "Assertion Error, {0}-{1} test failed".format(date_datestart3, date_dateend)

    print "Passed date-dependent tests"
    print "\nPassed all tests"
