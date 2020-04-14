"""Generate gracc-reporting index patterns"""

from datetime import datetime

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
        print(errmsg)
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
    