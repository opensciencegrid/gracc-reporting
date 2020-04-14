"""
Simple number formatter

>>> niceNum(123567.0, 1)
'123,567.0'

>>> niceNum(123567.0, 0)
'123,567'

>>> niceNum(10.53, 0)
'11'

>>> niceNum(10130.56, 1)
'10,130.6'


"""

def niceNum(num, precision=0):
    """Returns a string representation for a floating point number
    that is rounded to the given precision and displayed with
    commas and spaces."""
    return format(round(num, precision), ',.{}f'.format(precision))
