"""
Having written a bunch of scientific software, I am always amazed
at how few languages have built in routines for displaying numbers
nicely.  I was doing a little programming in Python and got surprised
again.  I couldn't find any routines for displaying numbers to
a significant number of digits and adding appropriate commas and
spaces to long digit sequences.  Below is my attempt to write
a nice number formatting routine for Python.  It is not particularly
fast.  I suspect building the string by concatenation is responsible
for much of its slowness.  Suggestions on how to improve the 
implementation will be gladly accepted.

                        David S. Harrison
                        (daha@best.com)
"""
import math

# Returns a nicely formatted string for the floating point number
# provided.  This number will be rounded to the supplied accuracy
# and commas and spaces will be added.  I think every language should
# do this for numbers.  Why don't they?  Here are some examples:
# >>> print niceNum(123567.0, 1000)
# 124,000
# >>> print niceNum(5.3918e-07, 1e-10)
# 0.000 000 539 2
# This kind of thing is wonderful for producing tables for
# human consumption.
#
def niceNum(num, precision = 1):
    """Returns a string representation for a floating point number
    that is rounded to the given precision and displayed with
    commas and spaces."""
    accpow = int(math.floor(math.log10(precision)))
    if num < 0:
        digits = int(math.fabs(num/pow(10,accpow)-0.5))
    else:
        digits = int(math.fabs(num/pow(10,accpow)+0.5))
    result = ''
    if digits > 0:
        for i in range(0,accpow):
            if (i % 3)==0 and i>0:
                result = '0,' + result
            else:
                result = '0' + result
        curpow = int(accpow)
        while digits > 0:
            adigit = chr((digits % 10) + ord('0'))
            if (curpow % 3)==0 and curpow!=0 and len(result)>0:
                if curpow < 0:
                    result = adigit + ' ' + result
                else:
                    result = adigit + ',' + result
            elif curpow==0 and len(result)>0:
                result = adigit + '.' + result
            else:
                result = adigit + result
            digits = digits/10
            curpow = curpow + 1
        for i in range(curpow,0):
            if (i % 3)==0 and i!=0:
                result = '0 ' + result
            else:
                result = '0' + result
        if curpow <= 0:
            result = "0." + result
        if num < 0:
            result = '-' + result
    else:
        result = "0"
    return result
