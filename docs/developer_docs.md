gracc-reporting Developer Documentation
================

These are the developer docs for _gracc-reporting_.  They're by no means complete, but I've attempted to 
note the important info for anyone coming in and trying to write a report.

_gracc-reporting_ consists of a number of libraries meant to be used as helpers on top of which reports
can be built.  The current format of reports (and thus of _gracc-reporting_) is very simply:

1. Query the elasticsearch query endpoint of the GRACC instance
2. Parse those results in a useful manner for your raw data
3. Aggregate the data into a reportable format 
4. Populate the report template (an HTML template)
5. Send the report

Currently, the _gracc-reporting_ libraries assume that queries to the GRACC instance will be made using
Honza Kral's [elasticsearch-dsl-py](https://github.com/elastic/elasticsearch-dsl-py) library, and 
indeed, this is a dependency of the _gracc-reporting_ package.  However, this may change in the 
future, since the ultimate aim is to provide flexibility for future report writers to simply 
send an HTTP POST or GET request to the GRACC instance, or use another API of their choosing.

I'll address each module and what it does first, and then discuss building the package and writing
a report on top of _gracc-reporting_.

# Modules

## ReportUtils.py

This is the main module in _gracc-reporting_.  It exports the [Reporter class](#class-reporter) and a 
few helper functions.


### class Reporter

The Reporter class is the class all gracc-reports are built on.  Instantiating the class, one must
provide:

* report_type (string):  Which report is being run. The name given here must match whatever name you
use in the configuration file.  Gets added as self.report\_type
* config_file (string):  The path to the configuration file. Gets added as dict self.config after parsing
* start and end (string):  Start and end times of report range (given in local TZ).  These can be in 
format that [dateutil.parser](https://dateutil.readthedocs.io/en/stable/parser.html) understands 
(e.g. YYYY-MM-dd HH:mm:ss).  Added as self.start_time and self.end_time (datetime.datetime 
objects) after parsing

You can also provide other optional keyword arguments.  These are the supported ones, along with their 
defaults (any others will result in an error, though that might change):

* althost_key (None): key to look for in config file to designate the elasticsearch host.  If this is 
None, Reporter will assume that the key to look for is 'host'.
* index_key ('index_pattern'): key to look for in config file for the elasticsearch index pattern to
use.  If this is not given, Reporter will look for 'index\_pattern' to be set in the config file.  If
that is not configured, it will simply use gracc.osg.summary.
* vo (None):  Virtual organization to run report on.  Doesn't apply to most OSG reports
* template (None):  HTML template file to use
* logfile (None):  Logfile to use
* is_test (False):  Test mode or not (If True, will send emails only to admins as set in config file 
(test.emails and test.names)
* no_email (False): Don't send any emails at all.  Just run the report
* verbose (False)

These are the main methods of the Reporter class.

#### query

Must be overridden. Define the elasticsearch query using elasticsearch_dsl, return the elasticsearch_dsl 
Search object.

#### run_query:   
Execute the query and check the status code before returning the relevant info (as either a Search 
object to run the scan/scroll API on, or an aggregations object if that's what the query requested).

#### generate_report_file or format_report:

Pick one!  

Use generate_report_file if you're building the data structure yourself (most complex reports need 
this).  Populate _self.text_ with the final HTML.  

Use format_report if you're fine with creating a report as a dict of the columns, that can be used for 
simultaneous CSV and HTML generation.  format_report should return this dict, and send_report will 
handle the HTML/CSV generation in that case.

#### send_report

Will email the report produced by either of the previous methods.  Checks if self.format_report returns
anything.  If not, send_report assumes self.text is populated (presumably by self.generate_report_file), 
and will send that as the HTML report.  Otherwise, it will use the dict returned by self.format_report
and generate the HTML and CSV files, and send those.


#### run_report

Must be overridden. Use it to run all of the above and any other helper methods/functions involved
in the generation of the report.


#### Helper methods

* Reporter.indexpattern_generate will grab the index pattern from the configuration file and will 
try to use IndexPattern.indexpattern\_generate to create a more specific index pattern to optimize 
query speed.
* check_no_email will look at the self.no_email flag, and if it's set, logs some info.
* get_logfile_path tries to set the logfile path to something that's valid for the user running the 
report.  It will try to set the logfile path to, respectively, the file given on the command line, 
the path given in the configuration, the user's home directory, or the current working directory
* __establish_client is a hidden method, but I wanted to mention it because it is where the connection
to the GRACC host is established.  It is not meant to be used in any reports.


### runerror

Function for handling errors during execution of report.  Ideally, all errors are passed to the top 
level of the report, which then has _runerror_ in an *except* clause.  _runerror_ will log the error, 
the traceback, and email the admins (test.emails).

### coroutine

A helper decorator that advances a coroutine to its first yield point.  Adapted from
http://www.dabeaz.com/coroutines/Coroutines.pdf

### get_report_parser

Creates a parser for evaluating command-line options.  Can be called with time options (start, end) by 
default, or without by calling get_report_parser(no_time_options=True).  



## TimeUtils.py

TimeUtils is a library of helper functions, built heavily on datetime,
time, and dateutil, to help with the conversions of timestamps in gracc-
reporting.  Note that in this module, parse_datetime is the only function
that can accept non-UTC timestamps.  epoch_to_datetime assumes you're giving it an epoch time, and 
returns a UTC datetime, and get_epoch_time_range_utc assumes both start_time and end_time are 
UTC datetime objects.


