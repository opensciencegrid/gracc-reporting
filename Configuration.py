import sys
import re
import ConfigParser
class Configuration:
    """Provides access to configuration"""
    def __init__(self):
        self.config=ConfigParser.ConfigParser()
    def configure(self,fn):
        self.config.read([fn,])

def checkRequiredArguments(opts, parser):
    """Checks for missing command line options:
    Args:
        opts(Values) - optparse.Values
        parser(OptionParser) - optparse.OptionParser
    """
    missing_options = []
    for option in parser.option_list:
        if re.match(r'.*\(required\)$', option.help) and eval('opts.' + option.dest) == None:
            missing_options.extend(option._long_opts)
    if len(missing_options) > 0:
        parser.error('Missing option: ' + str(missing_options))

if __name__=="__main__":
    config=Configuration()
    config.configure(sys.argv[1])
    print config.config.sections()
    print config.config.get("main_db", "hostname")
    print config.config.get("query", "OSG_flocking_probe_list")
