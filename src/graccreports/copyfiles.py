import shutil
import os
import pkg_resources
import sys
import argparse
import random
import re


basedir = 'gracc-reporting'
etcpath = os.path.join('/etc', basedir)
dirs = ['config', 'html_templates']


def setup_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('-v', '--verbose', help='Verbose flag',
                        action='store_true')
    parser.add_argument('-d', '--destdir', help='specify destination dir',
                        dest='destdir')
    args = parser.parse_args()
    return args


def test_access(d):
    """Test read-write access to dir"""

    testfilename = 'test{0}.test'.format(random.randint(0, 100))
    testdir = 'test{0}'.format(random.randint(0, 100))

    # Try to create the dir if it doesn't exist
    if not os.path.exists(d):
        try:
            os.makedirs(d)
            cleanup = True
        except OSError as e:
            print "Can't create dir {0}".format(d)
            print e
            return False
    else:
        cleanup = False

    # Can we write a file to the dir?
    try:
        fn = os.path.join(d, testfilename)
        with open(fn, 'w') as f:
            f.write('12345')
        os.unlink(fn)
    except IOError as e:
        print "Permission denied to write to {0}".format(fn)
        print e
        return False

    # Can we create a dir inside that dir?  (Should be the same answer to above)
    try:
        dd = os.path.join(d, testdir)
        os.makedirs(dd)
        shutil.rmtree(dd)
    except OSError as e:
        print "Permission denied to make dir in {0}".format(d)
        print e
        return False

    # If we had to create a dir in the first test, delete it
    if cleanup:
        shutil.rmtree(d)

    return True


def check_usedir(d):
    """Get confirmation to delete current location of config files"""
    answer = raw_input("Directory {0} already exists.  Delete and overwrite"
                       " it? (Y/[n])".format(d))
    if answer == 'Y':
        return True
    else:
        return False


def main():

    trydirs = [etcpath,]
    args = setup_parser()

    # Did we override the default dest dir?
    if args.destdir is not None:
        override = '{0}/{1}'.format(args.destdir, basedir)
        trydirs.insert(0, override)

    # Test our access, get destination dir
    for d in trydirs:
        if test_access(d):
            usedir = d
            print "Writing to {0}".format(d)
            break
    else:
        print "Can't write to any dirs"
        sys.exit(1)

    if os.path.exists(usedir):
        if check_usedir(usedir):
            shutil.rmtree(usedir)
        else:
            print "Not overwriting directory.  Please provide a different " \
                  "directory to use.  Exiting."
            sys.exit(0)

    # Copy files out
    for d in dirs:
        destpath = os.path.join(usedir, d)
        try:
            os.makedirs(destpath)
        except OSError as e:
            print e
            sys.exit(1)

        files = pkg_resources.resource_listdir('graccreports', d)

        for f in files:
            if re.search('\.spec$', f):
                # Skip our spec file.  We don't need to install it outside
                continue

            fname = pkg_resources.resource_filename('graccreports',
                                                    os.path.join(d, f))
            try:
                shutil.copy(fname, destpath)
            except OSError as e:
                print e
                sys.exit(1)

    print "Files copied to {0}".format(usedir)
    sys.exit(0)