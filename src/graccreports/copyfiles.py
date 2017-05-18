import shutil
import os
import pkg_resources
import sys

etcpath = '/etc/gracc-reporting/'
dirs = ['config', 'html_templates']


def main():
    if os.path.exists(etcpath):
        shutil.rmtree(etcpath)

    for d in dirs:
        destpath = '{0}{1}'.format(etcpath, d)
        try:
            os.makedirs(destpath)
        except OSError as e:
            print e
            sys.exit(1)
        files = pkg_resources.resource_listdir('reports', d)
        for f in files:
            fname = pkg_resources.resource_filename('reports',
                                                    '{0}/{1}'.format(d, f))
            try:
                shutil.copy(fname, destpath)
            except OSError as e:
                print e
                sys.exit(1)

    sys.exit(0)
