# Note:  Building this on an alpine image because it requires a lot less patching
FROM python:2.7-alpine
# FROM opensciencegrid/osg-wn

# Version
ARG version

# Setup:  Get EPEL -- not needed anymore
# RUN rpm -Uvh https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm

# Setup:  Install python development tools 
# RUN yum install -y python-pip
# RUN pip install -U pip
# Need this next line because python-setuptools in EPEL is too old for pip
# RUN rpm -e --nodeps python-setuptools
# RUN pip install setuptools && pip install wheel

ADD gracc-reporting-$version.tar.gz /tmp/gracc-reporting-$version.tar.gz 
WORKDIR /tmp/gracc-reporting-$version.tar.gz/gracc-reporting-$version/

RUN python setup.py install

