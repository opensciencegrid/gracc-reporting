# CentOS 7 base image 
FROM centos:7

# Set up all package managers and repos, make sure they're up to date
RUN rpm -Uvh https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm \
  && curl -o /etc/yum.repos.d/djw8605-GRACC-epel-7.repo https://copr.fedorainfracloud.org/coprs/djw8605/GRACC/repo/epel-7/djw8605-GRACC-epel-7.repo \
  && yum -y update

# Get PIP.  We need it for now to grab PyPI's urllib3, since SL7 and CentOS' python-urllib3 is broken for SSL
RUN yum -y install python-pip && pip install -U pip

# Copy in our RPM and install it
COPY gracc-reporting-0.11-2.noarch.rpm /tmp
RUN yum -y install /tmp/gracc-reporting-0.11-2.noarch.rpm

# Remove python-urllib3, install PyPi urllib3.  Eventually, hopefully this will go away
RUN rpm -qa | grep python-urllib3 | xargs rpm -e --nodeps && pip install urllib3
