# Build RPM
# Centos 7 base image
FROM centos:7 as builder

# Version
ARG version

# Setup:  Get EPEL
RUN rpm -Uvh https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm

# Setup:  Install RPM tools 
RUN yum install -y rpm-build rpmdevtools python-setuptools python-srpm-macros python-rpm-macros python2-rpm-macros epel-rpm-macros

# Set up directory structure
RUN mkdir -p ~/rpmbuild/{RPMS,SRPMS,BUILD,SOURCES,SPECS,tmp}

COPY gracc-reporting-$version.tar.gz /tmp/gracc-reporting-$version.tar.gz 
COPY gracc-reporting.spec /tmp

RUN cp /tmp/gracc-reporting-$version.tar.gz ~/rpmbuild/SOURCES/
RUN cp /tmp/gracc-reporting.spec ~/rpmbuild/SPECS/

WORKDIR ~/rpmbuild/SOURCES
RUN rpmbuild -ba ~/rpmbuild/SPECS/gracc-reporting.spec

# Build base image
FROM centos:7 as base
ARG version
COPY --from=builder /root/rpmbuild/RPMS/noarch/gracc-reporting-$version.noarch.rpm /tmp

# Set up all package managers and repos, make sure they're up to date
RUN rpm -Uvh https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm \
&& curl -o /etc/yum.repos.d/djw8605-GRACC-epel-7.repo https://copr.fedorainfracloud.org/coprs/djw8605/GRACC/repo/epel-7/djw8605-GRACC-epel-7.repo \
&& yum -y update

# Get PIP. We need it for now to grab PyPI's urllib3, since SL7 and CentOS' python-urllib3 is broken for SSL
RUN yum -y install python-pip && pip install -U pip

# Install gracc-reporting
RUN yum -y install /tmp/gracc-reporting-$version.noarch.rpm

# Remove python-urllib3, install PyPi urllib3. Eventually, hopefully this will go away
RUN rpm -qa | grep python-urllib3 | xargs rpm -e --nodeps && pip install urllib3
