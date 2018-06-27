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

# Install gracc-reporting
RUN yum -y install /tmp/gracc-reporting-$version.noarch.rpm

