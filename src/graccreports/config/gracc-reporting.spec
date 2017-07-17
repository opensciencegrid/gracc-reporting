%define name gracc-reporting
%define version 0.11.4
%define unmangled_version 0.11.4
%define release 1
%define _rpmfilename %%{ARCH}/%%{NAME}-%%{VERSION}.%%{ARCH}.rpm

Summary: 	GRACC Email Reports
Name: 		%{name}
Version: 	%{version}
Release: 	%{release}
Source0: 	%{name}-%{unmangled_version}.tar.gz
License: 	ASL 2.0
BuildRoot: 	%{_tmppath}/%{name}-%{version}-%{release}-buildrooty
Prefix: 	%{_prefix}
BuildArch: 	noarch
Url: 		https://github.com/opensciencegrid/gracc-reporting

BuildRequires:  systemd
BuildRequires:  python-setuptools
BuildRequires:  python-srpm-macros 
BuildRequires:  python-rpm-macros 
BuildRequires:  python2-rpm-macros 
BuildRequires:  epel-rpm-macros
Requires:       python-elasticsearch-dsl
Requires:	python-elasticsearch
Requires:       python-dateutil
Requires:	python-psycopg2
Requires:	python-requests
Requires:   python-toml
Requires(pre): shadow-utils

%description
gracc-reporting is a set of reports that collect and present data from the Open Science Grid accounting system GRACC.

%prep
%setup -n %{name}-%{unmangled_version} -n %{name}-%{unmangled_version}

%build
%{py2_build}

%install
%{py2_install}

# Install config and html_template files in /etc/graccreports
install -d -m 0755 $RPM_BUILD_ROOT/%{_sysconfdir}/graccreports/config/
install -m 0744 $RPM_BUILD_ROOT/%{python2_sitelib}/graccreports/config/*.toml $RPM_BUILD_ROOT/%{_sysconfdir}/graccreports/config/
install -d -m 0755 $RPM_BUILD_ROOT/%{_sysconfdir}/graccreports/html_templates/
install -m 0744 $RPM_BUILD_ROOT/%{python2_sitelib}/graccreports/html_templates/*.html $RPM_BUILD_ROOT/%{_sysconfdir}/graccreports/html_templates/

# Install doc files to /usr/share/doc/graccreports
install -d -m 0755 $RPM_BUILD_ROOT/%{_defaultdocdir}/graccreports/
install -m 0744 docs/*.md $RPM_BUILD_ROOT/%{_defaultdocdir}/graccreports/


%files
# Permissions
%defattr(-, root, root)
%attr(755, root, root) %{_bindir}/*

# Python package files
%{python2_sitelib}/graccreports
%{python2_sitelib}/gracc_reporting-%{version}-py2.7.egg-info

# Include config and doc files
%config(noreplace) %{_sysconfdir}/graccreports/config/*.toml
%config(noreplace) %{_sysconfdir}/graccreports/html_templates/*.html
%doc %{_defaultdocdir}/graccreports/*

%clean
rm -rf $RPM_BUILD_ROOT
