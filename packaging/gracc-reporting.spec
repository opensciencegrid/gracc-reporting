%define name gracc-reporting
%define version 2.0
%define unmangled_version 2.0 
%define release 1
%define _rpmfilename %%{ARCH}/%%{NAME}-%%{VERSION}.%%{ARCH}.rpm

Summary: 	GRACC Email Reporting Libraries
Name: 		%{name}
Version: 	%{version}
Release: 	%{release}
Source0: 	%{name}-%{unmangled_version}.tar.gz
License: 	ASL 2.0
BuildRoot: 	%{_tmppath}/%{name}-%{version}-%{release}-buildroot
Prefix: 	%{_prefix}
BuildArch: 	noarch
Url: 		https://github.com/opensciencegrid/gracc-reporting

# BuildRequires:  systemd
BuildRequires:  python-setuptools
BuildRequires:  python-srpm-macros 
BuildRequires:  python-rpm-macros 
BuildRequires:  python2-rpm-macros 
BuildRequires:  epel-rpm-macros
Requires:       python-elasticsearch-dsl
Requires:	python-elasticsearch
Requires:       python-dateutil
Requires:   python-toml
Requires(pre): shadow-utils

%description
gracc-reporting is a set of reports that collect and present data from the Open Science Grid accounting system GRACC.

%prep
test ! -d %{buildroot} || {
	rm -rf %{buildroot}
}
%setup -n %{name}-%{unmangled_version} -n %{name}-%{unmangled_version}

%build
%{py2_build}

%install
%{py2_install}

# Install config and html_template files in /etc/gracc-reporting
install -d -m 0755 %{buildroot}/%{_sysconfdir}/gracc-reporting/config/
install -d -m 0755 $RPM_BUILD_ROOT/%{_sysconfdir}/gracc-reporting/html_templates/

# Install doc files to /usr/share/docs/gracc-reporting
install -d -m 0755 %{buildroot}/%{_defaultdocdir}/gracc-reporting/ 
install -m 0744 docs/* %{buildroot}/%{_defaultdocdir}/gracc-reporting/


%files
# Permissions
%defattr(-, root, root)

# Python package files
%{python2_sitelib}/gracc_reporting
%{python2_sitelib}/gracc_reporting-%{version}-py2.7.egg-info

# Include config and doc files
%doc %{_defaultdocdir}/gracc-reporting/*

%clean
rm -rf $RPM_BUILD_ROOT

%changelog
* Tue Jun 26 2018 Shreyas Bhat <sbhat@fnal.gov> - 2.0.1
- Took out configs, executables, and html templates, as those will be packaged separately
