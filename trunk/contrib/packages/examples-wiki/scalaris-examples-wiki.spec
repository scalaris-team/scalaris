# norootforbuild

%define pkg_version 0.4.1+svn
Name:           scalaris-examples-wiki
Summary:        Wikipedia on Scalaris example
Version:        %{pkg_version}
Release:        1
License:        ASL 2.0
Group:          Productivity/Databases/Servers
URL:            http://code.google.com/p/scalaris
Source0:        %{name}-%{version}.tar.gz
Source100:      checkout.sh
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-build
BuildRequires:  ant
BuildRequires:  java-devel >= 1.6.0
BuildRequires:  scalaris-java >= 0.4.1+svn3050

##########################################################################################
## Fedora, RHEL or CentOS
##########################################################################################
%if 0%{?fedora_version} || 0%{?rhel_version} || 0%{?centos_version}
# the wiki includes some libraries which are normally not allowed in noarch packages:
%global _binaries_in_noarch_packages_terminate_build 0
%if 0%{?centos_version} >= 600 || 0%{?rhel_version} >= 600
%define with_tomcat5 0
%else
%define with_tomcat5 1
%endif
%if 0%{?centos_version} >= 600 || 0%{?rhel_version} >= 600 || 0%{?fedora_version}
%define with_tomcat6 1
%else
%define with_tomcat6 0
%endif
%endif

##########################################################################################
## Mandrake, Mandriva
##########################################################################################
%if 0%{?mandriva_version} || 0%{?mdkversion}
%define with_tomcat5 1
%define with_tomcat6 0
%endif

###########################################################################################
# SuSE, openSUSE
###########################################################################################
%if 0%{?suse_version}
%if 0%{?suse_version} >= 1110 
%define with_tomcat5 0
%else
%define with_tomcat5 1
%endif
%if 0%{?sles_version} == 10
%define with_tomcat6 0
%else
%define with_tomcat6 1
%endif
%endif

%description
This web application demonstrates the use of Scalaris as a data-store back-end for a
Wikipedia-like application.

%if 0%{?with_tomcat5}
%package -n scalaris-examples-wiki-tomcat5
Summary:    Wikipedia on Scalaris example using tomcat5
Group:      Productivity/Networking/Web/Servers
Requires:   tomcat5
Requires:   scalaris-java >= 0.4.1+svn3050
BuildArch:  noarch

%description -n scalaris-examples-wiki-tomcat5
This web application demonstrates the use of Scalaris as a data-store back-end for a
Wikipedia-like application.
%endif

%if 0%{?with_tomcat6}
%package -n scalaris-examples-wiki-tomcat6
Summary:    Wikipedia on Scalaris example using tomcat6
Group:      Productivity/Networking/Web/Servers
Requires:   tomcat6
Requires:   scalaris-java >= 0.4.1+svn3050
BuildArch:  noarch

%description -n scalaris-examples-wiki-tomcat6
This web application demonstrates the use of Scalaris as a data-store back-end for a
Wikipedia-like application.
%endif

%prep
%setup -q -n %{name}-%{version}

%build
export ANT_OPTS=-Dfile.encoding=utf8

ln -s %{_javadir}/scalaris/scalaris.jar ./contrib/
export JINTERFACE_VERSION=`ls %{_javadir}/scalaris/lib/ | grep ^OtpErlang- | sed "s|OtpErlang-||" | sed "s|.jar||"`
ln -s %{_javadir}/scalaris/lib/OtpErlang-$JINTERFACE_VERSION.jar ./contrib/
ant build

%install
# see http://en.opensuse.org/Java/Packaging/Cookbook#bytecode_version_error
export NO_BRP_CHECK_BYTECODE_VERSION=true
rm -rf $RPM_BUILD_ROOT

%if 0%{?with_tomcat5}
mkdir -p %{buildroot}/usr/share/tomcat5/webapps
cp -r scalaris-wiki %{buildroot}/usr/share/tomcat5/webapps/scalaris-wiki
%endif
%if 0%{?with_tomcat6}
mkdir -p %{buildroot}/usr/share/tomcat6/webapps
cp -r scalaris-wiki %{buildroot}/usr/share/tomcat6/webapps/scalaris-wiki
%endif

%clean
rm -rf $RPM_BUILD_ROOT

%if 0%{?with_tomcat5}
%files -n scalaris-examples-wiki-tomcat5
%defattr(-,root,root)
%dir /usr/share/tomcat5
%dir /usr/share/tomcat5/webapps
/usr/share/tomcat5/webapps/scalaris-wiki
%endif

%if 0%{?with_tomcat6}
%files -n scalaris-examples-wiki-tomcat6
%defattr(-,root,root)
%dir /usr/share/tomcat6
%dir /usr/share/tomcat6/webapps
/usr/share/tomcat6/webapps/scalaris-wiki
%endif

%changelog
* Fri Jun 17 2011 Nico Kruber <kruber@zib.de>
- Initial package
