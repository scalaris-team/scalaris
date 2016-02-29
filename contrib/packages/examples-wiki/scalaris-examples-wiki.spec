# norootforbuild

%define pkg_version 0.9.0+git
Name:           scalaris-examples-wiki
Summary:        Wikipedia on Scalaris example
Version:        %{pkg_version}
Release:        1
License:        Apache-2.0
Group:          Productivity/Databases/Servers
URL:            http://scalaris.zib.de
Source0:        %{name}-%{version}.tar.gz
Source100:      checkout.sh
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-build
BuildRequires:  ant
BuildRequires:  java-devel >= 1.6.0
BuildRequires:  scalaris-java >= 0.8.0

##########################################################################################
## Fedora, RHEL or CentOS
##########################################################################################
%if 0%{?fedora_version} || 0%{?rhel_version} || 0%{?centos_version}
# the wiki includes some libraries which are normally not allowed in noarch packages:
%global _binaries_in_noarch_packages_terminate_build 0
# we also do not need a debuginfo sub package
%global debug_package %{nil}
%if 0%{?centos_version} >= 600 || 0%{?rhel_version} >= 600 || 0%{?fedora_version} >= 17
%define with_tomcat5 0
%else
%define with_tomcat5 1
%endif
%if 0%{?fedora_version} >= 19
%define with_tomcat6 0
%else
%if 0%{?centos_version} >= 600 || 0%{?rhel_version} >= 600 || 0%{?fedora_version}
%define with_tomcat6 1
%else
%define with_tomcat6 0
%endif
%endif
%if 0%{?fedora_version} >= 16
%define with_tomcat7 1
%else
%define with_tomcat7 0
%endif
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
%if 0%{?sles_version} == 10 || 0%{?suse_version} >= 1220
%define with_tomcat6 0
%else
%define with_tomcat6 1
%endif
%if 0%{?suse_version} >= 1220
%define with_tomcat7 1
%else
%define with_tomcat7 0
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
Requires:   scalaris-java >= 0.8.0
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
Requires:   scalaris-java >= 0.8.0
BuildArch:  noarch

%description -n scalaris-examples-wiki-tomcat6
This web application demonstrates the use of Scalaris as a data-store back-end for a
Wikipedia-like application.
%endif

%if 0%{?with_tomcat7}
%package -n scalaris-examples-wiki-tomcat7
Summary:    Wikipedia on Scalaris example using tomcat7
Group:      Productivity/Networking/Web/Servers
Requires:   tomcat >= 7.0.0
Requires:   scalaris-java >= 0.8.0
BuildArch:  noarch

%description -n scalaris-examples-wiki-tomcat7
This web application demonstrates the use of Scalaris as a data-store back-end for a
Wikipedia-like application.
%endif

%prep
%setup -q -n %{name}-%{version}

%build
export ANT_OPTS="-Dfile.encoding=utf8 -Dant.build.javac.source=1.6 -Dant.build.javac.target=1.6"

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
%if 0%{?with_tomcat7}
mkdir -p %{buildroot}/usr/share/tomcat/webapps
cp -r scalaris-wiki %{buildroot}/usr/share/tomcat/webapps/scalaris-wiki
%endif

%clean
rm -rf $RPM_BUILD_ROOT

%if 0%{?with_tomcat5}
%files -n scalaris-examples-wiki-tomcat5
%defattr(-,root,root,-)
%dir /usr/share/tomcat5
%dir /usr/share/tomcat5/webapps
/usr/share/tomcat5/webapps/scalaris-wiki
%endif

%if 0%{?with_tomcat6}
%files -n scalaris-examples-wiki-tomcat6
%defattr(-,root,root,-)
%dir /usr/share/tomcat6
%dir /usr/share/tomcat6/webapps
/usr/share/tomcat6/webapps/scalaris-wiki
%endif

%if 0%{?with_tomcat7}
%files -n scalaris-examples-wiki-tomcat7
%defattr(-,root,root,-)
%dir /usr/share/tomcat
%dir /usr/share/tomcat/webapps
/usr/share/tomcat/webapps/scalaris-wiki
%endif

%changelog
