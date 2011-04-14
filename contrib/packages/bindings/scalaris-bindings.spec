# norootforbuild

%define pkg_version 0.0.1
Name:           scalaris-bindings
Summary:        Scalable Distributed key-value store
Version:        %{pkg_version}
Release:        1
License:        ASL 2.0
Group:          Productivity/Databases/Servers
URL:            http://code.google.com/p/scalaris
Source0:        %{name}-%{version}.tar.gz
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-build
BuildRequires:  ant
BuildRequires:  java-devel >= 1.6.0
# note: erlang is still needed for configure
BuildRequires:  erlang >= R13B01
BuildRequires:  ruby >= 1.8

##########################################################################################
## Fedora, RHEL or CentOS
##########################################################################################
%if 0%{?fedora_version} || 0%{?rhel_version} || 0%{?centos_version}
BuildRequires:  pkgconfig
BuildRequires:  ruby(abi) >= 1.8
%if 0%{?fedora_version} >= 12 || 0%{?rhel_version} >= 600
%define with_python 1
%endif
%if 0%{?fedora_version} >= 13
%define with_python3 1
BuildRequires:  python3-setuptools python-tools
%endif
%endif

##########################################################################################
## Mandrake, Mandriva
##########################################################################################
%if 0%{?mandriva_version} || 0%{?mdkversion}
BuildRequires:  pkgconfig
BuildRequires:  erlang-stack >= R13B01
%define with_python 1
%endif

###########################################################################################
# SuSE, openSUSE
###########################################################################################
%if 0%{?suse_version}
%if 0%{?suse_version} >= 1110
%define with_python 1
%endif
%if 0%{?suse_version} >= 1120
%define with_python3 1
# these macros are not integrated yet:
%global python3_ver  %(python3 -c "import sys; v=sys.version_info[:2]; print('%%d.%%d'%%v)" 2>/dev/null || echo PYTHON-NOT-FOUND)
%global python3_prefix  %(python3 -c "import sys; print(sys.prefix)" 2>/dev/null || echo PYTHON-NOT-FOUND)
%global python3_libdir   %{python3_prefix}/%{_lib}/python%{python3_ver}
%global python3_sitedir  %{python3_libdir}/site-packages
%endif
BuildRequires:  pkg-config
BuildRequires:  rubygems_with_buildroot_patch
%rubygems_requires
%if 0%{?suse_version} >= 1130 
BuildRequires:  ruby(abi) >= 1.8
%else
# py_requires is no longer needed since 11.3
%py_requires
%endif
%endif

%{!?rb_sitelib: %global rb_sitelib %(ruby -rrbconfig -e 'puts Config::CONFIG["sitelibdir"] ')}

%if 0%{?with_python}
BuildRequires:  python-devel >= 2.6
%{!?python_sitelib: %global python_sitelib %(python -c 'from distutils.sysconfig import get_python_lib; print (get_python_lib())')}
%endif

%if 0%{?with_python3}
BuildRequires:  python3-devel
%{!?python3_sitelib: %global python3_sitelib %(python3 -c 'from distutils.sysconfig import get_python_lib; print (get_python_lib())')}
%endif

%description
Scalaris is a scalable, transactional, distributed key-value store. It
can be used for building scalable services. Scalaris uses a structured
overlay with a non-blocking Paxos commit protocol for transaction
processing with strong consistency over replicas. Scalaris is
implemented in Erlang.

%package -n scalaris-java
Summary:    Java-API and Java-Client for scalaris
Group:      Productivity/Databases/Clients
Requires:   jre >= 1.6.0
%if 0%{?suse_version} || 0%{?mandriva_version} >= 2009 || 0%{?mdkversion} >= 200900
Requires:   erlang-jinterface >= R13B01
%else
Requires:   erlang
%endif
Requires:   jakarta-commons-cli
Requires:   %{name} == %{version}-%{release}
BuildArch:  noarch

%description -n scalaris-java
Java Bindings and Command line client for scalaris

%package -n ruby-scalaris
Summary:    Ruby API for scalaris and ruby client
Group:      Productivity/Databases/Clients
Requires:   ruby(abi) >= 1.8
Requires:   rubygem-json

%description -n ruby-scalaris
Ruby bindings and ruby client

%if 0%{?with_python}
%package -n python-scalaris
Summary:    Python API for scalaris and python client
Group:      Productivity/Databases/Clients
Requires:   python >= 2.6
BuildArch:  noarch

%description -n python-scalaris
Python bindings and python client
%endif

%if 0%{?with_python3}
%package -n python3-scalaris
Summary:    Python3 API for scalaris and python3 client
Group:      Productivity/Databases/Clients
Requires:   python3
BuildArch:  noarch

%description -n python3-scalaris
Python3 bindings and python3 client
%endif

%prep
%setup -q -n %{name}-%{version}

%build
./configure --prefix=%{_prefix} \
    --exec-prefix=%{_exec_prefix} \
    --bindir=%{_bindir} \
    --sbindir=%{_sbindir} \
    --sysconfdir=%{_sysconfdir} \
    --datadir=%{_datadir} \
    --includedir=%{_includedir} \
    --libdir=%{_prefix}/lib \
    --libexecdir=%{_libexecdir} \
    --localstatedir=%{_localstatedir} \
    --sharedstatedir=%{_sharedstatedir} \
    --mandir=%{_mandir} \
    --infodir=%{_infodir} \
    --docdir=%{_docdir}/scalaris \
%if 0%{?with_python3}
    --with-python3-sitelibdir=%{python3_sitelib} \
%endif
    --with-ruby-sitelibdir=%{rb_sitelib}
make java
%if 0%{?with_python}
make python-compile
%endif
%if 0%{?with_python3}
make python3-compile
%endif

%install
# see http://en.opensuse.org/Java/Packaging/Cookbook#bytecode_version_error
export NO_BRP_CHECK_BYTECODE_VERSION=true
rm -rf $RPM_BUILD_ROOT
make install-java DESTDIR=$RPM_BUILD_ROOT
make install-ruby DESTDIR=$RPM_BUILD_ROOT
%if 0%{?with_python}
make install-python DESTDIR=$RPM_BUILD_ROOT
%endif
%if 0%{?with_python3}
make install-python3 DESTDIR=$RPM_BUILD_ROOT
%endif

%clean
rm -rf $RPM_BUILD_ROOT

%files -n scalaris-java
%defattr(-,root,root)
%{_javadir}/scalaris
%dir %{_sysconfdir}/scalaris
%config(noreplace) %{_sysconfdir}/scalaris/scalaris-java.conf
%config %{_sysconfdir}/scalaris/scalaris-java.conf.sample
%config(noreplace) %{_sysconfdir}/scalaris/scalaris.properties
%{_bindir}/scalaris

%files -n ruby-scalaris
%defattr(-,root,root)
%{_bindir}/scalaris-ruby
%{rb_sitelib}/scalaris.rb

%if 0%{?with_python}
%files -n python-scalaris
%defattr(-,root,root)
%{_bindir}/scalaris-python
%{python_sitelib}/Scalaris.py
%{python_sitelib}/Scalaris.pyc
%{python_sitelib}/Scalaris.pyo
%endif

%if 0%{?with_python3}
%files -n python3-scalaris
%defattr(-,root,root)
%{_bindir}/scalaris-python3
%if 0%{?suse_version}
%dir %{python3_sitelib}
%dir %{python3_sitelib}/..
%endif
%{python3_sitelib}/Scalaris.py
%{python3_sitelib}/Scalaris.pyc
%{python3_sitelib}/Scalaris.pyo
%endif

%changelog
* Thu Apr 14 2011 Nico Kruber <kruber@zib.de>
- Initial package
