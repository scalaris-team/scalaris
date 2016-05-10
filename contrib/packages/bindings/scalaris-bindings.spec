# norootforbuild

%define pkg_version 0.9.0+git
Name:           scalaris-bindings
Summary:        Scalable Distributed key-value store
Version:        %{pkg_version}
Release:        1
License:        Apache-2.0
Group:          Productivity/Databases/Servers
URL:            http://scalaris.zib.de
Source0:        scalaris-%{version}.tar.gz
Source100:      checkout.sh
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-build
BuildRequires:  ant
BuildRequires:  java-devel >= 1.6.0
BuildRequires:  ruby >= 1.8

##########################################################################################
## Fedora, RHEL or CentOS
##########################################################################################
%if 0%{?fedora_version} || 0%{?rhel_version} || 0%{?centos_version}
BuildRequires:  erlang-erts >= R14B04, erlang-kernel, erlang-stdlib, erlang-compiler, erlang-crypto, erlang-edoc, erlang-inets, erlang-ssl, erlang-tools, erlang-xmerl, erlang-os_mon
BuildRequires:  pkgconfig
%if 0%{?fedora_version} >= 19 || 0%{?rhel_version} >= 700 || 0%{?centos_version} >= 700
%define with_cpp 1
# our cpp-api currently only provides a static library for which
# find-debuginfo.sh cannot find the debug infos, see
# https://fedoraproject.org/wiki/Packaging:Debuginfo
%global debug_package %{nil}
BuildRequires:  ruby(release) >= 1.8
%else
%define with_cpp 0
BuildRequires:  ruby(abi) >= 1.8
%endif
%if 0%{?fedora_version} >= 12 || 0%{?centos_version} >= 600
%define with_python 1
%define with_python_doc_html 1
%define with_python_doc_pdf 0
%endif
%if 0%{?rhel_version} >= 600
%define with_python 1
%define with_python_doc_html 0
%define with_python_doc_pdf 0
%endif
%if 0%{?fedora_version} >= 13
%define with_python3 1
BuildRequires:  python3-setuptools python-tools
%endif
%endif

###########################################################################################
# SuSE, openSUSE
###########################################################################################
%if 0%{?suse_version}
# note: erlang is still needed for configure
BuildRequires:  erlang >= R14B04
BuildRequires:  pkg-config
%if 0%{?suse_version} >= 1110 || 0%{?sles_version} >= 11 
%define with_python 1
%if 0%{?suse_version} == 1315
%define with_python_doc_html 0
%else
%define with_python_doc_html 1
%endif
%define with_python_doc_pdf 1
%if 0%{?suse_version} > 1220
%define with_python_doc_pdf 0
%endif
%if 0%{?suse_version} >= 1130 
%else
# py_requires is no longer needed since 11.3
%py_requires
%endif
%endif
%if 0%{?suse_version} >= 1120
%define with_python3 1
# these macros are not integrated yet:
%global python3_ver      %(python3 -c "import sys; v=sys.version_info[:2]; print('%%d.%%d'%%v)" 2>/dev/null || echo PYTHON-NOT-FOUND)
%global python3_prefix   %(python3 -c "import sys; print(sys.prefix)" 2>/dev/null || echo PYTHON-NOT-FOUND)
%global python3_libdir   %{python3_prefix}/%{_lib}/python%{python3_ver}
%global python3_sitedir  %{python3_libdir}/site-packages
%endif
%if 0%{?suse_version} > 1210
BuildRequires:  python3-2to3
%endif
%if %{?suse_version} >= 1130 && %{?suse_version} <= 1310
BuildRequires:  ruby(abi) >= 1.8
%endif
%if 0%{?suse_version} >= 1310 || 0%{?sles_version} >= 12
%define with_cpp 1
%else
%define with_cpp 0
%endif
%endif

%{!?rb_sitelib: %global rb_sitelib %(ruby -rrbconfig -e 'puts Config::CONFIG["sitelibdir"] ')}

%if 0%{?with_cpp}
BuildRequires:  automake
BuildRequires:  boost-devel >= 1.47
BuildRequires:  gcc-c++
%endif

%if 0%{?with_python}
%if 0%{?with_python_doc_html}
BuildRequires:  epydoc graphviz graphviz-gd
%endif
%if 0%{?with_python_doc_pdf}
BuildRequires:  epydoc texlive-latex
%endif
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
Summary:    Java-API and Java-Client for Scalaris
Group:      Productivity/Databases/Clients
Requires:   jre >= 1.6.0
Requires:   net-tools
%if 0%{?fedora_version} || 0%{?rhel_version} || 0%{?centos_version} || 0%{?suse_version} >= 1310
Requires:       which
%else
Requires:       util-linux
%endif
%if 0%{?sles_version} == 10 || 0%{?sles_version} == 11
# once noarch, always noarch on SLE <= 11
%else
BuildArch:  noarch
%endif

%description -n scalaris-java
Java Bindings and command line client for Scalaris

%if 0%{?with_cpp}
%package -n libscalaris-devel
Summary:    C++-API for Scalaris
Group:      Productivity/Databases/Clients
Provides:   libscalaris-static = %{version}-%{release}
Requires:   boost-devel >= 1.47

%description -n libscalaris-devel
C++ Bindings for Scalaris
%endif

%package -n ruby-scalaris
Summary:    Ruby-API and Ruby-client for Scalaris
Group:      Productivity/Databases/Clients
%if 0%{?fedora_version} >= 19 || 0%{?rhel_version} >= 700 || 0%{?centos_version} >= 700
Requires:   ruby(release) >= 1.8
%else
Requires:   ruby(abi) >= 1.8
%endif
Requires:   rubygems
Requires:   rubygem-json >= 1.4.1
%if 0%{?rhel_version} || 0%{?centos_version}
# (Recommends tag not supported by RHEL and CentOS 5-7)
%else
# Drag in the pure Ruby implementation too, so that jruby has something to
# fall back to: https://bugzilla.redhat.com/show_bug.cgi?id=1219502
Recommends: rubygem-json_pure >= 1.4.1
%endif

%description -n ruby-scalaris
Ruby bindings and Ruby command line client for Scalaris

%if 0%{?with_python}
%package -n python-scalaris
Summary:    Python-API and Python-client for Scalaris
Group:      Productivity/Databases/Clients
Requires:   python >= 2.6
%if 0%{?sles_version} == 10 || 0%{?sles_version} == 11
# once noarch, always noarch on SLE <= 11
%else
BuildArch:  noarch
%endif

%description -n python-scalaris
Python bindings and Python command line client for Scalaris
%endif

%if 0%{?with_python3}
%package -n python3-scalaris
Summary:    Python3-API and Python3-client for Scalaris
Group:      Productivity/Databases/Clients
Requires:   python3
%if 0%{?sles_version} == 10 || 0%{?sles_version} == 11
# once noarch, always noarch on SLE <= 11
%else
BuildArch:  noarch
%endif

%description -n python3-scalaris
Python3 bindings and Python3 command line client for Scalaris
%endif

%prep
%setup -q -n scalaris-%{version}

%build
export ANT_OPTS="-Dfile.encoding=utf8 -Dant.build.javac.source=1.6 -Dant.build.javac.target=1.6"

export CFLAGS="%{optflags}"
export CXXFLAGS=$CFLAGS

%if 0%{?fedora_version} >= 18
export PATH="%{_bindir}:$PATH"
%endif
./configure --prefix=%{_prefix} \
    --exec-prefix=%{_exec_prefix} \
    --bindir=%{_bindir} \
    --sbindir=%{_sbindir} \
    --sysconfdir=%{_sysconfdir} \
    --datadir=%{_datadir} \
    --includedir=%{_includedir} \
    --libdir=%{_libdir} \
    --libexecdir=%{_libexecdir} \
    --localstatedir=%{_localstatedir} \
    --sharedstatedir=%{_sharedstatedir} \
    --mandir=%{_mandir} \
    --infodir=%{_infodir} \
    --docdir=%{_docdir}/scalaris \
%if 0%{?with_cpp} == 0
    --disable-cpp \
%endif
    --with-ruby-sitelibdir=%{rb_sitelib}
make java
make java-doc
%if 0%{?with_cpp}
make cpp
%endif
%if 0%{?with_python}
make python
%endif
%if 0%{?with_python3}
make python3
%endif

%install
# see http://en.opensuse.org/openSUSE:Packaging_Java#bytecode_version_error
export NO_BRP_CHECK_BYTECODE_VERSION=true
rm -rf $RPM_BUILD_ROOT
make install-java DESTDIR=$RPM_BUILD_ROOT
make install-java-doc DESTDIR=$RPM_BUILD_ROOT
make install-ruby DESTDIR=$RPM_BUILD_ROOT
%if 0%{?with_python}
make install-python DESTDIR=$RPM_BUILD_ROOT
%if 0%{?with_python_doc_html}
make install-python-doc-html DESTDIR=$RPM_BUILD_ROOT
%endif
%if 0%{?with_python_doc_pdf}
make install-python-doc-pdf DESTDIR=$RPM_BUILD_ROOT
%endif
%endif
%if 0%{?with_python3}
make install-python3 DESTDIR=$RPM_BUILD_ROOT
%endif
%if 0%{?with_cpp}
make install-cpp DESTDIR=$RPM_BUILD_ROOT
%endif

%clean
rm -rf $RPM_BUILD_ROOT

%files -n scalaris-java
%defattr(-,root,root,-)
%{_javadir}/scalaris
%dir %{_sysconfdir}/scalaris
%config(noreplace) %{_sysconfdir}/scalaris/scalaris-java.conf
%config %{_sysconfdir}/scalaris/scalaris-java.conf.sample
%config(noreplace) %{_sysconfdir}/scalaris/scalaris.properties
%{_bindir}/scalaris
%dir %{_docdir}/scalaris/
%doc %{_docdir}/scalaris/java-api
%{_sysconfdir}/init.d/scalaris-monitor
%{_sysconfdir}/init.d/scalaris-first-monitor

%if 0%{?with_cpp}
%files -n libscalaris-devel
%defattr(-,root,root,-)
%{_includedir}/scalaris
%{_libdir}/libscalaris.a
%endif

%files -n ruby-scalaris
%defattr(-,root,root,-)
%{_bindir}/scalaris-ruby
%{rb_sitelib}/scalaris.rb
%{rb_sitelib}/scalaris_client.rb

%if 0%{?with_python}
%files -n python-scalaris
%defattr(-,root,root,-)
%{_bindir}/scalaris-python
%{python_sitelib}/*
%if 0%{?with_python_doc_html} || 0%{?with_python_doc_pdf}
%doc %{_docdir}/scalaris/python-api
%endif
%endif

%if 0%{?with_python3}
%files -n python3-scalaris
%defattr(-,root,root,-)
%{_bindir}/scalaris-python3
%if 0%{?suse_version}
%dir %{python3_sitelib}
%dir %{python3_sitelib}/..
%endif
%{python3_sitelib}/*
%endif

%changelog
