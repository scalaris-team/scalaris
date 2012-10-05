# norootforbuild

%define pkg_version 0.5.0+svn
Name:           scalaris-bindings
Summary:        Scalable Distributed key-value store
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
BuildRequires:  ruby >= 1.8

##########################################################################################
## Fedora, RHEL or CentOS
##########################################################################################
%if 0%{?fedora_version} || 0%{?rhel_version} || 0%{?centos_version}
BuildRequires:  erlang-erts >= R13B01, erlang-kernel, erlang-stdlib, erlang-compiler, erlang-crypto, erlang-edoc, erlang-inets, erlang-ssl, erlang-tools, erlang-xmerl
BuildRequires:  pkgconfig
BuildRequires:  ruby(abi) >= 1.8
%if 0%{?fedora_version} >= 12
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

##########################################################################################
## Mandrake, Mandriva
##########################################################################################
%if 0%{?mandriva_version} || 0%{?mdkversion}
# note: erlang is still needed for configure
BuildRequires:  erlang-base >= R13B01, erlang-compiler, erlang-crypto, erlang-edoc, erlang-inets, erlang-ssl, erlang-tools, erlang-xmerl
BuildRequires:  pkgconfig
%define with_python 1
%define with_python_doc_html 0
%define with_python_doc_pdf 0
%endif

###########################################################################################
# SuSE, openSUSE
###########################################################################################
%if 0%{?suse_version}
# note: erlang is still needed for configure
BuildRequires:  erlang >= R13B01
BuildRequires:  pkg-config
%if 0%{?suse_version} >= 1110 || 0%{?sles_version} >= 11 
%define with_python 1
%define with_python_doc_html 1
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
%if 0%{?suse_version} >= 1130 
BuildRequires:  ruby(abi) >= 1.8
%endif
%endif

%{!?rb_sitelib: %global rb_sitelib %(ruby -rrbconfig -e 'puts Config::CONFIG["sitelibdir"] ')}

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
Summary:    Java-API and Java-Client for scalaris
Group:      Productivity/Databases/Clients
Requires:   jre >= 1.6.0
%if 0%{?sles_version} == 10 || 0%{?sles_version} == 11
# once noarch, always noarch on SLE <= 11
%else
BuildArch:  noarch
%endif

%description -n scalaris-java
Java Bindings and command line client for scalaris

%package -n ruby-scalaris
Summary:    Ruby-API and Ruby-client for scalaris
Group:      Productivity/Databases/Clients
%if 0%{?mandriva_version} || 0%{?mdkversion}
Requires:   ruby >= 1.8
%else
Requires:   ruby(abi) >= 1.8
%endif
Requires:   rubygems
Requires:   rubygem-json >= 1.4.0

%description -n ruby-scalaris
Ruby bindings and Ruby command line client for scalaris

%if 0%{?with_python}
%package -n python-scalaris
Summary:    Python-API and Python-client for scalaris
Group:      Productivity/Databases/Clients
Requires:   python >= 2.6
%if 0%{?sles_version} == 10 || 0%{?sles_version} == 11
# once noarch, always noarch on SLE <= 11
%else
BuildArch:  noarch
%endif

%description -n python-scalaris
Python bindings and Python command line client for scalaris
%endif

%if 0%{?with_python3}
%package -n python3-scalaris
Summary:    Python3-API and Python3-client for scalaris
Group:      Productivity/Databases/Clients
Requires:   python3
%if 0%{?sles_version} == 10 || 0%{?sles_version} == 11
# once noarch, always noarch on SLE <= 11
%else
BuildArch:  noarch
%endif

%description -n python3-scalaris
Python3 bindings and Python3 command line client for scalaris
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
make java-doc
%if 0%{?with_python}
make python
%endif
%if 0%{?with_python3}
make python3
%endif

%install
# see http://en.opensuse.org/Java/Packaging/Cookbook#bytecode_version_error
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
%dir %{_docdir}/scalaris/
%doc %{_docdir}/scalaris/java-api

%files -n ruby-scalaris
%defattr(-,root,root)
%{_bindir}/scalaris-ruby
%{rb_sitelib}/scalaris.rb

%if 0%{?with_python}
%files -n python-scalaris
%defattr(-,root,root)
%{_bindir}/scalaris-python
%{python_sitelib}/*
%if 0%{?with_python_doc_html} || 0%{?with_python_doc_pdf}
%doc %{_docdir}/scalaris/python-api
%endif
%endif

%if 0%{?with_python3}
%files -n python3-scalaris
%defattr(-,root,root)
%{_bindir}/scalaris-python3
%if 0%{?suse_version}
%dir %{python3_sitelib}
%dir %{python3_sitelib}/..
%endif
%{python3_sitelib}/*
%endif

%changelog
* Thu Apr 14 2011 Nico Kruber <kruber@zib.de>
- Initial package
