# norootforbuild

%define pkg_version 0.0.1
Name:           scalaris
Summary:        Scalable Distributed key-value store
Version:        %{pkg_version}
Release:        1
License:        ASL 2.0 
Group:          Applications/Databases
URL:            http://code.google.com/p/scalaris
Source0:        %{name}-%{version}.tar.bz2
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-build
BuildRequires:  ant
Requires:       jre >= 1.6.0

##########################################################################################  
## Fedora, RHEL or CentOS  
########################################################################################## 
%if 0%{?fedora_version} || 0%{?rhel_version} || 0%{?centos_version}  
BuildRequires:  java-devel >= 1.6.0
BuildRequires:  erlang >= R12B-4
Requires:       erlang >= R12B-4
%endif 

##########################################################################################  
## Mandrake, Mandriva  
##########################################################################################  
%if 0%{?mandriva_version} || 0%{?mdkversion}
%if 0%{?mandriva_version} >= 2009 || 0%{?mdkversion} >= 200900
BuildRequires:  java-devel >= 1.6.0
BuildRequires:  erlang-stack >= R12B-4
Requires:       erlang-stack >= R12B-4
%else
#BuildRequires:  java-1.5.0-gcj-devel
BuildRequires:  java-devel >= 1.6.0
BuildRequires:  classpathx-jaf
BuildRequires:  erlang >= R12B-4
Requires:       erlang >= R12B-4
%endif
%endif

###########################################################################################
# SuSE, openSUSE
###########################################################################################
%if 0%{?suse_version}
BuildRequires:  java-devel >= 1.6.0
BuildRequires:  erlang >= R12B-4
Requires:       erlang >= R12B-4
%endif

%description 
Scalaris is a scalable, transactional, distributed key-value store. It
can be used for building scalable services. Scalaris uses a structured
overlay with a non-blocking Paxos commit protocol for transaction
processing with strong consistency over replicas. Scalaris is
implemented in Erlang.

%package doc
Summary:    Documentation for scalaris
Group:      Documentation
Requires:   %{name} == %{version}

%description doc
Documentation for scalaris.

%package java
Summary:    Java API for scalaris
Group:      Applications/Databases
Requires:   jre
Requires:   erlang
Requires:   jakarta-commons-cli
Requires:   %{name} == %{version}

%description java
Java Bindings

%package client
Summary:    Cli client for scalaris
Group:      Applications/Databases
Requires:   %{name}-java = %{version}

%description client
Command line client for scalaris using the Java interface

%prep
%setup -q -n %{name}

%build
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
    --docdir=%{_docdir}/%{name}
make all
make java
make docs

%install
# see http://en.opensuse.org/Java/Packaging/Cookbook#bytecode_version_error
export NO_BRP_CHECK_BYTECODE_VERSION=true
rm -rf $RPM_BUILD_ROOT
make install DESTDIR=$RPM_BUILD_ROOT

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(-,root,root)
%doc AUTHORS README LICENSE
%{_bindir}/scalarisctl
#%{_libdir}/%{name}/ebin
%{_libdir}/%{name}
#%{_libdir}/%{name}/*
#%{_libdir}/%{name}/contrib/yaws/ebin
#%{_libdir}/%{name}/contrib/yaws/include
#%{_libdir}/%{name}/docroot
#%{_libdir}/%{name}/docroot_node
%{_localstatedir}/log/%{name}
%{_sysconfdir}/%{name}
%config(noreplace) %{_sysconfdir}/%{name}/scalaris.cfg
%config %{_sysconfdir}/%{name}/scalaris.local.cfg.example

%files doc
%defattr(-,root,root)
%doc %{_docdir}/%{name}
%doc user-dev-guide/main.pdf

%files java
%defattr(-,root,root)
%{_javadir}/%{name}
%config(noreplace) %{_sysconfdir}/%{name}/scalaris-java.conf
%config %{_sysconfdir}/%{name}/scalaris-java.conf.sample
%config(noreplace) %{_sysconfdir}/%{name}/scalaris.properties

%files client
%defattr(-,root,root)
%{_bindir}/scalaris

%changelog
* Thu Mar 19 2009 Nico Kruber <nico.laus.2001@gmx.de>
- minor changes to the spec file improving support for snapshot rpms
* Thu Dec 11 2008 Thorsten Schuett <schuett@zib.de> - 0.0.1-1
- Initial build.
