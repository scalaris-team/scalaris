Summary: Scalable Distributed key-value store
Name: scalaris
Version: 0.0.1
Release: 1
License: ASL 2.0 
Group: Applications/Databases
URL: http://code.google.com/p/scalaris
Source0: %{name}-%{version}.tar.bz2
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-build

BuildRequires: erlang
BuildRequires: java
Requires: erlang
Requires: jre

%description 
Scalaris is a scalable, transactional, distributed key-value store. It
can be used for building scalable services. Scalaris uses a structured
overlay with a non-blocking Paxos commit protocol for transaction
processing with strong consistency over replicas. Scalaris is
implemented in Erlang.

%package doc
Summary: Documentation for scalaris
Group: Documentation

%description doc
Documentation for scalaris.

%package java
Summary: Java API for scalaris
Group: Applications/Databases
Requires: jre
Requires: erlang

%description java
Java Bindings

%package client
Summary: Cli client for scalaris
Group: Applications/Databases
Requires: %{name}-java=%{version}
Requires: jakarta-commons-cli

%description client
command line client for scalaris

%prep
%setup -q

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
rm -rf $RPM_BUILD_ROOT
make install DESTDIR=$RPM_BUILD_ROOT

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(-,root,root)
%{_bindir}/scalarisctl
%{_libdir}/%{name}/ebin
%{_libdir}/%{name}/contrib/yaws/ebin
%{_libdir}/%{name}/contrib/yaws/include
%{_libdir}/%{name}/docroot
%{_libdir}/%{name}/docroot_node
%{_localstatedir}/log/%{name}
%config(noreplace) %{_sysconfdir}/scalaris/scalaris.cfg
%config(noreplace) %{_sysconfdir}/scalaris/scalaris.local.cfg.example

%files doc
%defattr(-,root,root)
%doc %{_docdir}/%{name}
%doc AUTHORS README LICENSE user-dev-guide/main.pdf

%files java
%defattr(-,root,root)
%{_datadir}/java/chordsharp4j-lib.jar

%files client
%defattr(-,root,root)
%{_datadir}/java/chordsharp4j.jar
%{_bindir}/scalarisclient

%changelog
* Thu Dec 11 2008 Thorsten Schuett <schuett@zib.de> - 0.0.1-1
- Initial build.

