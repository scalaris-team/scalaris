Summary: Scalable Distributed key-value store
Name: scalaris
Version: 0.0.1
Release: 1
License: Apache
Provides: scalaris
Group: Applications/Databases
URL: http://code.google.com/p/scalaris
Source0: %{name}-%{version}.tar.gz
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
make all java docs

%install
rm -rf $RPM_BUILD_ROOT
make install DESTDIR=$RPM_BUILD_ROOT

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(-,root,root,-)
%doc %{_docdir}/*
%{_bindir}/scalarisctl
%{_bindir}/scalarisclient
%{_libdir}/%{name}/ebin/*.beam
%{_libdir}/%{name}/ebin/comm_layer/*.beam
%{_libdir}/%{name}/ebin/pubsub/*.beam
%{_libdir}/%{name}/ebin/transstore/*.beam
%{_libdir}/%{name}/contrib/yaws/ebin/*.beam
%{_libdir}/%{name}/docroot/*.yaws
%{_libdir}/%{name}/docroot_node/*.yaws
%config %{_sysconfdir}/scalaris/scalaris.cfg
%config %{_sysconfdir}/scalaris/scalaris.local.cfg.example
%{_datadir}/java/chordsharp4j.jar

%files doc
%defattr(-,root,root,-)
%doc AUTHORS README LICENSE doc/* user-dev-guide/main.pdf


%changelog
* Thu Dec 11 2008 Thorsten Schuett <schuett@zib.de> - 
- Initial build.

