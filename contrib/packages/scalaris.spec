# norootforbuild

%define pkg_version 0.0.1
Name:           scalaris
Summary:        Scalable Distributed key-value store
Version:        %{pkg_version}
Release:        1
License:        ASL 2.0
Group:          Productivity/Databases/Servers
URL:            http://code.google.com/p/scalaris
Source0:        %{name}-%{version}.tar.gz
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-build
BuildRequires:  ant
BuildRequires:  erlang >= R13B01
Requires:       erlang >= R13B01
BuildArch:      noarch

##########################################################################################
## Fedora, RHEL or CentOS
##########################################################################################
%if 0%{?fedora_version} || 0%{?rhel_version} || 0%{?centos_version}
BuildRequires:  pkgconfig
%endif

##########################################################################################
## Mandrake, Mandriva
##########################################################################################
%if 0%{?mandriva_version} || 0%{?mdkversion}
BuildRequires:  pkgconfig
%if 0%{?mandriva_version} >= 2009 || 0%{?mdkversion} >= 200900
BuildRequires:  erlang-stack >= R13B01
Requires:       erlang-stack >= R13B01
Suggests:       %{name}-client, %{name}-doc
%else
#BuildRequires:  classpathx-jaf
%endif
%endif

###########################################################################################
# SuSE, openSUSE
###########################################################################################
%if 0%{?suse_version}
BuildRequires:  pkg-config
Suggests:       %{name}-client, %{name}-doc
%endif

%description
Scalaris is a scalable, transactional, distributed key-value store. It
can be used for building scalable services. Scalaris uses a structured
overlay with a non-blocking Paxos commit protocol for transaction
processing with strong consistency over replicas. Scalaris is
implemented in Erlang.

%package doc
Summary:    Documentation for scalaris
Group:      Documentation/Other
Requires:   %{name} == %{version}-%{release}

%description doc
Documentation for scalaris.

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
    --docdir=%{_docdir}/scalaris
make all
make docs

%install
# see http://en.opensuse.org/Java/Packaging/Cookbook#bytecode_version_error
export NO_BRP_CHECK_BYTECODE_VERSION=true
rm -rf $RPM_BUILD_ROOT
make install-docs DESTDIR=$RPM_BUILD_ROOT
cp user-dev-guide/main.pdf $RPM_BUILD_ROOT/%{_docdir}/scalaris/user-dev-guide.pdf

%post
if grep -e '^cookie=\w\+' %{_sysconfdir}/scalaris/scalarisctl.conf > /dev/null 2>&1; then
  echo $RANDOM"-"$RANDOM"-"$RANDOM"-"$RANDOM >> %{_sysconfdir}/scalaris/scalarisctl.conf
fi

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(-,root,root)
%doc AUTHORS README LICENSE
%{_bindir}/scalarisctl
%{_prefix}/lib/scalaris
%{_localstatedir}/log/scalaris
%dir %{_sysconfdir}/scalaris
%config(noreplace) %{_sysconfdir}/scalaris/scalaris.cfg
%config(noreplace) %{_sysconfdir}/scalaris/scalaris.local.cfg
%config %{_sysconfdir}/scalaris/scalaris.local.cfg.example
%config(noreplace) %{_sysconfdir}/scalaris/scalarisctl.conf

%files doc
%defattr(-,root,root)
%doc %{_docdir}/scalaris
%doc %{_docdir}/scalaris/user-dev-guide.pdf

%changelog
* Thu Mar 19 2009 Nico Kruber <nico.laus.2001@gmx.de>
- minor changes to the spec file improving support for snapshot rpms
* Thu Dec 11 2008 Thorsten Schuett <schuett@zib.de> - 0.0.1-1
- Initial build.
