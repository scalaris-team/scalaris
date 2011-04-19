# norootforbuild

%define pkg_version 0.0.1
%define scalaris_user scalaris
%define scalaris_group scalaris
%define scalaris_home /var/lib/scalaris
Name:           scalaris
Summary:        Scalable Distributed key-value store
Version:        %{pkg_version}
Release:        1
License:        ASL 2.0
Group:          Productivity/Databases/Servers
URL:            http://code.google.com/p/scalaris
Source0:        %{name}-%{version}.tar.gz
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-build
BuildRequires:  erlang >= R13B01
Requires:       erlang >= R13B01
BuildArch:      noarch

##########################################################################################
## Fedora, RHEL or CentOS
##########################################################################################
%if 0%{?fedora_version} || 0%{?rhel_version} || 0%{?centos_version}
BuildRequires:  pkgconfig
Requires(pre):  shadow-utils
%endif

##########################################################################################
## Mandrake, Mandriva
##########################################################################################
%if 0%{?mandriva_version} || 0%{?mdkversion}
BuildRequires:  pkgconfig
BuildRequires:  erlang-stack >= R13B01
Requires:       erlang-stack >= R13B01
Suggests:       %{name}-java, %{name}-doc
Requires(pre):  shadow-utils
%endif

###########################################################################################
# SuSE, openSUSE
###########################################################################################
%if 0%{?suse_version}
BuildRequires:  pkg-config
Suggests:       %{name}-java, %{name}-doc
Requires(pre):  pwdutils
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
make doc

%install
rm -rf $RPM_BUILD_ROOT
make install DESTDIR=$RPM_BUILD_ROOT
make install-doc DESTDIR=$RPM_BUILD_ROOT
cp user-dev-guide/main.pdf $RPM_BUILD_ROOT/%{_docdir}/scalaris/user-dev-guide.pdf

%pre
getent group %{scalaris_group} >/dev/null || groupadd --system %{scalaris_group}
getent passwd %{scalaris_user} >/dev/null || useradd --system -g %{scalaris_group} -d %{scalaris_home} -s /sbin/nologin -c "user for scalaris" %{scalaris_user}
exit 0

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
%attr(-,scalaris,scalaris) %dir %{_sysconfdir}/scalaris
%attr(-,scalaris,scalaris) %config(noreplace) %{_sysconfdir}/scalaris/scalaris.cfg
%attr(-,scalaris,scalaris) %config(noreplace) %{_sysconfdir}/scalaris/scalaris.local.cfg
%attr(-,scalaris,scalaris) %config %{_sysconfdir}/scalaris/scalaris.local.cfg.example
%attr(-,scalaris,scalaris) %config(noreplace) %{_sysconfdir}/scalaris/scalarisctl.conf

%files doc
%defattr(-,root,root)
%dir %{_docdir}/scalaris
%doc %{_docdir}/scalaris/erlang
%doc %{_docdir}/scalaris/user-dev-guide.pdf

%changelog
* Thu Mar 19 2009 Nico Kruber <nico.laus.2001@gmx.de>
- minor changes to the spec file improving support for snapshot rpms
* Thu Dec 11 2008 Thorsten Schuett <schuett@zib.de> - 0.0.1-1
- Initial build.
