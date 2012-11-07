# norootforbuild

%define pkg_version 0.5.0+svn
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
Source99:       scalaris-rpmlintrc
Source100:      checkout.sh
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-build
BuildArch:      noarch
BuildRequires:  screen
Requires:       screen

##########################################################################################
## Fedora, RHEL or CentOS
##########################################################################################
%if 0%{?fedora_version} || 0%{?rhel_version} || 0%{?centos_version}
BuildRequires:  erlang-erts >= R13B01, erlang-kernel, erlang-stdlib, erlang-compiler, erlang-crypto, erlang-edoc, erlang-inets, erlang-ssl, erlang-tools, erlang-xmerl
Requires:       erlang-erts >= R13B01, erlang-kernel, erlang-stdlib, erlang-compiler, erlang-crypto, erlang-inets, erlang-ssl, erlang-xmerl
BuildRequires:  pkgconfig
Requires(pre):  shadow-utils
Requires(pre):  /usr/sbin/groupadd /usr/sbin/useradd /bin/mkdir /bin/chown
%endif

##########################################################################################
## Mandrake, Mandriva
##########################################################################################
%if 0%{?mandriva_version} || 0%{?mdkversion}
BuildRequires:  pkgconfig
BuildRequires:  erlang-base >= R13B01, erlang-compiler, erlang-crypto, erlang-edoc, erlang-inets, erlang-ssl, erlang-tools, erlang-xmerl, erlang-test_server
Requires:       erlang-base >= R13B01, erlang-compiler, erlang-crypto, erlang-inets, erlang-ssl, erlang-xmerl
Suggests:       %{name}-java, %{name}-doc
Requires(pre):  shadow-utils
Requires(pre):  /usr/sbin/groupadd /usr/sbin/useradd /bin/mkdir /bin/chown
%endif

###########################################################################################
# SuSE, openSUSE
###########################################################################################
%if 0%{?suse_version}
BuildRequires:  erlang >= R13B01
Requires:       erlang >= R13B01
BuildRequires:  pkg-config
Suggests:       %{name}-java, %{name}-doc
Requires(pre):  pwdutils
PreReq:         /usr/sbin/groupadd /usr/sbin/useradd /bin/mkdir /bin/chown
Requires(pre):  %insserv_prereq
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

%pre
# note: use "-r" instead of "--system" for old systems like CentOS5, RHEL5
getent group %{scalaris_group} >/dev/null || groupadd -r %{scalaris_group}
getent passwd %{scalaris_user} >/dev/null || mkdir -p %{scalaris_home} && useradd -r -g %{scalaris_group} -d %{scalaris_home} -M -s /sbin/nologin -c "user for scalaris" %{scalaris_user} && chown %{scalaris_user}:%{scalaris_group} %{scalaris_home}
exit 0

%post
if grep -e '^cookie=\w\+' %{_sysconfdir}/scalaris/scalarisctl.conf > /dev/null 2>&1; then
  echo $RANDOM"-"$RANDOM"-"$RANDOM"-"$RANDOM >> %{_sysconfdir}/scalaris/scalarisctl.conf
fi

%if 0%{?suse_version}
%fillup_and_insserv -f scalaris
%endif
%if 0%{?fedora_version}
/sbin/chkconfig --add scalaris
%endif
%if 0%{?mandriva_version}
%_post_service scalaris
%endif

%preun
%if 0%{?suse_version}
%stop_on_removal scalaris
%endif
%if 0%{?fedora_version}
# 0 packages after uninstall -> pkg is about to be removed
  if [ "$1" = "0" ] ; then
    /sbin/service scalaris stop >/dev/null 2>&1
    /sbin/chkconfig --del scalaris
  fi
%endif
%if 0%{?mandriva_version}
%_preun_service scalaris
%endif

%postun
%if 0%{?suse_version}
%restart_on_update scalaris
%insserv_cleanup
%endif
%if 0%{?fedora_version}
# >=1 packages after uninstall -> pkg was updated -> restart
if [ "$1" -ge "1" ] ; then
  /sbin/service scalaris try-restart >/dev/null 2>&1 || :
fi
%endif
%if 0%{?mandriva_version}
%endif

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(-,root,root,-)
%dir %{_docdir}/scalaris
%{_docdir}/scalaris/AUTHORS
%{_docdir}/scalaris/README
%{_docdir}/scalaris/LICENSE
%{_docdir}/scalaris/ChangeLog
%{_bindir}/scalarisctl
%{_prefix}/lib/scalaris
%attr(-,scalaris,scalaris) %{_localstatedir}/log/scalaris
%{_sysconfdir}/init.d/scalaris
%attr(-,scalaris,scalaris) %dir %{_sysconfdir}/scalaris
%attr(-,scalaris,scalaris) %config(noreplace) %{_sysconfdir}/scalaris/initd.conf
%attr(-,scalaris,scalaris) %config(noreplace) %{_sysconfdir}/scalaris/scalaris.cfg
%attr(-,scalaris,scalaris) %config(noreplace) %{_sysconfdir}/scalaris/scalaris.local.cfg
%attr(-,scalaris,scalaris) %config %{_sysconfdir}/scalaris/scalaris.local.cfg.example
%attr(-,scalaris,scalaris) %config(noreplace) %{_sysconfdir}/scalaris/scalarisctl.conf

%files doc
%defattr(-,root,root,-)
%doc %{_docdir}/scalaris/erlang
%doc %{_docdir}/scalaris/user-dev-guide.pdf

%changelog
