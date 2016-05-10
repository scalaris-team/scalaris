# norootforbuild

%define pkg_version 0.9.0+git
%define scalaris_user scalaris
%define scalaris_group scalaris
%define scalaris_home /var/lib/scalaris
Name:           scalaris
Summary:        Scalable Distributed key-value store
Version:        %{pkg_version}
Release:        1
License:        Apache-2.0
Group:          Productivity/Databases/Servers
URL:            http://scalaris.zib.de
Source0:        %{name}-%{version}.tar.gz
Source99:       scalaris-rpmlintrc
Source100:      checkout.sh
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-build
BuildArch:      noarch
BuildRequires:  screen
Requires:       screen net-tools

##########################################################################################
## Fedora, RHEL or CentOS
##########################################################################################
%if 0%{?fedora_version} || 0%{?rhel_version} || 0%{?centos_version}
BuildRequires:  erlang-erts >= R14B04, erlang-kernel, erlang-stdlib, erlang-compiler, erlang-crypto, erlang-edoc, erlang-inets, erlang-parsetools, erlang-ssl, erlang-tools, erlang-xmerl, erlang-os_mon
Requires:       erlang-erts >= R14B04, erlang-kernel, erlang-stdlib, erlang-compiler, erlang-crypto, erlang-inets, erlang-ssl, erlang-xmerl, erlang-os_mon
Requires:       which
%if 0%{?fedora_version} >= 19
BuildRequires:  erlang-js
Requires:       erlang-js
%endif
%if 0%{?fedora_version} >= 22
BuildRequires:  hostname
Requires:       hostname
%endif
BuildRequires:  pkgconfig
Requires(pre):  shadow-utils
Requires(pre):  /usr/sbin/groupadd /usr/sbin/useradd /bin/mkdir /bin/chown
%if 0%{?fedora_version} >= 19 || 0%{?rhel_version} >= 700 || 0%{?centos_version} >= 700
# https://fedoraproject.org/wiki/Packaging:Systemd?rd=Packaging:Guidelines:Systemd
%define with_systemd 1
BuildRequires:  systemd
Requires:       systemd
BuildRequires:  selinux-policy-devel
Requires:       policycoreutils, libselinux-utils
Requires(post): selinux-policy-base, policycoreutils
Requires(postun): policycoreutils
%else
%define with_systemd 0
%if 0%{?rhel_version} >= 600 || 0%{?centos_version} >= 600
# provides runuser for the init.d script
BuildRequires:  util-linux-ng >= 2.17
Requires:       util-linux-ng >= 2.17
%endif
%endif
BuildRequires:  sudo
Requires:       sudo
%endif

###########################################################################################
# SuSE, openSUSE
###########################################################################################
%if 0%{?suse_version}
BuildRequires:  erlang >= R14B04
Requires:       erlang >= R14B04
%if 0%{?suse_version} >= 1310
Requires:       which
%else
Requires:       util-linux
%endif
%if 0%{?suse_version} >= 1110
BuildRequires:  erlang-erlang_js
Requires:       erlang-erlang_js
%endif
BuildRequires:  pkg-config
Suggests:       %{name}-java, %{name}-doc
Requires(pre):  pwdutils
PreReq:         /usr/sbin/groupadd /usr/sbin/useradd /bin/mkdir /bin/chown
Requires(pre):  %insserv_prereq
# keep systemd disabled for openSUSE 13.1 due to the runuser bug (see below)
%if 0%{?suse_version} > 1310
# https://en.opensuse.org/openSUSE:Systemd_packaging_guidelines
%define with_systemd 1
BuildRequires:  systemd
%{?systemd_requires}
%else
%define with_systemd 0
%endif
BuildRequires:  sudo
Requires:       sudo
%endif

%description
Scalaris is a scalable, transactional, distributed key-value store. It
can be used for building scalable services. Scalaris uses a structured
overlay with a non-blocking Paxos commit protocol for transaction
processing with strong consistency over replicas. Scalaris is
implemented in Erlang.

%package doc
Summary:    Documentation for Scalaris
Group:      Documentation/Other
Requires:   %{name} == %{version}-%{release}

%description doc
Documentation for Scalaris including its User-Dev-Guide.

%prep
%setup -q -n %{name}-%{version}

%build
# NOTE: disable runuser on openSUSE 13.1 because of the following bug
# see https://bugzilla.novell.com/show_bug.cgi?id=892079
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
%if 0%{?suse_version} == 1310
    --disable-runuser \
%endif
%if 0%{?with_systemd}
    --with-systemd=%{_unitdir} \
%endif
    --docdir=%{_docdir}/scalaris \
    --disable-cpp
make all
make doc

%install
rm -rf $RPM_BUILD_ROOT
make install DESTDIR=$RPM_BUILD_ROOT
make install-doc DESTDIR=$RPM_BUILD_ROOT
%if 0%{?with_systemd}
%if 0%{?fedora_version} || 0%{?rhel_version} || 0%{?centos_version}
# setup SELinux policies
cd contrib/systemd
sed -e "s|/var/lib/scalaris|%{scalaris_home}|g" \
    -i scalaris.fc
make -f /usr/share/selinux/devel/Makefile
install -d %{buildroot}%{_datadir}/selinux/packages
install -m 644 scalaris.pp %{buildroot}%{_datadir}/selinux/packages/
cd -
%endif
%endif

%pre
# note: use "-r" instead of "--system" for old systems like CentOS5, RHEL5
getent group %{scalaris_group} >/dev/null || groupadd -r %{scalaris_group}
getent passwd %{scalaris_user} >/dev/null || mkdir -p %{scalaris_home} && useradd -r -g %{scalaris_group} -d %{scalaris_home} -M -s /sbin/nologin -c "user for scalaris" %{scalaris_user} && chown %{scalaris_user}:%{scalaris_group} %{scalaris_home}

%if 0%{?suse_version}
%if 0%{?with_systemd}
%service_add_pre scalaris.service scalaris-first.service
%endif
%endif

exit 0

%post
if grep -e '^cookie=\w\+' %{_sysconfdir}/scalaris/scalarisctl.conf > /dev/null 2>&1; then
  echo $RANDOM"-"$RANDOM"-"$RANDOM"-"$RANDOM >> %{_sysconfdir}/scalaris/scalarisctl.conf
fi

%if 0%{?suse_version}
%if 0%{?with_systemd}
%service_add_post scalaris.service scalaris-first.service
%else
%fillup_and_insserv -f scalaris scalaris-first
%endif
%endif

%if 0%{?fedora_version} || 0%{?rhel_version} || 0%{?centos_version}
%if 0%{?with_systemd}
semodule -i %{_datadir}/selinux/packages/scalaris.pp || :
if [ "$1" -le 1 ] ; then # First install
  semanage port -a -t scalaris_port_t -p tcp 14194-14198 || :
  semanage port -a -t scalaris_port_t -p tcp 8000-8004 || :
fi
/sbin/restorecon -R %{scalaris_home} || :
%systemd_post scalaris.service scalaris-first.service
%else
/sbin/chkconfig --add scalaris
%endif
%endif

%preun
%if 0%{?suse_version}
%if 0%{?with_systemd}
%service_del_preun scalaris.service scalaris-first.service
%else
%stop_on_removal scalaris scalaris-first
%endif
%endif

%if 0%{?fedora_version} || 0%{?rhel_version} || 0%{?centos_version}
%if 0%{?with_systemd}
%systemd_preun scalaris.service scalaris-first.service
%else
  if [ "$1" -eq 0 ] ; then # final removal
    /sbin/service scalaris stop >/dev/null 2>&1
    /sbin/chkconfig --del scalaris
  fi
%endif
%endif

%postun
%if 0%{?suse_version}
%if 0%{?with_systemd}
%service_del_postun scalaris.service scalaris-first.service
%else
%restart_on_update scalaris scalaris-first
%insserv_cleanup
%endif
%endif

%if 0%{?fedora_version} || 0%{?rhel_version} || 0%{?centos_version}
%if 0%{?with_systemd}
%systemd_postun_with_restart scalaris.service scalaris-first.service
if [ "$1" -eq 0 ]; then # final removal
  semanage port -d -t scalaris_port_t -p tcp 14194-14198 2>/dev/null || :
  semanage port -d -t scalaris_port_t -p tcp 8000-8004 2>/dev/null || :
  semodule -r scalaris || :
  /sbin/restorecon -R %{scalaris_home} || :
fi
%else
if [ "$1" -ge 1 ] ; then  # pkg was updated -> restart
  /sbin/service scalaris try-restart >/dev/null 2>&1 || :
fi
%endif
%endif

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(-,root,root,-)
%dir %{_docdir}/scalaris
%{_docdir}/scalaris/AUTHORS
%{_docdir}/scalaris/README.md
%{_docdir}/scalaris/LICENSE
%{_docdir}/scalaris/ChangeLog
%{_bindir}/scalarisctl
%{_prefix}/lib/scalaris
%exclude %{_prefix}/lib/scalaris/docroot/doc
%attr(-,scalaris,scalaris) %{_localstatedir}/log/scalaris
%if 0%{?with_systemd}
%{_unitdir}/scalaris.service
%{_unitdir}/scalaris-first.service
%dir %{_sysconfdir}/conf.d
%config(noreplace) %{_sysconfdir}/conf.d/scalaris
%config(noreplace) %{_sysconfdir}/conf.d/scalaris-first
%if 0%{?fedora_version} || 0%{?rhel_version} || 0%{?centos_version}
%attr(0600,root,root) %{_datadir}/selinux/packages/scalaris.pp
%endif
%else
%{_sysconfdir}/init.d/scalaris
%{_sysconfdir}/init.d/scalaris-first
%{_sbindir}/rcscalaris
%config(noreplace) %{_sysconfdir}/scalaris/initd.conf
%config(noreplace) %{_sysconfdir}/scalaris/initd-first.conf
%endif
%dir %{_sysconfdir}/scalaris
%config %{_sysconfdir}/scalaris/scalaris.cfg
%config(noreplace) %{_sysconfdir}/scalaris/scalaris.local.cfg
%config %{_sysconfdir}/scalaris/scalaris.local.cfg.example
%config(noreplace) %{_sysconfdir}/scalaris/scalarisctl.conf

%files doc
%defattr(-,root,root,-)
%doc %{_docdir}/scalaris/erlang
%doc %{_docdir}/scalaris/user-dev-guide.pdf
%{_prefix}/lib//scalaris/docroot/doc

%changelog
