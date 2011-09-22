# norootforbuild

%define pkg_version 1
Name:           scalaris-one-svn
Summary:        Scalable Distributed key-value store
Version:        %{pkg_version}
Release:        1
License:        ASL 2.0
Group:          Productivity/Databases/Servers
URL:            http://code.google.com/p/scalaris
Source0:        %{name}-%{version}.tar.gz
#Source100:      checkout.sh
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-build
BuildRequires:  ruby >= 1.8
%if 0%{?mandriva_version} || 0%{?mdkversion}
Requires:   ruby >= 1.8
%else
Requires:   ruby(abi) >= 1.8
%endif
Requires:   rubygems
Requires:   rubygem-json >= 1.4.0
Requires:   rubygem-sequel
Requires:   rubygem-sqlite3
Requires:   rubygem-json
Requires:   rubygem-sinatra
Requires:   rubygem-nokogiri

##########################################################################################
## Fedora, RHEL or CentOS
##########################################################################################
%if 0%{?fedora_version} || 0%{?rhel_version} || 0%{?centos_version}
BuildRequires:  erlang-erts >= R13B01, erlang-kernel, erlang-stdlib, erlang-compiler, erlang-crypto, erlang-edoc, erlang-inets, erlang-ssl, erlang-tools, erlang-xmerl
BuildRequires:  pkgconfig
BuildRequires:  ruby(abi) >= 1.8
%endif

##########################################################################################
## Mandrake, Mandriva
##########################################################################################
%if 0%{?mandriva_version} || 0%{?mdkversion}
# note: erlang is still needed for configure
BuildRequires:  erlang-base >= R13B01, erlang-compiler, erlang-crypto, erlang-edoc, erlang-inets, erlang-ssl, erlang-tools, erlang-xmerl
BuildRequires:  pkgconfig
%endif

###########################################################################################
# SuSE, openSUSE
###########################################################################################
%if 0%{?suse_version}
# note: erlang is still needed for configure
BuildRequires:  erlang >= R13B01
BuildRequires:  pkg-config
%if 0%{?suse_version} >= 1130
BuildRequires:  ruby(abi) >= 1.8
%endif
%endif

%{!?rb_sitelib: %global rb_sitelib %(ruby -rrbconfig -e 'puts Config::CONFIG["sitelibdir"] ')}

%description
Scalaris is a scalable, transactional, distributed key-value store. It
can be used for building scalable services. Scalaris uses a structured
overlay with a non-blocking Paxos commit protocol for transaction
processing with strong consistency over replicas. Scalaris is
implemented in Erlang.

%package -n scalaris-one-manager-svn
Conflicts:  scalaris-one-manager
Summary:    Manager for scalaris on Opennebula
Group:      Productivity/Databases/Clients
Requires:   jre >= 1.6.0
%if 0%{?sles_version} == 10 || 0%{?sles_version} == 11
# once noarch, always noarch on SLE <= 11
%else
BuildArch:  noarch
%endif

%description -n scalaris-one-manager-svn
Manager for scalaris on Opennebula

%package -n scalaris-one-frontend-svn
Conflicts:  scalaris-one-frontend
Summary:    Frontend for scalaris on Opennebula
Group:      Productivity/Databases/Clients
Requires:   jre >= 1.6.0
%if 0%{?sles_version} == 10 || 0%{?sles_version} == 11
# once noarch, always noarch on SLE <= 11
%else
BuildArch:  noarch
%endif

%description -n scalaris-one-frontend-svn
Frontend for scalaris on Opennebula

%package -n scalaris-one-client-svn
Conflicts:  scalaris-one-client
Summary:    Client for scalaris on Opennebula
Group:      Productivity/Databases/Clients
Requires:   jre >= 1.6.0
%if 0%{?sles_version} == 10 || 0%{?sles_version} == 11
# once noarch, always noarch on SLE <= 11
%else
BuildArch:  noarch
%endif

%description -n scalaris-svn-one-client
Client for scalaris on Opennebula

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
    --with-ruby-sitelibdir=%{rb_sitelib} \
    --enable-opennebula

%install
# see http://en.opensuse.org/Java/Packaging/Cookbook#bytecode_version_error
export NO_BRP_CHECK_BYTECODE_VERSION=true
rm -rf $RPM_BUILD_ROOT
make install-one DESTDIR=$RPM_BUILD_ROOT

%clean
rm -rf $RPM_BUILD_ROOT

%files -n scalaris-svn-one-manager
%defattr(-,root,root)
%{_prefix}/lib/scalaris/contrib/opennebula/start-manager.sh
%{_prefix}/lib/scalaris/contrib/opennebula/manager.rb
%{_prefix}/lib/scalaris/contrib/opennebula/database.rb
%{_prefix}/lib/scalaris/contrib/opennebula/hadoophelper.rb
%{_prefix}/lib/scalaris/contrib/opennebula/jsonrpc.rb
%{_prefix}/lib/scalaris/contrib/opennebula/opennebulahelper.rb
%{_prefix}/lib/scalaris/contrib/opennebula/scalarishelper.rb
%{_prefix}/lib/scalaris/contrib/opennebula/*.erb
%{_prefix}/lib/scalaris/contrib/opennebula/sc_views

%files -n scalaris-svn-one-frontend
%defattr(-,root,root)
%{_prefix}/lib/scalaris/contrib/opennebula/frontend.rb
%{_prefix}/lib/scalaris/contrib/opennebula/database.rb
%{_prefix}/lib/scalaris/contrib/opennebula/hadoophelper.rb
%{_prefix}/lib/scalaris/contrib/opennebula/jsonrpc.rb
%{_prefix}/lib/scalaris/contrib/opennebula/opennebulahelper.rb
%{_prefix}/lib/scalaris/contrib/opennebula/scalarishelper.rb
%{_prefix}/lib/scalaris/contrib/opennebula/*.erb
%{_prefix}/lib/scalaris/contrib/opennebula/fe_views

%files -n scalaris-svn-one-client
%defattr(-,root,root)
%{_prefix}/lib/scalaris/contrib/opennebula/cli.rb

%changelog
* Thu Sep 22 2011 Thorsten Schuett <schuett@zib.de> - 1-1
- Initial package
