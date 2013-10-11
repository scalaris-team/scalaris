#
# Cookbook Name:: scalaris_PIC
# Recipe:: Deploy_PIC
#
# Copyright 2012, Zuse Institute Berlin
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

include_recipe "apt"

scalaris_pkgs = ["scalaris", "scalaris-java"]

scalaris_repo_base = "http://download.opensuse.org/repositories/home:/scalaris:/svn/"

# debian ubuntu centos redhat fedora suse
scalaris_repo = value_for_platform(
  "debian" => {:default => scalaris_repo_base + "Debian_" + node[:platform_version]},
  "ubuntu" => {:default => scalaris_repo_base + "xUbuntu_" + node[:platform_version]},
  "centos" => {:default => scalaris_repo_base + "CentOS_" + node[:platform_version]},
  "redhat" => {:default => scalaris_repo_base + "RHEL_" + node[:platform_version]},
  "fedora" => {:default => scalaris_repo_base + "Fedora_" + node[:platform_version]},
  "suse"   => {:default => scalaris_repo_base + "openSUSE_" + node[:platform_version]}
  )

case node[:platform]
when "debian", "ubuntu"
  apt_repository "scalaris-svn" do
    action :add
    uri scalaris_repo + " ./"
    key "http://download.opensuse.org/repositories/home:/scalaris:/svn/openSUSE_Factory/repodata/repomd.xml.key"
  end
when "centos", "redhat", "fedora"
  execute "create-yum-cache" do
    command "yum -q makecache"
    action :nothing
  end
 
  ruby_block "reload-internal-yum-cache" do
    block do
      Chef::Provider::Package::Yum::YumCache.instance.reload
    end
    action :nothing
  end
 
  remote_file "/etc/yum.repos.d/home:scalaris:svn.repo" do
    source scalaris_repo + "/home:scalaris:svn.repo"
    mode "0644"
    notifies :run, resources(:execute => "create-yum-cache"), :immediately
    notifies :create, resources(:ruby_block => "reload-internal-yum-cache"), :immediately
  end
when "suse"
  execute "create-zypper-repo" do
    command "zypper --gpg-auto-import-keys addrepo --refresh \"" + scalaris_repo + "\" scalaris-svn"
    action :run
  end
end

scalaris_pkgs.each do |pkg|
  package pkg do
    action :install
  end
end

template "/etc/scalaris/initd.conf" do
  source "initd.conf.erb"
  owner "scalaris"
  group "scalaris"
  mode "0644"
  variables(
    :scalaris_node => node[:REC][:PICs][:scalaris_PIC][0][:attributes][:scalaris_node]
  )
#  notifies :restart, resources(:service => "scalaris")
end

template "/etc/scalaris/scalaris.local.cfg" do
  source "scalaris.local.cfg.erb"
  owner "scalaris"
  group "scalaris"
  mode "0644"
  variables(
    :scalaris_port => node[:REC][:PICs][:scalaris_PIC][0][:attributes][:scalaris_port],
    :scalaris_port_web => node[:REC][:PICs][:scalaris_PIC][0][:attributes][:scalaris_port_web],
    :scalaris_start_first => node[:REC][:PICs][:scalaris_PIC][0][:attributes][:scalaris_start_first],
    :scalaris_start_mgmt_server => node[:REC][:PICs][:scalaris_PIC][0][:attributes][:scalaris_start_mgmt_server],
    :scalaris_mgmt_server => node[:REC][:PICs][:scalaris_PIC][0][:attributes][:scalaris_mgmt_server],
    :scalaris_known_hosts => node[:REC][:PICs][:scalaris_PIC][0][:attributes][:scalaris_known_hosts],
    :scalaris_nodes_per_vm => node[:REC][:PICs][:scalaris_PIC][0][:attributes][:scalaris_nodes_per_vm],
    :scalaris_max_json_req_size => node[:REC][:PICs][:scalaris_PIC][0][:attributes][:scalaris_max_json_req_size],
    :scalaris_users => node[:REC][:PICs][:scalaris_PIC][0][:attributes][:scalaris_users]
  )
#  notifies :restart, resources(:service => "scalaris")
end
