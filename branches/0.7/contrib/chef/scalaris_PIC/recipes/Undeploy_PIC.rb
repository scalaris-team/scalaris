#
# Cookbook Name:: scalaris_PIC
# Recipe:: Undeploy_PIC
# 		Note: Undeploy_PIC assumes that Stop_PIC has been executed already...
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

[ "/etc/scalaris/initd.conf", "/etc/scalaris/scalaris.local.cfg"].each do |conffile|
  file conffile do
    action :delete
  end
end

scalaris_pkgs = ["scalaris"]

scalaris_pkgs.each do |pkg|
  package pkg do
    action [ :purge, :remove ]
  end
end

case node[:platform]
when "debian", "ubuntu"
  apt_repository "scalaris-svn" do
    action :remove
  end
when "centos", "redhat", "fedora"
  execute "clean-yum-cache" do
    command "yum clean all"
    action :nothing
  end
 
  file "/etc/yum.repos.d/home:scalaris:svn.repo" do
    action :delete
    notifies :run, resources(:execute => "clean-yum-cache"), :immediately
    notifies :create, resources(:ruby_block => "reload-internal-yum-cache"), :immediately
  end
when "suse"
  execute "remove-zypper-repo" do
    command "zypper removerepo scalaris-svn"
    action :run
  end
end
