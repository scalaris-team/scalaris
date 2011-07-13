#!/usr/bin/ruby
# Copyright 2008-2011 Zuse Institute Berlin
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.

require 'rubygems'
require 'json'
require 'net/http'
require 'optparse'
require 'pp'

$url = 'http://localhost:4568/jsonrpc'

def json_call(url, function, params)
  req = Net::HTTP::Post.new(url.path)
  req.add_field 'Content-Type', 'application/json'
  req.body =
    {
    :version => '1.1',
    :method => function,
    :params => params,
    :id => 0}.to_json
  begin
    res = Net::HTTP.start(url.host, url.port){|http|http.request(req)}
    JSON.parse(res.body)
  rescue
    puts "#{url}: #{$!}"
    nil
  end
end

def get_scalaris_info(url)
  json_call(URI.parse(url), "get_scalaris_info", []).to_json
end

def create_scalaris(url, name)
  json_call(URI.parse(url), "create_scalaris", [name]).to_json
end

def destroy_scalaris(fe_url, mgmt_url)
  json_call(URI.parse(fe_url), "destroy_scalaris", [mgmt_url]).to_json
end

def add_nodes(url, count)
  json_call(URI.parse(url), "add_nodes", [count]).to_json
end

def remove_nodes(url, count)
  json_call(URI.parse(url), "remove_nodes", [count]).to_json
end

def list_nodes(url)
  json_call(URI.parse(url), "list_nodes", []).to_json
end

def get_node_info(url, vmid)
  json_call(URI.parse(url), "get_node_info", [vmid]).to_json
end

def get_node_performance(url, vmid)
  json_call(URI.parse(url), "get_node_performance", [vmid]).to_json
end

def get_service_info(url)
  json_call(URI.parse(url), "get_service_info", []).to_json
end

def get_service_performance(url)
  json_call(URI.parse(url), "get_service_performance", []).to_json
end

options = {}

optparse = OptionParser.new do |opts|
  options[:url] = $url
  opts.on('--url URL', String, 'Service URL' ) do |url|
    options[:url] = url
  end

  options[:scalaris_create] = nil
  opts.on('--create-scalaris NAME', String, 'Create new scalaris cluster' ) do |name|
    options[:scalaris_create] = name
  end

  options[:scalaris_destroy] = nil
  opts.on('--destroy-scalaris URL', String, 'Destroy scalaris cluster URL' ) do |url|
    options[:scalaris_destroy] = url
  end

  options[:add_nodes] = nil
  opts.on('--add-nodes Count', Integer, 'Add more VMs to this service instance' ) do |count|
    options[:add_nodes] = count
  end

  options[:remove_nodes] = nil
  opts.on('--remove-nodes Count', Integer, 'Remove random VMs from this service instance' ) do |count|
    options[:remove_nodes] = count
  end

  options[:list_nodes] = nil
  opts.on('--list-nodes', 'List the VMs of this service instance' ) do |key|
    options[:list_nodes] = true
  end

  options[:get_node_info] = nil
  opts.on('--get-node-info VMID', String, 'Get information on VM' ) do |vmid|
    options[:get_node_info] = vmid
  end

  options[:get_node_performance] = nil
  opts.on('--get-node-performance VMID', String, 'Get performance information on VM' ) do |vmid|
    options[:get_node_performance] = vmid
  end

  options[:get_service_info] = nil
  opts.on('--get-service-info', 'Get information on service' ) do |key|
    options[:get_service_info] = true
  end

  options[:get_service_performance] = nil
  opts.on('--get-service-performance', 'Get performance information on service' ) do |vmid|
    options[:get_service_performance] = true
  end

  opts.on_tail("-h", "--help", "Show this message") do
    puts opts
    exit
  end
end

begin
  optparse.parse!
rescue OptionParser::ParseError
  $stderr.print "Error: " + $! + "\n"
  exit
end

puts create_scalaris(options[:url], options[:scalaris_create]) unless options[:scalaris_create] == nil
puts add_nodes(options[:url], options[:add_nodes]) unless options[:add_nodes] == nil
puts remove_nodes(options[:url], options[:remove_nodes]) unless options[:remove_nodes] == nil
puts destroy_scalaris(options[:url], options[:scalaris_destroy]) unless options[:scalaris_destroy] == nil
puts list_nodes(options[:url]) unless options[:list_nodes] == nil
puts get_node_info(options[:url], options[:get_node_info]) unless options[:get_node_info] == nil
puts get_node_performance(options[:url], options[:get_node_performance]) unless options[:get_node_performance] == nil
puts get_service_info(options[:url]) unless options[:get_service_info] == nil
puts get_service_performance(options[:url]) unless options[:get_service_performance] == nil
