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

$url = 'http://localhost:4567/jsonrpc'

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

def create_scalaris(url)
  json_call(URI.parse(url), "create_scalaris", []).to_json
end

def add_vm_scalaris(url, count)
  json_call(URI.parse(url), "add_vm_scalaris", [count]).to_json
end

def destroy_scalaris(fe_url, mgmt_url)
  json_call(URI.parse(fe_url), "destroy_scalaris", [mgmt_url]).to_json
end

options = {}

optparse = OptionParser.new do |opts|
  options[:url] = $url
  opts.on('--url URL', String, 'Service URL' ) do |url|
    options[:url] = url
  end

  options[:scalaris_create] = nil
  opts.on('--create-scalaris', 'Create new scalaris cluster' ) do |key|
    options[:scalaris_create] = true
  end

  options[:scalaris_add_vm] = nil
  opts.on('--scalaris-add-vm Count', Integer, 'Add more VMs to this scalaris cluster' ) do |count|
    options[:scalaris_add_vm] = count
  end

  options[:scalaris_destroy] = nil
  opts.on('--destroy-scalaris URL', String, 'Destroy scalaris cluster URL' ) do |url|
    options[:scalaris_destroy] = url
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

puts create_scalaris(options[:url]) unless options[:scalaris_create] == nil
puts add_vm_scalaris(options[:url], options[:scalaris_add_vm]) unless options[:scalaris_add_vm] == nil
puts destroy_scalaris(options[:url], options[:scalaris_destroy]) unless options[:scalaris_destroy] == nil
