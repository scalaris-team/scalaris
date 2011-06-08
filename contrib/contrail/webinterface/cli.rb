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
$uri = URI.parse $url

def json_call(function, params)
  req = Net::HTTP::Post.new($uri.path)
  req.add_field 'Content-Type', 'application/json'
  req.body =
    {
    :version => '1.1',
    :method => function,
    :params => params,
    :id => 0}.to_json
  res = Net::HTTP.start($uri.host, $uri.port){|http|http.request(req)}
  jsonres = JSON.parse(res.body)
  if jsonres['error'] == nil
    JSON.parse(res.body)["result"]
  else
    JSON.parse(res.body)['error']
  end
end

def get_scalaris_info(id)
  json_call("get_scalaris_info", [id]).to_json
end

def create_scalaris(user)
  json_call("create_scalaris", [user]).to_json
end

def add_vm_scalaris(id)
  json_call("add_vm_scalaris", [id]).to_json
end

def destroy_scalaris(id)
  json_call("destroy_scalaris", [id]).to_json
end

options = {}

optparse = OptionParser.new do |opts|
  options[:scalaris_info] = nil
  opts.on('--scalaris-info ID', Integer, 'get info on scalaris instance ID' ) do |id|
    options[:scalaris_info] = id
  end

  options[:scalaris_create] = nil
  opts.on('--scalaris-create', 'create new scalaris instance' ) do |key|
    options[:scalaris_create] = true
  end

  options[:scalaris_add_vm] = nil
  opts.on('--scalaris-add-vm ID', Integer, 'add another vm to this scalaris instance' ) do |id|
    options[:scalaris_add_vm] = id
  end

  options[:scalaris_destroy] = nil
  opts.on('--scalaris-destroy ID', Integer, 'destroy scalaris instance ID' ) do |id|
    options[:scalaris_destroy] = id
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

puts get_scalaris_info(options[:scalaris_info]) unless options[:scalaris_info] == nil
puts create_scalaris(ENV['USER']) unless options[:scalaris_create] == nil
puts add_vm_scalaris(options[:scalaris_add_vm]) unless options[:scalaris_add_vm] == nil
puts destroy_scalaris(options[:scalaris_destroy]) unless options[:scalaris_destroy] == nil
