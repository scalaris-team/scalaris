#!/usr/bin/ruby

require 'rubygems'
require 'sinatra'
require 'sequel'
require 'erb'
require 'optparse'
require 'json'

require 'one.rb'
require 'database.rb'
require 'jsonrpc.rb'
require 'scalarishelper.rb'
require 'hadoophelper.rb'

set :views, Proc.new { File.join(root, "sc_views") }

if TYPE == "scalaris"
  vmid = ENV['SCALARIS_VMID']
  helper = ScalarisHelper.new
  instance = Scalaris.first(:head_node => vmid)
  if instance == nil
    instance = Scalaris.create(:head_node => vmid)
    instance.add_scalarisvm(:one_vm_id => vmid)
  end
  puts instance.id
elsif TYPE == "hadoop"
  vmid = ENV['HADOOP_VMID']
  exit
else
  exit
end


get '/' do
  erb :index
end

post '/add' do
  res = helper.add(1, instance)
  if res[0] == true
    @url = res[1]
    @error = ""
  else
    @url = ""
    @error = res[1]
  end
  erb :add
end

post '/list' do
  @list = helper.list(instance)
  erb :list
end

get '/jsonrpc' do
  redirect "/"
end

post '/jsonrpc' do
  req = JSON.parse(request.body.read)
  res = JSONRPC.call(req, helper, instance)
  puts res
  res
end
