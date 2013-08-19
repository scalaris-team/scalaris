#!/usr/bin/ruby

require 'rubygems'
require 'sinatra'
require 'sinatra-sequel'
require 'erb'
require 'cgi'
require 'json'
require 'oca'
require 'nokogiri'

include OpenNebula

require 'database.rb'
require 'one.rb'
require 'opennebulahelper.rb'
require 'hadoophelper.rb'
require 'scalarishelper.rb'
require 'wikihelper.rb'
require 'jsonrpc.rb'

get '/' do
  @ps = CGI.escapeHTML(%x[ps aux | grep beam.smp]).gsub(/\n/, '<br>')
  erb :index
end

get '/one' do
  client = Client.new(CREDENTIALS, ENDPOINT)
  @vm_pool = VirtualMachinePool.new(client, -1)
  @image_pool = ImagePool.new(client)
  @host_pool = HostPool.new(client)
  @vnet_pool = VirtualNetworkPool.new(client, -1)
  @vm_pool.info
  @image_pool.info
  @host_pool.info
  @vnet_pool.info
  erb :opennebula
end

get '/one/image/:id' do
  @id = params[:id]
  client = Client.new(CREDENTIALS, ENDPOINT)
  image_pool = ImagePool.new(client)
  image_pool.info
  @image = image_pool.find {|i| i.id == @id.to_i}
  erb :opennebula_image
end

get '/one/host/:id' do
  @id = params[:id]
  client = Client.new(CREDENTIALS, ENDPOINT)
  host_pool = HostPool.new(client)
  host_pool.info
  @host = host_pool.find {|i| i.id == @id.to_i}
  erb :opennebula_host
end

get '/one/vm/:id' do
  @id = params[:id]
  client = Client.new(CREDENTIALS, ENDPOINT)
  pool = VirtualMachinePool.new(client, -1)
  pool.info
  @vm = pool.find {|i| i.id == @id.to_i}
  erb :opennebula_vm
end

post '/one/vm/:id/delete' do
  @id = params[:id]
  OpenNebulaHelper.delete_vm(@id)
  redirect '/one'
end

get '/one/vnet/:id' do
  @id = params[:id]
  client = Client.new(CREDENTIALS, ENDPOINT)
  pool = VirtualNetworkPool.new(client)
  pool.info
  @vnet = pool.find {|i| i.id == @id.to_i}
  erb :opennebula_vnet
end

get '/scalaris' do
  @instances = Scalaris.all
  erb :scalaris_list
end

post '/scalaris' do
  if params["user"] != nil and params["user"] != ""
    @valid = true
    @id = ScalarisHelper.create(params["user"])[1]
  else
    @valid = false
    @id = ""
    @error = "Please provide a user"
  end
  @params = params.to_json
  erb :scalaris_create
end

get '/scalaris/:id' do
  @id = params[:id].to_i
  @instance = Scalaris[@id]
  @ips = ScalarisHelper.get_ips(@id)
  @master = ScalarisHelper.get_ip(@instance.head_node)
  erb :scalaris_instance
end

post '/scalaris/:id/add' do
  @id = params[:id].to_i
  @new_vm_id = ScalarisHelper.add(@id)[1]
  @params = params.to_json
  @instance = Scalaris[@id]
  erb :scalaris_add
end

post '/scalaris/:id/destroy' do
  @id = params[:id].to_i
  ScalarisHelper.destroy(@id)
  redirect '/scalaris'
end

get '/hadoop' do
  @instances = Hadoop.all
  erb :hadoop
end

post '/hadoop' do
  if params["user"] != nil and params["user"] != ""
    @valid = true
    @id = HadoopHelper.create(params["user"])[1]
  else
    @valid = false
    @id = ""
    @error = "Please provide a user"
  end
  @params = params.to_json
  erb :hadoop_create
end

get '/hadoop/:id' do
  @id = params[:id].to_i
  @instance = Hadoop[@id]
  @ips = HadoopHelper.get_ips(@id)
  @master = HadoopHelper.get_ip(@instance.master_node)
  erb :hadoop_instance
end

post '/hadoop/:id/add' do
  @id = params[:id].to_i
  @new_vm_id = HadoopHelper.add(@id)[1]
  @params = params.to_json
  @instance = Hadoop[@id]
  erb :hadoop_add
end

post '/hadoop/:id/destroy' do
  @id = params[:id].to_i
  HadoopHelper.destroy(@id)
  redirect '/hadoop'
end

get '/wiki' do
  @instances = Wiki.all
  erb :wiki
end

post '/wiki' do
  if params["user"] != nil and params["user"] != "" and params["node"] != nil and params["node"] != ""
    @valid = true
    @id = WikiHelper.create(params["user"], params["node"])[1]
  else
    @valid = false
    @id = ""
    @error = "Please provide a user and a node"
  end
  @params = params.to_json
  erb :wiki_create
end

get '/wiki/:id' do
  @id = params[:id].to_i
  @instance = Wiki[@id]
  @master = WikiHelper.get_ip(@instance.master_node)
  erb :wiki_instance
end

post '/wiki/:id/destroy' do
  @id = params[:id].to_i
  WikiHelper.destroy(@id)
  redirect '/wiki'
end

post '/jsonrpc' do
  req = JSON.parse(request.body.read)
  res = JSONRPC.call(req)
  puts res
  res
end

#DB.loggers << Logger.new($stdout)
