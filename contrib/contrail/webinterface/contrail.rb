#!/usr/bin/ruby

require 'rubygems'
require 'sinatra'
require 'sinatra-sequel'
require 'erb'
require 'cgi'
require 'json'
require 'oca'

include OpenNebula

require 'database.rb'
require 'one.rb'
require 'scalarishelper.rb'

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

get '/one/vnet/:id' do
  @id = params[:id]
  client = Client.new(CREDENTIALS, ENDPOINT)
  pool = VirtualNetworkPool.new(client)
  pool.info
  @vnet = pool.find {|i| i.id == @id.to_i}
  erb :opennebula_vnet
end

get '/scalaris/:id' do
  @id = params[:id].to_i
  @instance = Scalaris[@id]
  erb :scalaris_instance
end

get '/scalaris' do
  @instances = Scalaris.all
  erb :scalaris_list
end

post '/scalaris' do
  if params["user"] != nil and params["user"] != ""
    @valid = true
    instance = Scalaris.create(:user => params["user"])
    @vm_id = ScalarisHelper.create[1]
    instance.add_vm(:one_vm_id => @vm_id)
    @id = instance.id
  else
    @valid = false
    @id = ""
    @error = "Please provide a user"
  end
  @params = params.to_json
  erb :scalaris_create
end

#DB.loggers << Logger.new($stdout)
