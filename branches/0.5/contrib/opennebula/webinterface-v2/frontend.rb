#!/usr/bin/ruby

require 'rubygems'
require 'sinatra'
require 'json'
require 'erb'

require 'jsonrpc.rb'
require 'scalarishelper.rb'
require 'hadoophelper.rb'

set :views, Proc.new { File.join(root, "fe_views") }

get '/' do
  erb :index
end

post '/scalaris' do
  res = ScalarisHelper.create()
  if res[0] == true
    @url = res[1]
    @error = ""
  else
    @url = ""
    @error = res[1]
  end
  erb :scalaris
end

post '/hadoop' do
  res = HadoopHelper.create()
  @url = ""
  @error = ""
  if res[0] == true
    @url = res[1]
  else
    @error = res[1]
  end
  erb :hadoop
end

post '/destroy' do
  redirect "#{params[:url]}destroy"
end

post '/jsonrpc' do
  req = JSON.parse(request.body.read)
  res = JSONRPC.call(req, nil, nil)
  puts "res: #{res}"
  res
end
