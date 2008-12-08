#!/usr/bin/ruby

require 'rubygems'
require 'json'
require 'net/http'

def test_and_set(url, key, oldvalue, newvalue)
  uri = URI.parse url
  req = Net::HTTP::Post.new(url)
  req.add_field 'Content-Type', 'application/json'
  req.body = {:version => '1.1', :method => 'test_and_set', :params => [key, oldvalue, newvalue], :id => 0}.to_json
  res = Net::HTTP.start(uri.host, uri.port){|http|
    http.request(req)
  }
  JSON.parse(res.body)['result']
end

def read(url, key)
  uri = URI.parse url
  req = Net::HTTP::Post.new(url)
  req.add_field 'Content-Type', 'application/json'
  req.body = {:version => '1.1', :method => 'read', :params => [key], :id => 0}.to_json
  res = Net::HTTP.start(uri.host, uri.port){|http|
    http.request(req)
  }
  JSON.parse(res.body)['result']
end

def write(url, key, value)
  uri = URI.parse url
  req = Net::HTTP::Post.new(url)
  req.add_field 'Content-Type', 'application/json'
  req.body = {:version => '1.1', :method => 'write', :params => [key, value], :id => 0}.to_json
  res = Net::HTTP.start(uri.host, uri.port){|http|
    http.request(req)
  }
  JSON.parse(res.body)['result']
end

puts "read('http://localhost:8000/jsonrpc.yaws', 'Bar') \n    == #{read('http://localhost:8000/jsonrpc.yaws', 'Bar').to_json}"
puts "test_and_set('http://localhost:8000/jsonrpc.yaws', 'Bar', '', 'Foo') \n    == #{test_and_set("http://localhost:8000/jsonrpc.yaws", "Bar", "", "Foo").to_json}"
puts "read('http://localhost:8000/jsonrpc.yaws', 'Bar') \n    == #{read('http://localhost:8000/jsonrpc.yaws', 'Bar').to_json}"
puts "test_and_set('http://localhost:8000/jsonrpc.yaws', 'Bar', '', 'Foo') \n    == #{test_and_set("http://localhost:8000/jsonrpc.yaws", "Bar", "", "Foo").to_json}"
puts "write('http://localhost:8000/jsonrpc.yaws', 'Bar', '') \n    == #{write("http://localhost:8000/jsonrpc.yaws", "Bar", "").to_json}"
puts "test_and_set('http://localhost:8000/jsonrpc.yaws', 'Bar', '', 'Foo') \n    == #{test_and_set("http://localhost:8000/jsonrpc.yaws", "Bar", "", "Foo").to_json}"


