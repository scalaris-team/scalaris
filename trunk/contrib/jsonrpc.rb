#!/usr/bin/ruby

require 'rubygems'
require 'json'
require 'net/http'
require 'benchmark'

def test_and_set(url, key, oldvalue, newvalue)
  uri = URI.parse url
  req = Net::HTTP::Post.new(uri.path)
  req.add_field 'Content-Type', 'application/json'
  req.body = {:version => '1.1', :method => 'test_and_set', :params => [key, oldvalue, newvalue], :id => 0}.to_json
  res = Net::HTTP.start(uri.host, uri.port){|http|
    http.request(req)
  }
  JSON.parse(res.body)['result']
end

def read(url, key)
  uri = URI.parse url
  req = Net::HTTP::Post.new(uri.path)
  req.add_field 'Content-Type', 'application/json'
  req.body = {:version => '1.1', :method => 'read', :params => [key], :id => 0}.to_json
  res = Net::HTTP.start(uri.host, uri.port){|http|
    http.request(req)
  }
  JSON.parse(res.body)['result']
end

def write(url, key, value)
  uri = URI.parse url
  req = Net::HTTP::Post.new(uri.path)
  req.add_field 'Content-Type', 'application/json'
  req.body = {:version => '1.1', :method => 'write', :params => [key, value], :id => 0}.to_json
  res = Net::HTTP.start(uri.host, uri.port){|http|
    http.request(req)
  }
  JSON.parse(res.body)['result']
end

def nop(url, value)
  uri = URI.parse url
  req = Net::HTTP::Post.new(uri.path)
  req.add_field 'Content-Type', 'application/json'
  req.body = {:version => '1.1', :method => 'nop', :params => [value], :id => 0}.to_json
  res = Net::HTTP.start(uri.host, uri.port){|http|
    http.request(req)
  }
  JSON.parse(res.body)['result']
end

#puts "read('http://localhost:8000/jsonrpc.yaws', 'Bar') \n    == #{read('http://localhost:8000/jsonrpc.yaws', 'Bar').to_json}"
#puts "test_and_set('http://localhost:8000/jsonrpc.yaws', 'Bar', '', 'Foo') \n    == #{test_and_set("http://localhost:8000/jsonrpc.yaws", "Bar", "", "Foo").to_json}"
#puts "read('http://localhost:8000/jsonrpc.yaws', 'Bar') \n    == #{read('http://localhost:8000/jsonrpc.yaws', 'Bar').to_json}"
#puts "test_and_set('http://localhost:8000/jsonrpc.yaws', 'Bar', '', 'Foo') \n    == #{test_and_set("http://localhost:8000/jsonrpc.yaws", "Bar", "", "Foo").to_json}"
#puts "write('http://localhost:8000/jsonrpc.yaws', 'Bar', '') \n    == #{write("http://localhost:8000/jsonrpc.yaws", "Bar", "").to_json}"
#puts "test_and_set('http://localhost:8000/jsonrpc.yaws', 'Bar', '', 'Foo') \n    == #{test_and_set("http://localhost:8000/jsonrpc.yaws", "Bar", "", "Foo").to_json}"



n = 100
url = 'http://localhost:8000/jsonrpc.yaws'

nop = Benchmark.realtime {
  n.times do
    nop(url, "key")
  end
}

read = Benchmark.realtime {
  n.times do
    read(url, "key")
  end
}

write = Benchmark.realtime {
  n.times do
    write(url, "key", "value")
  end
}

test_and_set = Benchmark.realtime {
  n.times do
    test_and_set(url, "key", "value", "value")
  end
}

printf("              time[s]\t1/s\n")
printf("nop          : %0.02f     %0.02f\n", nop, n/nop)
printf("read         : %0.02f     %0.02f\n", read, n/read)
printf("write        : %0.02f     %0.02f\n", write, n/write)
printf("test_and_set : %0.02f     %0.02f\n", test_and_set, n/test_and_set)
