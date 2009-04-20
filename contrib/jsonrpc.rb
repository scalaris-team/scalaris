#!/usr/bin/ruby

require 'rubygems'
require 'json'
require 'net/http'
require 'benchmark'

$url = 'http://localhost:8000/jsonrpc.yaws'

def json_call(function, params)
  uri = URI.parse $url
  req = Net::HTTP::Post.new(uri.path)
  req.add_field 'Content-Type', 'application/json'
  req.body =
    {
    :version => '1.1',
    :method => function,
    :params => params,
    :id => 0}.to_json
  res = Net::HTTP.start(uri.host, uri.port){|http|http.request(req)}
  JSON.parse(res.body)['result']
end

def test_and_set(key, oldvalue, newvalue)
  json_call('test_and_set', [key, oldvalue, newvalue])
end

def read(key)
  json_call('read', [key])
end

def write(key, value)
  json_call('write', [key, value])
end

def nop(value)
  json_call('nop', [value])
end

def req_list()
  # request list preparation
  rlist = []
#  rlist[0] = { :read => 'keyA' }
#  rlist[1] = { :read => 'keyB' }
  rlist[0] = { :write => {'keyA', 'valueA'} }
  rlist[1] = { :write => {'keyB', 'valueB'} }
  rlist[2] = { :commit => 'commit' }

  puts rlist.to_json
  puts json_call('req_list', [rlist]).to_json


  rlist2 = []
  rlist2[0] = { :read => 'keyA' }
  rlist2[1] = { :read => 'keyB' }
  result = json_call('req_list', [rlist2])
  puts "Result: #{result.keys}"

  translog = result['translog']
  puts translog.to_json
  rlist3 = []
  rlist3[0] = { :write => {'keyA', 'valueA2'} }
  rlist3[1] = { :commit => 'commit' }

  puts rlist3.to_json
  puts json_call('req_list', [translog, rlist3]).to_json

end

n = 100

req_list()

nop = Benchmark.realtime {
  n.times do
    nop("key")
  end
}

read = Benchmark.realtime {
  n.times do
    read("key")
  end
}

write = Benchmark.realtime {
  n.times do
    write("key", "value")
  end
}

test_and_set = Benchmark.realtime {
  n.times do
    test_and_set("key", "value", "value")
  end
}

printf("              time[s]\t1/s\n")
printf("nop          : %0.02f     %0.02f\n", nop, n/nop)
printf("read         : %0.02f     %0.02f\n", read, n/read)
printf("write        : %0.02f     %0.02f\n", write, n/write)
printf("test_and_set : %0.02f     %0.02f\n", test_and_set, n/test_and_set)
