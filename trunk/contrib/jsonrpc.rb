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
  puts "Start sample request list run..."

  # first transaction sets two keys and commits
  puts "  Write keyA, KeyB and commit."
  rlist = []
  rlist[0] = { :write => {'keyA' => 'valueA'} }
  rlist[1] = { :write => {'keyB' => 'valueB'} }
  rlist[2] = { :commit => 'commit' }

  result = json_call('req_list', [rlist])
  values = result['results']
  puts "  Commit was: #{values[2]['value']}."

  # second transaction reads two keys and then modifies one of them
  rlist2 = []
  rlist2[0] = { :read => 'keyA' }
  rlist2[1] = { :read => 'keyB' }
  result = json_call('req_list', [rlist2])

  translog = result['tlog']
  values = result['results']

  puts "  Read key #{values[0]['key']} as: #{values[0]['value']}"
  puts "  Read key #{values[1]['key']} as: #{values[1]['value']}"

  puts "  Modify keyA and commit."
  rlist3 = []
  rlist3[0] = { :write => {'keyA' => 'valueA2'} }
  rlist3[1] = { :commit => 'commit' }
  result = json_call('req_list', [translog, rlist3])
  values = result['results']

  puts "  Commit was: #{values[1]['value']}#{values[1]['fail']}, write request was #{values[0]['value']}."
   result = json_call('delete', ["keyA"])
   puts "  Delete keyA: #{result['ok']} replicas deleted. Report: #{result['results'].to_json}."
  puts
end

def req_list2()
  # first transaction sets two keys and commits
  rlist = []
  rlist[0] = { :write => {'keyA' => 'valueA'} }
  rlist[1] = { :write => {'keyB' => 'valueB'} }
  rlist[2] = { :commit => 'commit' }

  result = json_call('req_list', [rlist])
  values = result['results']

  # second transaction reads two keys and then modifies one of them
  rlist2 = []
  rlist2[0] = { :read => 'keyA' }
  rlist2[1] = { :read => 'keyB' }
  result = json_call('req_list', [rlist2])

  translog = result['tlog']
  values = result['results']

  rlist3 = []
  rlist3[0] = { :write => {'keyA' => 'valueA2'} }
  rlist3[1] = { :commit => 'commit' }
  result = json_call('req_list', [translog, rlist3])
  values = result['results']

  result = json_call('delete', ["keyA"])
end

def range_query()
  result = json_call('range_read',
                     [0, 0x40000000000000000000000000000000])
  puts result.to_json
  write("keyA", "valueA")
  write("keyB", "valueB")
  write("keyC", "valueC")
  result = json_call('range_read',
                     [0, 0x40000000000000000000000000000000])
  puts result.to_json
end

def publish(topic, content)
  json_call('publish', [topic,content])
end

def subscribe(topic, url)
  json_call('subscribe', [topic, url])
end

def unsubscribe(topic,url)
  json_call('unsubscribe', [topic,url])
end

def get_subscribers(topic)
  json_call('get_subscribers', [topic])
end

n = 100

puts "testing request lists ..."
req_list()

puts "testing range queries ..."
range_query()

puts "benchmarking ..."

puts " nops ..."
nop = Benchmark.realtime {
  n.times do
    nop("key")
  end
}

puts " read ..."
read = Benchmark.realtime {
  n.times do
    read("key")
  end
}

puts " write ..."
write = Benchmark.realtime {
  n.times do
    write("key", "value")
  end
}

puts " test and set ..."
test_and_set = Benchmark.realtime {
  n.times do
    test_and_set("key", "value", "value")
  end
}

puts " request list processing ..."
reql = Benchmark.realtime {
  n.times do
    req_list2()
  end
}


puts "testing pub sub once more ..."
def pubsub()
  subs = get_subscribers("Topic")
  printf("subscribers: %s\n", subs.to_json)
  # register scalaris boot server itself as subscriber
  # (prints a message on the console then)
  subscribe("Topic", "http://localhost:8000/jsonrpc.yaws")
  subscribe("Topic", "http://localhost:8000/jsonrpc.yaws")
  subs = get_subscribers("Topic")
  printf("subscribers: %s\n", subs.to_json)
  publish("Topic", "Value")
  unsubscribe("Topic", "http://localhost:8000/jsonrpc.yaws")
  subs = get_subscribers("Topic")
  printf("subscribers: %s\n", subs.to_json)
end

pubsub()

printf("              time[s]\t1/s\n")
printf("nop          : %0.02f     %0.02f\n", nop, n/nop)
printf("read         : %0.02f     %0.02f\n", read, n/read)
printf("write        : %0.02f     %0.02f\n", write, n/write)
printf("test_and_set : %0.02f     %0.02f\n", test_and_set, n/test_and_set)
printf("req_list     : %0.02f     %0.02f\n", reql, n/reql)
