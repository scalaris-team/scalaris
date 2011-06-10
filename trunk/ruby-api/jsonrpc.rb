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
require 'benchmark'
require 'scalaris'

def req_list(sc)
  puts "Start sample request list run..."

  # first transaction sets two keys and commits
  puts "  Write keyA, KeyB and commit."
  rlist = []
  rlist[0] = { :write => {'keyA' => sc._json_encode_value('valueA')} }
  rlist[1] = { :write => {'keyB' => sc._json_encode_value('valueB')} }
  rlist[2] = { :commit => '' }

  result = sc.json_call('req_list', [rlist])
  values = sc._json_replace_value_in_results(result['results'])
  puts "  Commit was: #{values[2]['value']}."

  # second transaction reads two keys and then modifies one of them
  rlist2 = []
  rlist2[0] = { :read => 'keyA' }
  rlist2[1] = { :read => 'keyB' }
  result = sc.json_call('req_list', [rlist2])

  translog = result['tlog']
  values = sc._json_replace_value_in_results(result['results'])

  puts "  Read key #{values[0]['key']} as: #{values[0]['value']}"
  puts "  Read key #{values[1]['key']} as: #{values[1]['value']}"

  puts "  Modify keyA and commit."
  rlist3 = []
  rlist3[0] = { :write => {'keyA' => sc._json_encode_value('valueA2')} }
  rlist3[1] = { :commit => '' }
  result = sc.json_call('req_list', [translog, rlist3])
  values = sc._json_replace_value_in_results(result['results'])

  puts "  Commit was: #{values[1]['value']}#{values[1]['fail']}, write request was #{values[0]['value']}."
   result = sc.json_call('delete', ["keyA"])
   puts "  Delete keyA: #{result['ok']} replicas deleted. Report: #{result['results'].to_json}."
  puts
end

def req_list2(sc)
  # first transaction sets two keys and commits
  rlist = []
  rlist[0] = { :write => {'keyA' => sc._json_encode_value('valueA')} }
  rlist[1] = { :write => {'keyB' => sc._json_encode_value('valueB')} }
  rlist[2] = { :commit => '' }

  result = sc.json_call('req_list', [rlist])
  values = sc._json_replace_value_in_results(result['results'])

  # second transaction reads two keys and then modifies one of them
  rlist2 = []
  rlist2[0] = { :read => 'keyA' }
  rlist2[1] = { :read => 'keyB' }
  result = sc.json_call('req_list', [rlist2])

  translog = result['tlog']
  values = sc._json_replace_value_in_results(result['results'])

  rlist3 = []
  rlist3[0] = { :write => {'keyA' => sc._json_encode_value('valueA2')} }
  rlist3[1] = { :commit => '' }
  result = sc.json_call('req_list', [translog, rlist3])
  values = sc._json_replace_value_in_results(result['results'])

  result = sc.json_call('delete', ["keyA"])
end

def range_query(sc)
  result = sc.json_call('range_read',
                     [0, 0x40000000000000000000000000000000])
  puts result.to_json
  sc.write("keyA", "valueA")
  sc.write("keyB", "valueB")
  sc.write("keyC", "valueC")
  result = sc.json_call('range_read',
                     [0, 0x40000000000000000000000000000000])
  puts result.to_json
end

def pubsub(sc)
  subs = sc.get_subscribers("Topic")
  printf("subscribers: %s\n", subs.to_json)
  # register scalaris boot server itself as subscriber
  # (prints a message on the console then)
  sc.subscribe("Topic", "http://localhost:8000/jsonrpc.yaws")
  sc.subscribe("Topic", "http://localhost:8000/jsonrpc.yaws")
  subs = sc.get_subscribers("Topic")
  printf("subscribers: %s\n", subs.to_json)
  sc.publish("Topic", "Value")
  sc.unsubscribe("Topic", "http://localhost:8000/jsonrpc.yaws")
  subs = sc.get_subscribers("Topic")
  printf("subscribers: %s\n", subs.to_json)
end

sc = Scalaris::Scalaris.new

n = 100

puts "testing request lists ..."
req_list(sc)

puts "testing range queries ..."
range_query(sc)

puts "benchmarking ..."

puts " nops ..."
nop = Benchmark.realtime {
  n.times do
    sc.nop("key")
  end
}

puts " read ..."
read = Benchmark.realtime {
  n.times do
    sc.read("key")
  end
}

puts " write ..."
write = Benchmark.realtime {
  n.times do
    sc.write("key", "value")
  end
}

puts " test and set ..."
test_and_set = Benchmark.realtime {
  n.times do
    sc.test_and_set("key", "value", "value")
  end
}

puts " request list processing ..."
reql = Benchmark.realtime {
  n.times do
    req_list2(sc)
  end
}

puts "testing pub sub once more ..."
pubsub(sc)

puts "testing range read once more (with pubsub data in the ring -- lists as values) ..."
range_query(sc)

printf("              time[s]\t1/s\n")
printf("nop          : %0.02f     %0.02f\n", nop, n/nop)
printf("read         : %0.02f     %0.02f\n", read, n/read)
printf("write        : %0.02f     %0.02f\n", write, n/write)
printf("test_and_set : %0.02f     %0.02f\n", test_and_set, n/test_and_set)
printf("req_list     : %0.02f     %0.02f\n", reql, n/reql)
