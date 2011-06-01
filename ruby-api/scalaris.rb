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

class Scalaris
  #$url = 'http://localhost:8000/jsonrpc.yaws'

  def initialize(url = 'http://localhost:8000/jsonrpc.yaws')
    @url = url
    @uri = URI.parse @url
  end

  def json_call(function, params)
    req = Net::HTTP::Post.new(@uri.path)
    req.add_field 'Content-Type', 'application/json'
    req.body =
      {
      :jsonrpc => '2.0',
      :method => function,
      :params => params,
      :id => 0}.to_json
    res = Net::HTTP.start(@uri.host, @uri.port){|http|http.request(req)}
    JSON.parse(res.body)['result']
  end

  def _json_encode_value(value)
    # TODO: encode to base64 if binary
    return { :type => 'as_is',
      :value => value }
  end

  def _json_decode_value(value)
    if value['type'] == 'as_bin':
        # TODO: decode from base64 if binary
        return value['value']
    else
      return value['value']
    end
  end

  def _json_replace_value_in_result(result)
    if result.has_key?('value')
      result['value'] = _json_decode_value(result['value'])
    end
    return result
  end

  def _json_replace_value_in_results(results)
    results.each do |result|
      _json_replace_value_in_result(result)
    end
  end

  def test_and_set(key, oldvalue, newvalue)
    result = json_call('test_and_set', [key, _json_encode_value(oldvalue),
                                        _json_encode_value(newvalue)])
    _json_replace_value_in_result(result)
  end

  def read(key)
    result = json_call('read', [key])
    _json_replace_value_in_result(result)
  end

  def write(key, value)
    json_call('write', [key, _json_encode_value(value)])
  end

  def nop(value)
    json_call('nop', [_json_encode_value(value)])
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
end
