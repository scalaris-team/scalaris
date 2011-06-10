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

# default URL and port to a scalaris node

if ENV.has_key?('SCALARIS_JSON_URL')
  $DEFAULT_URL = ENV['SCALARIS_JSON_URL']
else
  $DEFAULT_URL = 'http://localhost:8000'
end
    
# socket timeout in seconds
$DEFAULT_TIMEOUT = 5
# path to the json rpc page
$DEFAULT_PATH = '/jsonrpc.yaws'

module ScalarisSimpleException
  attr_reader :raw_result
  def initialize(raw_result)
    @raw_result = raw_result
  end

  def to_s
    @raw_result
  end
end

module Scalaris
  # Exception that is thrown if a the commit of a write operation on a Scalaris
  # ring fails.
  class AbortError < StandardError
    include ScalarisSimpleException
  end

  # Exception that is thrown if an operation on a Scalaris ring fails because
  # a connection does not exist or has been disconnected.
  class ConnectionError < StandardError
    include ScalarisSimpleException
  end

  # Exception that is thrown if a test_and_set operation on a Scalaris ring
  # fails because the old value did not match the expected value.
  class KeyChangedError < StandardError
    attr_reader :raw_result
    attr_reader :old_value
    def initialize(raw_result, old_value)
      @raw_result = raw_result
      @old_value = old_value
    end

    def to_s
      @raw_result + ", old value: " + @old_value
    end
  end

  # Exception that is thrown if a delete operation on a Scalaris ring fails
  # because no Scalaris node was found.
  class NodeNotFoundError < StandardError
    include ScalarisSimpleException
  end

  # Exception that is thrown if a read operation on a Scalaris ring fails
  # because the key did not exist before.
  class NotFoundError < StandardError
    include ScalarisSimpleException
  end

  # Exception that is thrown if a read or write operation on a Scalaris ring
  # fails due to a timeout.
  class TimeoutError < StandardError
    include ScalarisSimpleException
  end

  # Generic exception that is thrown during operations on a Scalaris ring, e.g.
  # if an unknown result has been returned.
  class UnknownError < StandardError
    include ScalarisSimpleException
  end
  
  # Abstracts connections to Scalaris using JSON
  class JSONConnection
    # Creates a JSON connection to the given URL using the given TCP timeout
    def initialize(url = $DEFAULT_URL, timeout = $DEFAULT_TIMEOUT)
      begin
      @uri = URI.parse(url)
      @timeout = timeout
      start
      rescue Exception => error
        raise ConnectionError.new(error)
      end
    end
    
    def start
      if @conn == nil or not @conn.started?
        @conn = Net::HTTP.start(@uri.host, @uri.port)
        @conn.read_timeout = @timeout
      end
    end
    private :start
    
    # Calls the given function with the given parameters via the JSON
    # interface of Scalaris.
    def call(function, params)
      start
      req = Net::HTTP::Post.new($DEFAULT_PATH)
      req.add_field('Content-Type', 'application/json; charset=utf-8')
      req.body =  {
        :jsonrpc => :'2.0',
        :method => function,
        :params => params,
        :id => 0 }.to_json
      begin
        res = @conn.request(req)
        if res.is_a?(Net::HTTPSuccess)
          # TODO: is a decode from UTF-8 necessary as in python?
          data = res.body
          return JSON.parse(data)['result']
        else
          raise ConnectionError.new(res)
        end
      rescue ConnectionError => error
        raise error
      rescue Exception => error
        raise ConnectionError.new(error)
      end
    end
  
    # Encodes the value to the form required by the Scalaris JSON API
    def encode_value(value)
      # TODO: encode to base64 if binary
      # python: {'type': 'as_bin', 'value': (base64.b64encode(value)).decode('ascii')}
      return { :type => :as_is,  :value => value }
    end
  
    # Decodes the value from the Scalaris JSON API form to a native type
    def decode_value(value)
      if not (value.has_key?('type') and value.has_key?('value'))
        raise ConnectionError.new(value)
      end
      if value['type'] == 'as_bin'
        # TODO: decode from base64 if binary
        # python: bytearray(base64.b64decode(value['value'].encode('ascii')))
        return value['value']
      else
        return value['value']
      end
    end

    # Processes the result of a read operation.
    # Returns the read value on success.
    # Raises the appropriate exception if the operation failed.
    # 
    # result: {'status' => 'ok', 'value': xxx} or
    #         {'status' => 'fail', 'reason' => 'timeout' or 'not_found'}
    def process_result_read(result)
      if result.is_a?(Hash) and result.has_key?('status') and result.length == 2
        if result['status'] == 'ok' and result.has_key?('value')
          return decode_value(result['value'])
        elsif result['status'] == 'fail' and result.has_key?('reason')
          if result['reason'] == 'timeout'
            raise TimeoutError.new(result)
          elsif result['reason'] == 'not_found'
            raise NotFoundError.new(result)
          end
        end
      end
      raise UnknownError.new(result)
    end

    # Processes the result of a write operation.
    # Raises the appropriate exception if the operation failed.
    # 
    # result: {'status' => 'ok'} or
    #         {'status' => 'fail', 'reason' => 'timeout'}
    def process_result_write(result)
      if result.is_a?(Hash)
        if result == {'status' => 'ok'}
          return true
        elsif result == {'status' => 'fail', 'reason' => 'timeout'}
          raise TimeoutError.new(result)
        end
      end
      raise UnknownError.new(result)
    end

    # Processes the result of a commit operation.
    # Raises the appropriate exception if the operation failed.
    # 
    # result: {'status' => 'ok'} or
    #         {'status' => 'fail', 'reason' => 'timeout' or 'abort'}
    def process_result_commit(result)
      if result.is_a?(Hash) and result.has_key?('status')
        if result == {'status' => 'ok'}
          return true
        elsif result['status'] == 'fail' and result.has_key?('reason') and result.length == 2
          if result['reason'] == 'timeout'
            raise TimeoutError.new(result)
          elsif result['reason'] == 'abort'
            raise AbortError.new(result)
          end
        end
      end
      raise UnknownError.new(result)
    end

    # Processes the result of a testAndSet operation.
    # Raises the appropriate exception if the operation failed.
    # 
    # results: {'status' => 'ok'} or
    #          {'status' => 'fail', 'reason' => 'timeout' or 'abort' or 'not_found'} or
    #          {'status' => 'fail', 'reason' => 'key_changed', 'value': xxx}
    def process_result_testAndSet(result)
      if result.is_a?(Hash) and result.has_key?('status')
        if result == {'status' => 'ok'}
          return nil
        elsif result['status'] == 'fail' and result.has_key?('reason')
          if result.length == 2
            if result['reason'] == 'timeout'
              raise TimeoutError.new(result)
            elsif result['reason'] == 'abort'
              raise AbortError.new(result)
            elsif result['reason'] == 'not_found'
              raise NotFoundError.new(result)
            end
          elsif result['reason'] == 'key_changed' and result.has_key?('value') and result.length == 3
            raise KeyChangedError(result, decode_value(result['value']))
          end
        end
      end
      raise UnknownError.new(result)
    end

    # Processes the result of a nop operation.
    # Raises the appropriate exception if the operation failed.
    # 
    # result: 'ok'
    def process_result_nop(result)
      if result != 'ok'
        raise UnknownError.new(result)
      end
    end

    def close
      @conn.finish()
    end
  end
  
  # deprecated:
  class Scalaris
    def initialize(url = $DEFAULT_URL)
      @url = url + $DEFAULT_PATH
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
  
  # Single write or read operations on Scalaris.
  class TransactionSingleOp
    # Create a new object using the given connection
    def initialize(conn = JSONConnection.new)
      @conn = conn
    end
    
    # Read the value at key
    def read(key)
      result = @conn.call(:read, [key])
      return @conn.process_result_read(result)
    end

    # Write the value to key
    def write(key, value)
      value = @conn.encode_value(value)
      result = @conn.call('write', [key, value])
      @conn.process_result_commit(result)
    end

    # Atomic test and set, i.e. if the old value at key is oldvalue, then
    # write newvalue.
    def testAndSet(key, oldvalue, newvalue)
      oldvalue = @conn.encode_value(oldvalue)
      newvalue = @conn.encode_value(newvalue)
      result = @conn.call('test_and_set', [key, oldvalue, newvalue])
      @conn.process_result_testAndSet(result)
    end

    # No operation (may be used for measuring the JSON overhead)
    def nop(value)
      value = @conn.encode_value(value)
      result = @conn.call('nop', [value])
      @conn.process_result_nop(result)
    end

    # Close the connection to Scalaris
    # (it will automatically be re-opened on the next request)
    def closeConnection
      @conn.close()
    end
  end
  
end
