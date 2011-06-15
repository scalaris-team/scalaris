#!/usr/bin/ruby -KU
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
require 'base64'

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

module InternalScalarisSimpleException
  attr_reader :raw_result
  def initialize(raw_result)
    @raw_result = raw_result
  end

  def to_s
    @raw_result
  end
end

module InternalScalarisNopClose
  # No operation (may be used for measuring the JSON overhead).
  def nop(value)
    value = @conn.class.encode_value(value)
    result = @conn.call(:nop, [value])
    @conn.class.process_result_nop(result)
  end
  
  # Close the connection to Scalaris
  # (it will automatically be re-opened on the next request).
  def closeConnection
    @conn.close()
  end
end

# work around floating point numbers not being printed precisely enough
class Float
  # note: can not override to_json (this is not done recursively, e.g. in a Hash, before ruby 1.9)
  alias_method :orig_t_s, :to_s
  def to_s
    if not finite?
      orig_to_json(*a)
    else
      sprintf("%#.17g", self)
    end
  end
end

module Scalaris
  # Exception that is thrown if a the commit of a write operation on a Scalaris
  # ring fails.
  class AbortError < StandardError
    include InternalScalarisSimpleException
  end

  # Exception that is thrown if an operation on a Scalaris ring fails because
  # a connection does not exist or has been disconnected.
  class ConnectionError < StandardError
    include InternalScalarisSimpleException
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
    include InternalScalarisSimpleException
  end

  # Exception that is thrown if a read operation on a Scalaris ring fails
  # because the key did not exist before.
  class NotFoundError < StandardError
    include InternalScalarisSimpleException
  end

  # Exception that is thrown if a read or write operation on a Scalaris ring
  # fails due to a timeout.
  class TimeoutError < StandardError
    include InternalScalarisSimpleException
  end

  # Generic exception that is thrown during operations on a Scalaris ring, e.g.
  # if an unknown result has been returned.
  class UnknownError < StandardError
    include InternalScalarisSimpleException
  end

  # Stores the result of a delete operation.
  class DeleteResult
    attr_reader :ok
    attr_reader :locks_set
    attr_reader :undefined
    def initialize(ok, locks_set, undefined)
      @ok = ok
      @locks_set = locks_set
      @undefined = undefined
    end
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
        :id => 0 }.to_json({:ascii_only => true})
      begin
        res = @conn.request(req)
        if res.is_a?(Net::HTTPSuccess)
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
    def self.encode_value(value, binary = false)
      if binary
        return { :type => :as_bin, :value => Base64.encode64(value) }
      else
        return { :type => :as_is, :value => value }
      end
    end
  
    # Decodes the value from the Scalaris JSON API form to a native type
    def self.decode_value(value)
      if not (value.has_key?('type') and value.has_key?('value'))
        raise ConnectionError.new(value)
      end
      if value['type'] == 'as_bin'
        return Base64.decode64(value['value'])
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
    def self.process_result_read(result)
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
    def self.process_result_write(result)
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
    def self.process_result_commit(result)
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
    def self.process_result_testAndSet(result)
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
            raise KeyChangedError.new(result, decode_value(result['value']))
          end
        end
      end
      raise UnknownError.new(result)
    end
    
    # Processes the result of a publish operation.
    # Raises the appropriate exception if the operation failed.
    # 
    # results: {'status': 'ok'}
    def self.process_result_publish(result)
      if result == {'status' => 'ok'}
        return nil
      end
      raise UnknownError.new(result)
    end
    
    # Processes the result of a subscribe operation.
    # Raises the appropriate exception if the operation failed.
    # 
    # results: {'status': 'ok'} or
    #          {'status': 'fail', 'reason': 'timeout' or 'abort'}
    def self.process_result_subscribe(result)
        process_result_commit(result)
    end
    
    # Processes the result of a unsubscribe operation.
    # Raises the appropriate exception if the operation failed.
    #  
    # results: {'status': 'ok'} or
    #          {'status': 'fail', 'reason': 'timeout' or 'abort' or 'not_found'}
    def self.process_result_unsubscribe(result)
      if result == {'status' => 'ok'}
        return nil
      elsif result.is_a?(Hash) and result.has_key?('status')
        if result['status'] == 'fail' and result.has_key?('reason') and result.length == 2
          if result['reason'] == 'timeout'
            raise TimeoutError.new(result)
          elsif result['reason'] == 'abort'
            raise AbortError.new(result)
          elsif result['reason'] == 'not_found'
            raise NotFoundError.new(result)
          end
        end
      end
      raise UnknownError.new(result)
    end
    
    # Processes the result of a getSubscribers operation.
    # Returns the list of subscribers on success.
    # Raises the appropriate exception if the operation failed.
    # 
    # results: [urls=str()]
    def self.process_result_getSubscribers(result)
      if result.is_a?(Array)
        return result
      end
      raise UnknownError.new(result)
    end

    # Processes the result of a delete operation.
    # Returns an Array of
    # {:success => true | :timeout, :ok => <number of deleted items>, :results => <detailed results>}
    # on success.
    # Does not raise an exception if the operation failed unless the result
    # is invalid!
    # 
    # results: {'ok': xxx, 'results': ['ok' or 'locks_set' or 'undef']} or
    #          {'failure': 'timeout', 'ok': xxx, 'results': ['ok' or 'locks_set' or 'undef']}
    def self.process_result_delete(result)
      if result.is_a?(Hash) and result.has_key?('ok') and result.has_key?('results')
        if not result.has_key?('failure')
          return {:success => true,
            :ok => result['ok'],
            :results => result['results']}
        elsif result['failure'] == 'timeout'
          return {:success => :timeout,
            :ok => result['ok'],
            :results => result['results']}
        end
      end
      raise UnknownError.new(result)
    end
    
    # Creates a new DeleteResult from the given result list.
    # 
    # result: ['ok' or 'locks_set' or 'undef']
    def self.create_delete_result(result)
      ok = 0
      locks_set = 0
      undefined = 0
      if result.is_a?(Array)
        for element in result
          if element == 'ok':
              ok += 1
          elsif element == 'locks_set':
              locks_set += 1
          elsif element == 'undef':
              undefined += 1
          else
            raise UnknownError.new(:'Unknown reason ' + element + :'in ' + result)
          end
        end
        return DeleteResult.new(ok, locks_set, undefined)
      end
      raise UnknownError.new(:'Unknown result ' + result)
    end
    
    # Processes the result of a req_list operation.
    # Returns the Array (:tlog => <tlog>, :result => <result>) on success.
    # Raises the appropriate exception if the operation failed.
    # 
    # results: {'tlog': xxx,
    #           'results': [{'status': 'ok'} or {'status': 'ok', 'value': xxx} or
    #                       {'status': 'fail', 'reason': 'timeout' or 'abort' or 'not_found'}]}
    def self.process_result_req_list(result)
      if (not result.has_key?('tlog')) or (not result.has_key?('results')) or
        (not result['results'].is_a?(Array)) or result['results'].length < 1
          raise UnknownError.new(result)
      end
      {:tlog => result['tlog'], :result => result['results']}
    end

    # Processes the result of a nop operation.
    # Raises the appropriate exception if the operation failed.
    # 
    # result: 'ok'
    def self.process_result_nop(result)
      if result != 'ok'
        raise UnknownError.new(result)
      end
    end
    
    # Returns a new ReqList object allowing multiple parallel requests.
    def self.newReqList
      JSONReqList.new()
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
      @conn.class.process_result_read(result)
    end

    # Write the value to key
    def write(key, value, binary = false)
      value = @conn.class.encode_value(value, binary)
      result = @conn.call(:write, [key, value])
      @conn.class.process_result_commit(result)
    end

    # Atomic test and set, i.e. if the old value at key is oldvalue, then
    # write newvalue.
    def testAndSet(key, oldvalue, newvalue)
      oldvalue = @conn.class.encode_value(oldvalue)
      newvalue = @conn.class.encode_value(newvalue)
      result = @conn.call(:test_and_set, [key, oldvalue, newvalue])
      @conn.class.process_result_testAndSet(result)
    end

    include InternalScalarisNopClose
  end

  # Write or read operations on Scalaris inside a transaction.
  class Transaction
    # Create a new object using the given connection
    def initialize(conn = JSONConnection())
      @conn = conn
      @tlog = nil
    end
    
    # Returns a new ReqList object allowing multiple parallel requests.
    def newReqList
      @conn.class.newReqList()
    end
    
    # Issues multiple parallel requests to Scalaris.
    # Request lists can be created using newReqList().
    # The returned list has the following form:
    # [{'status': 'ok'} or {'status': 'ok', 'value': xxx} or
    # {'status': 'fail', 'reason': 'timeout' or 'abort' or 'not_found'}].
    # The elements of this list can be processed with process_result_read(),
    # process_result_write() and process_result_commit().
    def req_list(reqlist)
      if @tlog == nil
        result = @conn.call(:req_list, [reqlist.getRequests()])
      else
        result = @conn.call(:req_list, [@tlog, reqlist.getRequests()])
      end
      result = @conn.class.process_result_req_list(result)
      @tlog = result[:tlog]
      result[:result]
    end
    
    # Processes a result element from the list returned by req_list() which
    # originated from a read operation.
    # Returns the read value on success.
    # Raises the appropriate exceptions if a failure occurred during the
    # operation.
    def process_result_read(result)
      @conn.class.process_result_read(result)
    end
    
    # Processes a result element from the list returned by req_list() which
    # originated from a write operation.
    # Raises the appropriate exceptions if a failure occurred during the
    # operation.
    def process_result_write(result)
      @conn.class.process_result_write(result)
    end
    
    # Processes a result element from the list returned by req_list() which
    # originated from a commit operation.
    # Raises the appropriate exceptions if a failure occurred during the
    # operation.
    def process_result_commit(result)
      @conn.class.process_result_commit(result)
    end
    
    # Issues a commit operation to Scalaris validating the previously
    # created operations inside the transaction.
    def commit
      result = req_list(newReqList().addCommit())[0]
      process_result_commit(result)
      # reset tlog (minor optimization which is not done in req_list):
      @tlog = nil
    end
    
    # Aborts all previously created operations inside the transaction.
    def abort
      @tlog = nil
    end
    
    # Issues a read operation to Scalaris and adds it to the current
    # transaction.
    def read(key)
      result = req_list(newReqList().addRead(key))[0]
      return process_result_read(result)
    end
    
    # Issues a write operation to Scalaris and adds it to the current
    # transaction.
    def write(key, value, binary = false)
      result = req_list(newReqList().addWrite(key, value, binary))[0]
      process_result_commit(result)
    end
    
    include InternalScalarisNopClose
  end

  # Request list for use with Transaction.req_list()
  class JSONReqList
    # Create a new object using a JSON connection.
    def initialize()
      @requests = []
    end
    
    # Adds a read operation to the request list.
    def addRead(key)
      @requests << {'read' => key}
      self
    end
    
    # Adds a write operation to the request list.
    def addWrite(key, value, binary = false)
      @requests << {'write' => {key => JSONConnection.encode_value(value, binary)}}
      self
    end
    
    # Adds a commit operation to the request list.
    def addCommit
      @requests << {'commit' => ''}
      self
    end
    
    # Gets the collected requests.
    def getRequests
      @requests
    end
  end

  # Publish and subscribe methods accessing Scalaris' pubsub system
  class PubSub
    # Create a new object using the given connection.
    def initialize(conn = JSONConnection())
      @conn = conn
    end
    
    # Publishes content under topic.
    def publish(topic, content)
      # note: do NOT encode the content, this is not decoded on the erlang side!
      # (only strings are allowed anyway)
      # content = @conn.class.encode_value(content)
      result = @conn.call(:publish, [topic, content])
      @conn.class.process_result_publish(result)
    end
    
    # Subscribes url for topic.
    def subscribe(topic, url)
      # note: do NOT encode the URL, this is not decoded on the erlang side!
      # (only strings are allowed anyway)
      # url = @conn.class.encode_value(url)
      result = @conn.call(:subscribe, [topic, url])
      @conn.class.process_result_subscribe(result)
    end
    
    # Unsubscribes url from topic.
    def unsubscribe(topic, url)
      # note: do NOT encode the URL, this is not decoded on the erlang side!
      # (only strings are allowed anyway)
      # url = @conn.class.encode_value(url)
      result = @conn.call(:unsubscribe, [topic, url])
      @conn.class.process_result_unsubscribe(result)
    end
    
    # Gets the list of all subscribers to topic.
    def getSubscribers(topic)
      result = @conn.call(:get_subscribers, [topic])
      @conn.class.process_result_getSubscribers(result)
    end
    
    include InternalScalarisNopClose
  end

  # Non-transactional operations on the replicated DHT of Scalaris
  class ReplicatedDHT
    # Create a new object using the given connection.
    def initialize(conn = JSONConnection())
      @conn = conn
    end
    
    # Tries to delete the value at the given key.
    # 
    # WARNING: This function can lead to inconsistent data (e.g. deleted items
    # can re-appear). Also when re-creating an item the version before the
    # delete can re-appear.
    # 
    # returns the number of successfully deleted items
    # use getLastDeleteResult() to get more details
    def delete(key, timeout = 2000)
      result_raw = @conn.call(:delete, [key, timeout])
      result = @conn.class.process_result_delete(result_raw)
      @lastDeleteResult = result[:results]
      if result[:success] == true
        return result[:ok]
      elsif result[:success] == :timeout
        raise TimeoutError.new(result_raw)
      else
        raise UnknownError.new(result_raw)
      end
    end
    
    # Returns the result of the last call to delete().
    # 
    # NOTE: This function traverses the result list returned by Scalaris and
    # therefore takes some time to process. It is advised to store the returned
    # result object once generated.
    def getLastDeleteResult
      @conn.class.create_delete_result(@lastDeleteResult)
    end

    include InternalScalarisNopClose
  end

  # Converts a string to a list of integers.
  # If the expected value of a read operation is a list, the returned value
  # could be (mistakenly) a string if it is a list of integers.
  def str_to_list(value)
    if value.is_a?(String)
      return value.unpack("U*")
    else
      return value
    end
  end
  
  module_function :str_to_list
end
