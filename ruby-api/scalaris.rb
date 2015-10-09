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
begin
gem 'json', '>=1.4.1'
rescue LoadError
end
require 'json'
require 'net/http'
require 'base64'
require 'open-uri'

module InternalScalarisSimpleError
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
  def close_connection
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
  
  # default URL and port to a scalaris node
  if ENV.has_key?('SCALARIS_JSON_URL') and not ENV['SCALARIS_JSON_URL'].empty?
    DEFAULT_URL = ENV['SCALARIS_JSON_URL']
  else
    DEFAULT_URL = 'http://localhost:8000'
  end
  
  # path to the json rpc page
  DEFAULT_PATH = '/jsonrpc.yaws'
  
  # Base class for errors in the scalaris package.
  class ScalarisError < StandardError
  end
  
  # Exception that is thrown if a the commit of a write operation on a Scalaris
  # ring fails.
  class AbortError < ScalarisError
    attr_reader :raw_result
    attr_reader :failed_keys
    def initialize(raw_result, failed_keys)
      @raw_result = raw_result
      @failed_keys = failed_keys
    end
  
    def to_s
      @raw_result
    end
  end

  # Exception that is thrown if an operation on a Scalaris ring fails because
  # a connection does not exist or has been disconnected.
  class ConnectionError < ScalarisError
    include InternalScalarisSimpleError
  end

  # Exception that is thrown if a test_and_set operation on a Scalaris ring
  # fails because the old value did not match the expected value.
  class KeyChangedError < ScalarisError
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
  class NodeNotFoundError < ScalarisError
    include InternalScalarisSimpleError
  end

  # Exception that is thrown if a read operation on a Scalaris ring fails
  # because the key did not exist before.
  class NotFoundError < ScalarisError
    include InternalScalarisSimpleError
  end

  # Exception that is thrown if a add_del_on_list operation on a scalaris ring
  # fails because the participating values are not lists.
  class NotAListError < ScalarisError
    include InternalScalarisSimpleError
  end

  # Exception that is thrown if a add_del_on_list operation on a scalaris ring
  # fails because the participating values are not numbers.
  class NotANumberError < ScalarisError
    include InternalScalarisSimpleError
  end

  # Exception that is thrown if a read or write operation on a Scalaris ring
  # fails due to a timeout.
  class TimeoutError < ScalarisError
    include InternalScalarisSimpleError
  end

  # Generic exception that is thrown during operations on a Scalaris ring, e.g.
  # if an unknown result has been returned.
  class UnknownError < ScalarisError
    include InternalScalarisSimpleError
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
    # Creates a JSON connection to the given URL using the given TCP timeout (or default)
    def initialize(url = DEFAULT_URL, timeout = nil)
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
        unless @timeout.nil?
          @conn.read_timeout = @timeout
        end
      end
    end
    private :start
    
    # Calls the given function with the given parameters via the JSON
    # interface of Scalaris.
    def call(function, params)
      start
      req = Net::HTTP::Post.new(DEFAULT_PATH)
      req.add_field('Content-Type', 'application/json; charset=utf-8')
      req.body = URI::encode({
        :jsonrpc => :'2.0',
        :method => function,
        :params => params,
        :id => 0 }.to_json({:ascii_only => true}))
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

    # Processes the result of some Scalaris operation and raises a
    # TimeoutError if found.
    #
    # result: {'status': 'ok'} or
    #         {'status': 'fail', 'reason': 'timeout'}
    def self.check_fail_abort(result)
      if result == {:status => 'fail', :reason => 'timeout'}
        raise TimeoutError.new(result)
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
    #         {'status' => 'fail', 'reason' => 'abort', 'keys' => <list>} or
    #         {'status' => 'fail', 'reason' => 'timeout'}
    def self.process_result_commit(result)
      if result.is_a?(Hash) and result.has_key?('status')
        if result == {'status' => 'ok'}
          return true
        elsif result['status'] == 'fail' and result.has_key?('reason')
          if result.length == 2 and result['reason'] == 'timeout'
            raise TimeoutError.new(result)
          elsif result.length == 3 and result['reason'] == 'abort' and result.has_key?('keys')
            raise AbortError.new(result, result['keys'])
          end
        end
      end
      raise UnknownError.new(result)
    end

    # Processes the result of a add_del_on_list operation.
    # Raises the appropriate exception if the operation failed.
    #
    # results: {'status': 'ok'} or
    #          {'status': 'fail', 'reason': 'timeout' or 'not_a_list'}
    def self.process_result_add_del_on_list(result)
      if result.is_a?(Hash) and result.has_key?('status')
        if result == {'status' => 'ok'}
          return nil
        elsif result['status'] == 'fail' and result.has_key?('reason')
          if result.length == 2
            if result['reason'] == 'timeout'
              raise TimeoutError.new(result)
            elsif result['reason'] == 'not_a_list'
              raise NotAListError.new(result)
            end
          end
        end
      end
      raise UnknownError.new(result)
    end

    # Processes the result of a add_on_nr operation.
    # Raises the appropriate exception if the operation failed.
    #
    # results: {'status': 'ok'} or
    #          {'status': 'fail', 'reason': 'timeout' or 'not_a_number'}
    def self.process_result_add_on_nr(result)
      if result.is_a?(Hash) and result.has_key?('status')
        if result == {'status' => 'ok'}
          return nil
        elsif result['status'] == 'fail' and result.has_key?('reason')
          if result.length == 2
            if result['reason'] == 'timeout'
              raise TimeoutError.new(result)
            elsif result['reason'] == 'not_a_number'
              raise NotANumberError.new(result)
            end
          end
        end
      end
      raise UnknownError.new(result)
    end

    # Processes the result of a test_and_set operation.
    # Raises the appropriate exception if the operation failed.
    # 
    # results: {'status' => 'ok'} or
    #          {'status' => 'fail', 'reason' => 'timeout' or 'not_found'} or
    #          {'status' => 'fail', 'reason' => 'key_changed', 'value': xxx}
    def self.process_result_test_and_set(result)
      if result.is_a?(Hash) and result.has_key?('status')
        if result == {'status' => 'ok'}
          return nil
        elsif result['status'] == 'fail' and result.has_key?('reason')
          if result.length == 2
            if result['reason'] == 'timeout'
              raise TimeoutError.new(result)
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
          if element == 'ok'
              ok += 1
          elsif element == 'locks_set'
              locks_set += 1
          elsif element == 'undef'
              undefined += 1
          else
            raise UnknownError.new(:'Unknown reason ' + element + :'in ' + result)
          end
        end
        return DeleteResult.new(ok, locks_set, undefined)
      end
      raise UnknownError.new(:'Unknown result ' + result)
    end
    
    # Processes the result of a req_list operation of the Transaction class.
    # Returns the Array (:tlog => <tlog>, :result => <result>) on success.
    # Raises the appropriate exception if the operation failed.
    # 
    # results: {'tlog': xxx,
    #           'results': [{'status': 'ok'} or {'status': 'ok', 'value': xxx} or
    #                       {'status': 'fail', 'reason': 'timeout' or 'abort' or 'not_found'}]}
    def self.process_result_req_list_t(result)
      if (not result.has_key?('tlog')) or (not result.has_key?('results')) or
          (not result['results'].is_a?(Array))
        raise UnknownError.new(result)
      end
      {:tlog => result['tlog'], :result => result['results']}
    end

    # Processes the result of a req_list operation of the TransactionSingleOp class.
    # Returns <result> on success.
    # Raises the appropriate exception if the operation failed.
    # 
    # results: [{'status': 'ok'} or {'status': 'ok', 'value': xxx} or
    #           {'status': 'fail', 'reason': 'timeout' or 'abort' or 'not_found'}]
    def self.process_result_req_list_tso(result)
      if not result.is_a?(Array)
        raise UnknownError.new(result)
      end
      result
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
    
    # Returns a new ReqList object allowing multiple parallel requests for
    # the Transaction class.
    def self.new_req_list_t(other = nil)
      JSONReqListTransaction.new(other)
    end
    
    # Returns a new ReqList object allowing multiple parallel requests for
    # the TransactionSingleOp class.
    def self.new_req_list_tso(other = nil)
      JSONReqListTransactionSingleOp.new(other)
    end

    def close
      if @conn.started?
        @conn.finish()
      end
    end
  end
  
  # Single write or read operations on Scalaris.
  class TransactionSingleOp
    # Create a new object using the given connection
    def initialize(conn = JSONConnection.new())
      @conn = conn
    end
    
    # Returns a new ReqList object allowing multiple parallel requests.
    def new_req_list(other = nil)
      @conn.class.new_req_list_t(other)
    end
    
    # Issues multiple parallel requests to scalaris; each will be committed.
    # NOTE: The execution order of multiple requests on the same key is
    # undefined!
    # Request lists can be created using new_req_list().
    # The returned list has the following form:
    # [{'status': 'ok'} or {'status': 'ok', 'value': xxx} or
    # {'status': 'fail', 'reason': 'timeout' or 'abort' or 'not_found'}].
    # Elements of this list can be processed with process_result_read() and
    # process_result_write().
    def req_list(reqlist)
      result = @conn.call(:req_list_commit_each, [reqlist.get_requests()])
      @conn.class.process_result_req_list_tso(result)
    end

    # Processes a result element from the list returned by req_list() which
    # originated from a read operation.
    # Returns the read value on success.
    # Raises the appropriate exceptions if a failure occurred during the
    # operation.
    # Beware: lists of (small) integers may be (falsely) returned as a string -
    # use str_to_list() to convert such strings.
    def process_result_read(result)
      @conn.class.process_result_read(result)
    end

    # Processes a result element from the list returned by req_list() which
    # originated from a write operation.
    # Raises the appropriate exceptions if a failure occurred during the
    # operation.
    def process_result_write(result)
      # note: we need to process a commit result as the write has been committed
      @conn.class.check_fail_abort(result)
      @conn.class.process_result_commit(result)
    end
    
    # Processes a result element from the list returned by req_list() which
    # originated from a add_del_on_list operation.
    # Raises the appropriate exceptions if a failure occurred during the
    # operation.
    def process_result_add_del_on_list(result)
      @conn.class.check_fail_abort(result)
      @conn.class.process_result_add_del_on_list(result)
    end

    # Processes a result element from the list returned by req_list() which
    # originated from a add_on_nr operation.
    # Raises the appropriate exceptions if a failure occurred during the
    # operation.
    def process_result_add_on_nr(result)
      @conn.class.check_fail_abort(result)
      @conn.class.process_result_add_on_nr(result)
    end

    # Processes a result element from the list returned by req_list() which
    # originated from a test_and_set operation.
    # Raises the appropriate exceptions if a failure occurred during the
    # operation.
    def process_result_test_and_set(result)
      @conn.class.check_fail_abort(result)
      @conn.class.process_result_test_and_set(result)
    end

    # Read the value at key.
    # Beware: lists of (small) integers may be (falsely) returned as a string -
    # use str_to_list() to convert such strings.
    def read(key)
      result = @conn.call(:read, [key])
      @conn.class.process_result_read(result)
    end

    # Write the value to key.
    def write(key, value, binary = false)
      value = @conn.class.encode_value(value, binary)
      result = @conn.call(:write, [key, value])
      @conn.class.check_fail_abort(result)
      @conn.class.process_result_commit(result)
    end

    # Changes the list stored at the given key, i.e. first adds all items in
    # to_add then removes all items in to_remove.
    # Both, to_add and to_remove, must be lists.
    # Assumes en empty list if no value exists at key.
    def add_del_on_list(key, to_add, to_remove)
      result = @conn.call(:add_del_on_list, [key, to_add, to_remove])
      @conn.class.check_fail_abort(result)
      @conn.class.process_result_add_del_on_list(result)
    end

    # Changes the number stored at the given key, i.e. adds some value.
    # Assumes 0 if no value exists at key.
    def add_on_nr(key, to_add)
      result = @conn.call(:add_on_nr, [key, to_add])
      @conn.class.check_fail_abort(result)
      @conn.class.process_result_add_on_nr(result)
    end

    # Atomic test and set, i.e. if the old value at key is old_value, then
    # write new_value.
    def test_and_set(key, old_value, new_value)
      result = @conn.call(:test_and_set, [key, old_value, new_value])
      @conn.class.check_fail_abort(result)
      @conn.class.process_result_test_and_set(result)
    end

    # Atomic test and set, i.e. if the old value at key is oldvalue, then
    # write newvalue.
    def test_and_set(key, oldvalue, newvalue)
      oldvalue = @conn.class.encode_value(oldvalue)
      newvalue = @conn.class.encode_value(newvalue)
      result = @conn.call(:test_and_set, [key, oldvalue, newvalue])
      @conn.class.check_fail_abort(result)
      @conn.class.process_result_test_and_set(result)
    end

    include InternalScalarisNopClose
  end

  # Write or read operations on Scalaris inside a transaction.
  class Transaction
    # Create a new object using the given connection
    def initialize(conn = JSONConnection.new())
      @conn = conn
      @tlog = nil
    end
    
    # Returns a new ReqList object allowing multiple parallel requests.
    def new_req_list(other = nil)
      @conn.class.new_req_list_t(other)
    end
    
    # Issues multiple parallel requests to Scalaris.
    # Request lists can be created using new_req_list().
    # The returned list has the following form:
    # [{'status': 'ok'} or {'status': 'ok', 'value': xxx} or
    # {'status': 'fail', 'reason': 'timeout' or 'abort' or 'not_found'}].
    # The elements of this list can be processed with process_result_read(),
    # process_result_write() and process_result_commit().
    def req_list(reqlist)
      if @tlog == nil
        result = @conn.call(:req_list, [reqlist.get_requests()])
      else
        result = @conn.call(:req_list, [@tlog, reqlist.get_requests()])
      end
      result = @conn.class.process_result_req_list_t(result)
      @tlog = result[:tlog]
      result = result[:result]
      if reqlist.is_commit()
        _process_result_commit(result[-1])
        # transaction was successful: reset transaction log
        @tlog = nil
      end
      result
    end
    
    # Processes a result element from the list returned by req_list() which
    # originated from a read operation.
    # Returns the read value on success.
    # Raises the appropriate exceptions if a failure occurred during the
    # operation.
    # Beware: lists of (small) integers may be (falsely) returned as a string -
    # use str_to_list() to convert such strings.
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
    # originated from a add_del_on_list operation.
    # Raises the appropriate exceptions if a failure occurred during the
    # operation.
    def process_result_add_del_on_list(result)
      @conn.class.process_result_add_del_on_list(result)
    end

    # Processes a result element from the list returned by req_list() which
    # originated from a add_on_nr operation.
    # Raises the appropriate exceptions if a failure occurred during the
    # operation.
    def process_result_add_on_nr(result)
      @conn.class.process_result_add_on_nr(result)
    end

    # Processes a result element from the list returned by req_list() which
    # originated from a test_and_set operation.
    # Raises the appropriate exceptions if a failure occurred during the
    # operation.
    def process_result_test_and_set(result)
      @conn.class.process_result_test_and_set(result)
    end
    
    # Processes a result element from the list returned by req_list() which
    # originated from a commit operation.
    # Raises the appropriate exceptions if a failure occurred during the
    # operation.
    def _process_result_commit(result)
      @conn.class.process_result_commit(result)
    end

    private :_process_result_commit
    
    # Issues a commit operation to Scalaris validating the previously
    # created operations inside the transaction.
    def commit
      result = req_list(new_req_list().add_commit())[0]
      _process_result_commit(result)
      # reset tlog (minor optimization which is not done in req_list):
      @tlog = nil
    end
    
    # Aborts all previously created operations inside the transaction.
    def abort
      @tlog = nil
    end
    
    # Issues a read operation to Scalaris, adds it to the current
    # transaction and returns the result.
    # Beware: lists of (small) integers may be (falsely) returned as a string -
    # use str_to_list() to convert such strings.
    def read(key)
      result = req_list(new_req_list().add_read(key))[0]
      return process_result_read(result)
    end
    
    # Issues a write operation to Scalaris and adds it to the current
    # transaction.
    def write(key, value, binary = false)
      result = req_list(new_req_list().add_write(key, value, binary))[0]
      _process_result_commit(result)
    end

    # Issues a add_del_on_list operation to scalaris and adds it to the
    # current transaction.
    # Changes the list stored at the given key, i.e. first adds all items in
    # to_add then removes all items in to_remove.
    # Both, to_add and to_remove, must be lists.
    # Assumes en empty list if no value exists at key.
    def add_del_on_list(key, to_add, to_remove)
      result = req_list(new_req_list().add_add_del_on_list(key, to_add, to_remove))[0]
      process_result_add_del_on_list(result)
    end

    # Issues a add_on_nr operation to scalaris and adds it to the
    # current transaction.
    # Changes the number stored at the given key, i.e. adds some value.
    # Assumes 0 if no value exists at key.
    def add_on_nr(key, to_add)
      result = req_list(new_req_list().add_add_on_nr(key, to_add))[0]
      process_result_add_on_nr(result)
    end

    # Issues a test_and_set operation to scalaris and adds it to the
    # current transaction.
    # Atomic test and set, i.e. if the old value at key is old_value, then
    # write new_value.
    def test_and_set(key, old_value, new_value)
      result = req_list(new_req_list().add_test_and_set(key, old_value, new_value))[0]
      process_result_test_and_set(result)
    end

    include InternalScalarisNopClose
  end

  # Request list for use with Transaction.req_list()
  class JSONReqList
    # Create a new object using a JSON connection.
    def initialize(other = nil)
      @requests = []
      @is_commit = false
      if not other == nil
        concat(other)
      end
    end
    
    # Adds a read operation to the request list.
    def add_read(key)
      if (@is_commit)
          raise RuntimeError.new('No further request supported after a commit!')
      end
      @requests << {'read' => key}
      self
    end
    
    # Adds a write operation to the request list.
    def add_write(key, value, binary = false)
      if (@is_commit)
          raise RuntimeError.new('No further request supported after a commit!')
      end
      @requests << {'write' => {key => JSONConnection.encode_value(value, binary)}}
      self
    end

    # Adds a add_del_on_list operation to the request list.
    def add_add_del_on_list(key, to_add, to_remove)
      if (@is_commit)
          raise RuntimeError.new('No further request supported after a commit!')
      end
      @requests << {'add_del_on_list' => {'key' => key, 'add' => to_add, 'del'=> to_remove}}
      self
    end

    # Adds a add_on_nr operation to the request list.
    def add_add_on_nr(key, to_add)
      if (@is_commit)
          raise RuntimeError.new('No further request supported after a commit!')
      end
      @requests << {'add_on_nr' => {key => to_add}}
      self
    end

    # Adds a test_and_set operation to the request list.
    def add_test_and_set(key, old_value, new_value)
      if (@is_commit)
          raise RuntimeError.new('No further request supported after a commit!')
      end
      @requests << {'test_and_set' => {'key' => key,
          'old' => JSONConnection.encode_value(old_value, false),
          'new' => JSONConnection.encode_value(new_value, false)}}
      self
    end

    # Adds a commit operation to the request list.
    def add_commit
      if (@is_commit)
          raise RuntimeError.new('Only one commit per request list allowed!')
      end
      @requests << {'commit' => ''}
      @is_commit = true
      self
    end
    
    # Gets the collected requests.
    def get_requests
      @requests
    end

    # Returns whether the transactions contains a commit or not.
    def is_commit()
      @is_commit
    end
    
    # Checks whether the request list is empty.
    def is_empty()
      @requests.empty?
    end
    
    # Gets the number of requests in the list.
    def size()
      @requests.length
    end

    # Adds all requests of the other request list to the end of this list.
    def concat(other)
      @requests.concat(other.get_requests())
      self
    end
  end

  # Request list for use with Transaction.req_list().
  class JSONReqListTransaction < JSONReqList
    def initialize(other = nil)
      super(other)
    end
  end
  
  # Request list for use with TransactionSingleOp.req_list() which does not
  # support commits.
  class JSONReqListTransactionSingleOp < JSONReqList
    def initialize(other = nil)
      super(other)
    end
    
    # Adds a commit operation to the request list.
    def add_commit()
      raise RuntimeError.new('No commit allowed in TransactionSingleOp.req_list()!')
    end
  end

  # Non-transactional operations on the replicated DHT of Scalaris
  class ReplicatedDHT
    # Create a new object using the given connection.
    def initialize(conn = JSONConnection.new())
      @conn = conn
    end
    
    # Tries to delete the value at the given key.
    # 
    # WARNING: This function can lead to inconsistent data (e.g. deleted items
    # can re-appear). Also when re-creating an item the version before the
    # delete can re-appear.
    # 
    # returns the number of successfully deleted items
    # use get_last_delete_result() to get more details
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
    def get_last_delete_result
      @conn.class.create_delete_result(@lastDeleteResult)
    end

    include InternalScalarisNopClose
  end

  # API for using routing tables
  class RoutingTable
    # Create a new object using the given connection.
    def initialize(conn = JSONConnection.new())
      @conn = conn
    end

    def get_replication_factor()
      result_raw = @conn.call(:get_replication_factor, [])
      if result_raw.is_a?(Hash) and result_raw.has_key?('status') and result_raw.has_key?('value')
        return result_raw['value']
      else
        raise UnknownError.new(result)
      end
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
