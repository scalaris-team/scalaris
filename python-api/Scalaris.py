#!/usr/bin/python
# Copyright 2011 Zuse Institute Berlin
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

import json, httplib, urlparse,  socket
import sys
import base64

default_url = 'http://localhost:8000/jsonrpc.yaws'
default_timeout = 5 # socket timeout in seconds

def _json_call(conn,  uri,  function, params):
    params = {'version': '1.1',
              'method': function,
              'params': params,
              'id': 0}
    #print params
    # use compact JSON encoding:
    params_json = json.dumps(params, separators=(',',':'))
    #print params_json
    # no need to quote - we already encode to json:
    headers = {"Content-type": "application/json"}
    try:
        #conn.request("POST", uri.path, urllib.quote(params_json), headers)
        conn.request("POST", uri.path, params_json, headers)
        response = conn.getresponse()
        #print response.status, response.reason
        if (response.status < 200 or response.status >= 300):
            raise ConnectionException(response)
        data = response.read()
    except socket.timeout,  (instance):
        raise ConnectionException(instance)
    #print data
    response_json = json.loads(data)
    #print response_json
    return response_json['result']

def _json_encode_value(value):
    if isinstance(value,  bytearray):
        return {'type': 'as_bin',  'value': base64.b64encode(value)}
    else:
        return {'type': 'as_is',  'value': value}

def _json_decode_value(value):
    if ('type' not in value) or ('value' not in value):
        raise UnknownException(value)
    if value['type'] == 'as_bin':
        return bytearray(base64.b64decode(value['value']),  'base64')
    else:
        return value['value']

def getConnection(url,  timeout = default_timeout):
    uri = urlparse.urlparse(url)
    return httplib.HTTPConnection(uri.hostname,  uri.port, timeout = timeout)
    
# result: {'status': 'ok'} or {'status': 'ok', 'value': xxx} or
#         {'status': 'fail', 'reason': 'timeout' or 'abort' or 'not_found}
# op: 'read' or 'write' or 'commit', 'test_and_set' or 'any'
def _process_result(result,  op):
    if isinstance(result,  dict) and 'status' in result:
        if result['status'] == 'ok':
            if 'value' in result:
                return _json_decode_value(result['value'])
            elif op == 'read':
                raise UnknownException((result,  op))
        elif result['status'] == 'fail' and 'reason' in result:
            if result['reason'] == 'timeout':
                raise TimeoutException(result)
            elif result['reason'] == 'not_found' and op not in ['commit',  'write']:
                raise NotFoundException(result)
            elif result['reason'] == 'abort' and op not in ['read',  'write']:
                raise AbortException(result)
            elif result['reason'] == 'key_changed' and op == 'test_and_set' and 'value' in result:
                raise KeyChangedException(result,  _json_decode_value(result['value']))
            else:
                raise UnknownException(result)
        else:
            raise UnknownException(result)
    else:
        raise UnknownException(result)

# Exception that is thrown if a the commit of a write operation on a
# scalaris ring fails.
class AbortException(Exception):
   def __init__(self, raw_result):
       self.raw_result = raw_result
   def __str__(self):
       return repr(self.raw_result)

# Exception that is thrown if an operation on a scalaris ring fails because
# a connection does not exist or has been disconnected.
class ConnectionException(Exception):
   def __init__(self, raw_result):
       self.raw_result = raw_result
   def __str__(self):
       return repr(self.raw_result)

# Exception that is thrown if a test_and_set operation on a scalaris ring
# fails because the old value did not match the expected value.
class KeyChangedException(Exception):
   def __init__(self, raw_result,  old_value):
       self.raw_result = raw_result
       self.old_value = old_value
   def __str__(self):
       return repr(self.raw_result) + ', old value: ' + repr(self.old_value)

# Exception that is thrown if a delete operation on a scalaris ring fails
# because no scalaris node was found.
class NodeNotFoundException(Exception):
   def __init__(self, raw_result):
       self.raw_result = raw_result
   def __str__(self):
       return repr(self.raw_result)

# Exception that is thrown if a read operation on a scalaris ring fails
# because the key did not exist before.
class NotFoundException(Exception):
   def __init__(self, raw_result):
       self.raw_result = raw_result
   def __str__(self):
       return repr(self.raw_result)

# Exception that is thrown if a read or write operation on a scalaris ring
#fails due to a timeout.
class TimeoutException(Exception):
   def __init__(self, raw_result):
       self.raw_result = raw_result
   def __str__(self):
       return repr(self.raw_result)

# Generic exception that is thrown during operations on a scalaris ring, e.g.
# if an unknown result has been returned.
class UnknownException(Exception):
   def __init__(self, raw_result):
       self.raw_result = raw_result
   def __str__(self):
       return repr(self.raw_result)

class TransactionSingleOp(object):
    def __init__(self, url = None,  conn = None,  timeout = default_timeout):
        if (url == None):
            self._uri = urlparse.urlparse(default_url)
        else:
            self._uri = urlparse.urlparse(url)
        
        if (conn == None):
            try:
                self._conn = httplib.HTTPConnection(self._uri.hostname,  self._uri.port,  timeout = timeout)
            except Exception,  (instance):
                raise ConnectionException(instance)
        else:
            self._conn = conn

    def read(self,  key):
        result = _json_call(self._conn,  self._uri, 'read', [key])
        # results: {'status': 'ok', 'value', xxx} or
        #          {'status': 'fail', 'reason': 'timeout' or 'not_found'}
        return _process_result(result,  'read')

    def write(self,  key, value):
        value = _json_encode_value(value)
        result = _json_call(self._conn,  self._uri, 'write', [key, value])
        # results: {'status': 'ok'} or
        #          {'status': 'fail', 'reason': 'timeout' or 'abort'}
        _process_result(result,  'commit')
    
    def testAndSet(self,  key, oldvalue, newvalue):
        oldvalue = _json_encode_value(oldvalue)
        newvalue = _json_encode_value(newvalue)
        result = _json_call(self._conn,  self._uri, 'test_and_set', [key, oldvalue, newvalue])
        # results: {'status': 'ok'} or
        #          {'status': 'fail', 'reason': 'timeout' or 'abort' or 'not_found'} or
        #          {'status': 'fail', 'reason': 'key_changed', 'value': xxx}
        _process_result(result,  'test_and_set')

    def nop(self,  value):
        value = _json_encode_value(value)
        _json_call(self._conn,  self._uri, 'nop', [value])
        # results: 'ok'
    
    def closeConnection(self):
        self._conn.close()

class Transaction(object):
    def __init__(self, url = None,  conn = None,  timeout = default_timeout):
        if (url == None):
            self._uri = urlparse.urlparse(default_url )
        else:
            self._uri = urlparse.urlparse(url)
        
        if (conn == None):
            try:
                self._conn = httplib.HTTPConnection(self._uri.hostname,  self._uri.port,  timeout = timeout)
            except Exception,  (instance):
                raise ConnectionException(instance)
        else:
            self._conn = conn
        
        self._tlog = None
    
    def req_list(self,  reqlist):
        if self._tlog == None:
            result = _json_call(self._conn,  self._uri, 'req_list', [reqlist.getJSONRequests()])
        else:
            result = _json_call(self._conn,  self._uri, 'req_list', [self._tlog, reqlist.getJSONRequests()])
        # results: {'tlog': xxx,
        #           'results': [{'status': 'ok'} or {'status': 'ok', 'value': xxx} or
        #                       {'status': 'fail', 'reason': 'timeout' or 'abort' or 'not_found}]}
        if 'tlog' not in result or 'results' not in result or not isinstance(result['results'],  list) or len(result['results']) < 1:
            raise UnknownException(result)
        self._tlog = result['tlog']
        return result['results']
    
    # result: {'status': 'ok'} or {'status': 'ok', 'value': xxx} or
    #         {'status': 'fail', 'reason': 'timeout' or 'abort' or 'not_found}
    # op: 'read' or 'write' or 'commit' or 'any'
    def process_result(self,  result,  op):
        return _process_result(result,  op)
    
    # results: {'status': 'ok'} or
    #          {'status': 'fail', 'reason': 'timeout' or 'abort'}
    def commit(self):
        result = self.req_list(ReqList().addCommit())[0]
        self.process_result(result,  'commit')
        # reset tlog (minor optimization which is not done in req_list):
        self._tlog = None
    
    # results: None
    def abort(self):
        self._tlog = None
    
    # results: {'status': 'ok', 'value', xxx} or
    #              {'status': 'fail', 'reason': 'timeout' or 'not_found'}
    def read(self,  key):
        result = self.req_list(ReqList().addRead(key))[0]
        return self.process_result(result,  'read')
    
    # results: {'status': 'ok'} or
    #              {'status': 'fail', 'reason': 'timeout' or 'abort'}
    def write(self,  key,  value):
        result = self.req_list(ReqList().addWrite(key,  value))[0]
        self.process_result(result,  'commit')

    def nop(self,  value):
        value = _json_encode_value(value)
        _json_call(self._conn,  self._uri, 'nop', [value])
        # results: 'ok'
    
    def closeConnection(self):
        self._conn.close()

class ReqList(object):
    def __init__(self):
        self.requests = []
    
    def addRead(self,  key):
        self.requests.append({'read': key})
        return self
    
    def addWrite(self,  key,  value):
        self.requests.append({'write': {key: _json_encode_value(value)}})
        return self
    
    def addCommit(self):
        self.requests.append({'commit': 'commit'})
        return self
    
    def getJSONRequests(self):
        return self.requests

class PubSub(object):
    def __init__(self, url = None,  conn = None,  timeout = default_timeout):
        if (url == None):
            self._uri = urlparse.urlparse(default_url)
        else:
            self._uri = urlparse.urlparse(url)
        
        if (conn == None):
            try:
                self._conn = httplib.HTTPConnection(self._uri.hostname,  self._uri.port,  timeout = timeout)
            except Exception,  (instance):
                raise ConnectionException(instance)
        else:
            self._conn = conn

    def publish(self,  topic,  content):
        content = _json_encode_value(content)
        result = _json_call(self._conn,  self._uri, 'publish', [topic,  content])
        # results: {'status': 'ok'} or
        #          {'status': 'fail', 'reason': 'timeout' or 'abort'}
        _process_result(result,  'commit')

    def subscribe(self,  topic,  url):
        url = _json_encode_value(url)
        result = _json_call(self._conn,  self._uri, 'subscribe', [topic,  url])
        # results: {'status': 'ok'} or
        #          {'status': 'fail', 'reason': 'timeout' or 'abort'}
        _process_result(result,  'commit')

    def unsubscribe(self,  topic,  url):
        url = _json_encode_value(url)
        result = _json_call(self._conn,  self._uri, 'unsubscribe', [topic,  url])
        # results: {'status': 'ok'} or
        #          {'status': 'fail', 'reason': 'timeout' or 'abort'}
        _process_result(result,  'commit')

    def getSubscribers(self,  topic):
        result = _json_call(self._conn,  self._uri, 'get_subscribers', [topic])
        # results: {'status': 'ok', 'value', xxx} or
        #          {'status': 'fail', 'reason': 'timeout' or 'not_found'}
        return _process_result(result,  'read')

    def nop(self,  value):
        value = _json_encode_value(value)
        _json_call(self._conn,  self._uri, 'nop', [value])
        # results: 'ok'
    
    def closeConnection(self):
        self._conn.close()
    
# if the expected value is a list, the returned value could by (mistakenly) a string if it is a list of integers
# -> provide a method for converting such a string to a list
def str_to_list(value):
    if (isinstance(value, str) or isinstance(value, unicode)):
        chars = list(value)
        return [ord(char) for char in chars]
    else:
        return value

if __name__ == "__main__":
    import sys
    sc = TransactionSingleOp()
    if (sys.argv[1] == "read"):
        key = sys.argv[2]
        try:
            value = sc.read(key)
            print 'read(' + key + ') = ' + rep(value)
        except ConnectionException,  (instance):
            print 'read(' + key + ') failed with connection error'
            sys.exit(1)
        except TimeoutException,  (instance):
            print 'read(' + key + ') failed with timeout'
            sys.exit(1)
        except NotFoundException,  (instance):
            print 'read(' + key + ') failed with not_found'
            sys.exit(1)
        except UnknownException,  (instance):
            print 'read(' + key + ') failed with unknown: ' + str(instance)
            sys.exit(1)
    elif (sys.argv[1] == "write"):
        key = sys.argv[2]
        value = sys.argv[3]
        try:
            sc.write(key,  value)
            print 'write(' + key + ', ' + value + '): ok'
        except ConnectionException,  (instance):
            print 'write(' + key + ', ' + value + ') failed with connection error'
            sys.exit(1)
        except TimeoutException,  (instance):
            print 'write(' + key + ', ' + value + ') failed with timeout'
            sys.exit(1)
        except AbortException,  (instance):
            print 'write(' + key + ', ' + value + ') failed with abort'
            sys.exit(1)
        except UnknownException,  (instance):
            print 'write(' + key + ', ' + value + ') failed with unknown: ' + str(instance)
            sys.exit(1)
