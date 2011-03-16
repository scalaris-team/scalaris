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

import json, httplib, urlparse
import sys
import base64
#import urllib

default_url = 'http://localhost:8000/jsonrpc.yaws'

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
    #conn.request("POST", uri.path, urllib.quote(params_json), headers)
    conn.request("POST", uri.path, params_json, headers)
    response = conn.getresponse()
    # TODO: handle connection error
    #print response.status, response.reason
    data = response.read()
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
    if value['type'] == 'as_bin':
        return bytearray(base64.b64decode(value['value']),  'base64')
    else:
        return value['value']

def _json_replace_value_in_result(result):
    if 'value' in result:
        result['value'] = _json_decode_value(result['value'])
    return result

class TransactionSingleOp(object):
    def __init__(self,  **parameters):
        if ('url' in parameters):
            self.uri = urlparse.urlparse(parameters['url'])
        else:
            self.uri = urlparse.urlparse(default_url )
        
        if ('conn' in parameters):
            self.conn = parameters['conn']
        else:
            self.conn = httplib.HTTPConnection(self.uri.hostname,  self.uri.port)
    
    # results: {'status': 'ok'} or
    #              {'status': 'fail', 'reason': 'timeout' or 'abort' or 'not_found'} or
    #              {'status': 'fail', 'reason': 'key_changed', 'value': xxx}
    def test_and_set(self,  key, oldvalue, newvalue):
        oldvalue = _json_encode_value(oldvalue)
        newvalue = _json_encode_value(newvalue)
        result = _json_call(self.conn,  self.uri, 'test_and_set', [key, oldvalue, newvalue])
        return _json_replace_value_in_result(result)

    # results: {'status': 'ok', 'value', xxx} or
    #              {'status': 'fail', 'reason': 'timeout' or 'not_found'}
    def read(self,  key):
        result = _json_call(self.conn,  self.uri, 'read', [key])
        return _json_replace_value_in_result(result)

    # results: {'status': 'ok'} or
    #              {'status': 'fail', 'reason': 'timeout' or 'abort'}
    def write(self,  key, value):
        value = _json_encode_value(value)
        return _json_call(self.conn,  self.uri, 'write', [key, value])

    # results: 'ok'
    def nop(self,  value):
        value = _json_encode_value(value)
        return _json_call(self.conn,  self.uri, 'nop', [value])
    
    def close_connection(self):
        self.conn.close()
    
    def __del__ (self):
        self.conn.close()

class Transaction(object):
    def __init__(self,  **parameters):
        if ('url' in parameters):
            self.uri = urlparse.urlparse(parameters['url'])
        else:
            self.uri = urlparse.urlparse(default_url )
        
        if ('conn' in parameters):
            self.conn = parameters['conn']
        else:
            self.conn = httplib.HTTPConnection(self.uri.hostname,  self.uri.port)
        
        self.tlog = None
    
    # results: [{'status': 'ok'} or {'status': 'ok', 'value': xxx} or
    #              {'status': 'fail', 'reason': 'timeout' or 'abort' or 'not_found}]
    def req_list(self,  reqlist):
        if self.tlog == None:
            result = _json_call(self.conn,  self.uri, 'req_list', [reqlist.getJSONRequests()])
        else:
            result = _json_call(self.conn,  self.uri, 'req_list', [self.tlog, reqlist.getJSONRequests()])
        self.tlog = result['tlog']
        resultlist = [_json_replace_value_in_result(res) for res in result['results']]
        return resultlist
    
    # results: {'status': 'ok'} or
    #              {'status': 'fail', 'reason': 'timeout' or 'abort'}
    def commit(self):
        result = self.req_list(ReqList().addCommit())[0]
        # reset tlog (minor optimization which is not done in req_list):
        self.tlog = None
        return result
    
    # results: None
    def abort(self):
        self.tlog = None
    
    # results: {'status': 'ok', 'value', xxx} or
    #              {'status': 'fail', 'reason': 'timeout' or 'not_found'}
    def read(self,  key):
        return self.req_list(ReqList().addRead(key))[0]
    
    # results: {'status': 'ok'} or
    #              {'status': 'fail', 'reason': 'timeout' or 'abort'}
    def write(self,  key,  value):
        return self.req_list(ReqList().addWrite(key,  value))[0]

    # results: 'ok'
    def nop(self,  value):
        value = _json_encode_value(value)
        return _json_call(self.conn,  self.uri, 'nop', [value])
    
    def __del__ (self):
        self.tlog = None
        self.conn.close()

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
        res = sc.read(key)
        if (res['status'] == 'ok'):
            print 'read(' + key + ') = ' + res['value']
        elif (res['status'] == 'fail'):
            print 'read(' + key + ') failed with ' + res['reason']
    elif (sys.argv[1] == "write"):
        key = sys.argv[2]
        value = sys.argv[3]
        res = sc.write(key,  value)
        if (res['status'] == 'ok'):
            print 'write(' + key + ', ' + value + ') = ' + res['status']
        elif (res['status'] == 'fail'):
            print 'write(' + key + ', ' + value + ') failed with ' + res['reason']
