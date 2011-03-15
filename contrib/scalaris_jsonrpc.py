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
#import urllib

url = 'http://localhost:8000/jsonrpc.yaws'

def json_call(function, params):
    uri = urlparse.urlparse(url)
    params = {'version': '1.1',
              'method': function,
              'params': params,
              'id': 0}
    #print params
    # use compact JSON encoding:
    params_json = json.dumps(params, separators=(',',':'))
    #print params_json
    headers = {"Content-type": "application/json"}
    conn = httplib.HTTPConnection(uri.hostname,  uri.port)
    # no need to quote - we already encode to json:
    #conn.request("POST", uri.path, urllib.quote(params_json), headers)
    conn.request("POST", uri.path, params_json, headers)
    response = conn.getresponse()
    #print response.status, response.reason
    data = response.read()
    #print data
    response_json = json.loads(data)
    #print response_json
    conn.close()
    return response_json['result']

def test_and_set(key, oldvalue, newvalue):
  return json_call('test_and_set', [key, oldvalue, newvalue])

def read(key):
  return json_call('read', [key])

def write(key, value):
  return json_call('write', [key, value])

def nop(value):
  return json_call('nop', [value])

# if the expected value is a list, the returned value could by (mistakenly) a string if it is a list of integers
# -> provide a method for converting such a string to a list
def str_to_list(value):
    if (type(value).__name__=='str' or type(value).__name__=='unicode'):
        chars = list(value)
        return [ord(char) for char in chars]
#    elif (type(value).__name__=='list'):
#        return value
    else:
        return value

if __name__ == "__main__":
    import sys
    if (sys.argv[1] == "read"):
        key = sys.argv[2]
        res = read(key)
        if (res['status'] == 'ok'):
            print 'read(' + key + ') = ' + res['value']
        elif (res['status'] == 'fail'):
            print 'read(' + key + ') failed with ' + res['reason']
    elif (sys.argv[1] == "write"):
        key = sys.argv[2]
        value = sys.argv[3]
        res = write(key,  value)
        if (res['status'] == 'ok'):
            print 'write(' + key + ', ' + value + ') = ' + res['status']
        elif (res['status'] == 'fail'):
            print 'write(' + key + ', ' + value + ') failed with ' + res['reason']
