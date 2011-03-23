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

import json, httplib, urlparse, socket
import sys, os
import base64

if 'SCALARIS_JSON_URL' in os.environ:
    default_url = os.environ['SCALARIS_JSON_URL']
else:
    default_url = 'http://localhost:8000'
default_timeout = 5 # socket timeout in seconds
default_path = '/jsonrpc.yaws'

def _json_call(conn, function, params):
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
        #conn.request("POST", default_path, urllib.quote(params_json), headers)
        conn.request("POST", default_path, params_json, headers)
        response = conn.getresponse()
        #print response.status, response.reason
        if (response.status < 200 or response.status >= 300):
            raise ConnectionException(response)
        data = response.read()
    except socket.timeout as instance:
        raise ConnectionException(instance)
    #print data
    response_json = json.loads(data)
    #print response_json
    return response_json['result']

def _json_encode_value(value):
    if isinstance(value, bytearray):
        return {'type': 'as_bin', 'value': base64.b64encode(buffer(value))}
    else:
        return {'type': 'as_is', 'value': value}

def _json_decode_value(value):
    if ('type' not in value) or ('value' not in value):
        raise UnknownException(value)
    if value['type'] == 'as_bin':
        return bytearray(base64.b64decode(value['value']), 'base64')
    else:
        return value['value']

def getConnection(url, timeout = default_timeout):
    try:
        uri = urlparse.urlparse(url)
        return httplib.HTTPConnection(uri.hostname, uri.port, timeout = timeout)
    except Exception as instance:
        raise ConnectionException(instance)
    
# result: {'status': 'ok', 'value': xxx} or
#         {'status': 'fail', 'reason': 'timeout' or 'not_found'}
def _process_result_read(result):
    if isinstance(result, dict) and 'status' in result and len(result) == 2:
        if result['status'] == 'ok' and 'value' in result:
            return _json_decode_value(result['value'])
        elif result['status'] == 'fail' and 'reason' in result:
            if result['reason'] == 'timeout':
                raise TimeoutException(result)
            elif result['reason'] == 'not_found':
                raise NotFoundException(result)
    raise UnknownException(result)
    
# result: {'status': 'ok'} or
#         {'status': 'fail', 'reason': 'timeout'}
# op: 'read' or 'write' or 'commit', 'test_and_set'
def _process_result_write(result, op):
    if isinstance(result, dict):
        if result == {'status': 'ok'}:
            return None
        elif result == {'status': 'fail', 'reason': 'timeout'}:
            raise TimeoutException(result)
    raise UnknownException(result)
    
# result: {'status': 'ok'} or
#         {'status': 'fail', 'reason': 'timeout' or 'abort'}
def _process_result_commit(result):
    if isinstance(result, dict) and 'status' in result:
        if result == {'status': 'ok'}:
            return None
        elif result['status'] == 'fail' and 'reason' in result and len(result) == 2:
            if result['reason'] == 'timeout':
                raise TimeoutException(result)
            elif result['reason'] == 'abort':
                raise AbortException(result)
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
   def __init__(self, raw_result, old_value):
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
    def __init__(self, url = default_url, conn = None, timeout = default_timeout):
        if (conn == None):
            self._conn = getConnection(url, timeout)
        else:
            self._conn = conn

    def read(self, key):
        result = _json_call(self._conn, 'read', [key])
        # results: {'status': 'ok', 'value', xxx} or
        #          {'status': 'fail', 'reason': 'timeout' or 'not_found'}
        return _process_result_read(result)

    def write(self, key, value):
        value = _json_encode_value(value)
        result = _json_call(self._conn, 'write', [key, value])
        # results: {'status': 'ok'} or
        #          {'status': 'fail', 'reason': 'timeout' or 'abort'}
        _process_result_commit(result)
    
    def testAndSet(self, key, oldvalue, newvalue):
        oldvalue = _json_encode_value(oldvalue)
        newvalue = _json_encode_value(newvalue)
        result = _json_call(self._conn, 'test_and_set', [key, oldvalue, newvalue])
        # results: {'status': 'ok'} or
        #          {'status': 'fail', 'reason': 'timeout' or 'abort' or 'not_found'} or
        #          {'status': 'fail', 'reason': 'key_changed', 'value': xxx}
        if isinstance(result, dict) and 'status' in result:
            if result['status'] == 'ok' and len(result) == 1:
                return None
            elif result['status'] == 'fail' and 'reason' in result:
                if len(result) == 2:
                    if result['reason'] == 'timeout':
                        raise TimeoutException(result)
                    elif result['reason'] == 'abort':
                        raise AbortException(result)
                    elif result['reason'] == 'not_found':
                        raise NotFoundException(result)
                elif result['reason'] == 'key_changed' and 'value' in result and len(result) == 3:
                    raise KeyChangedException(result, _json_decode_value(result['value']))
        raise UnknownException(result)

    def nop(self, value):
        value = _json_encode_value(value)
        _json_call(self._conn, 'nop', [value])
        # results: 'ok'
    
    def closeConnection(self):
        self._conn.close()

class Transaction(object):
    def __init__(self, url = default_url, conn = None, timeout = default_timeout):
        if (conn == None):
            self._conn = getConnection(url, timeout)
        else:
            self._conn = conn
        
        self._tlog = None
    
    # returns: [{'status': 'ok'} or {'status': 'ok', 'value': xxx} or
    #           {'status': 'fail', 'reason': 'timeout' or 'abort' or 'not_found}]
    # (the elements of this list can be processed with Transaction.process_result_*(result))
    def req_list(self, reqlist):
        if self._tlog == None:
            result = _json_call(self._conn, 'req_list', [reqlist.getJSONRequests()])
        else:
            result = _json_call(self._conn, 'req_list', [self._tlog, reqlist.getJSONRequests()])
        # results: {'tlog': xxx,
        #           'results': [{'status': 'ok'} or {'status': 'ok', 'value': xxx} or
        #                       {'status': 'fail', 'reason': 'timeout' or 'abort' or 'not_found}]}
        if 'tlog' not in result or 'results' not in result or not isinstance(result['results'], list) or len(result['results']) < 1:
            raise UnknownException(result)
        self._tlog = result['tlog']
        return result['results']
    
    def process_result_read(self, result):
        return _process_result_read(result)

    def process_result_write(self, result):
        return _process_result_write(result)

    def process_result_commit(self, result):
        return _process_result_commit(result)
    
    # results: {'status': 'ok'} or
    #          {'status': 'fail', 'reason': 'timeout' or 'abort'}
    def commit(self):
        result = self.req_list(ReqList().addCommit())[0]
        self.process_result_commit(result)
        # reset tlog (minor optimization which is not done in req_list):
        self._tlog = None
    
    # results: None
    def abort(self):
        self._tlog = None
    
    # results: {'status': 'ok', 'value', xxx} or
    #              {'status': 'fail', 'reason': 'timeout' or 'not_found'}
    def read(self, key):
        result = self.req_list(ReqList().addRead(key))[0]
        return self.process_result_read(result)
    
    # results: {'status': 'ok'} or
    #              {'status': 'fail', 'reason': 'timeout' or 'abort'}
    def write(self, key, value):
        result = self.req_list(ReqList().addWrite(key, value))[0]
        self.process_result_commit(result)

    def nop(self, value):
        value = _json_encode_value(value)
        _json_call(self._conn, 'nop', [value])
        # results: 'ok'
    
    def closeConnection(self):
        self._conn.close()

class ReqList(object):
    def __init__(self):
        self.requests = []
    
    def addRead(self, key):
        self.requests.append({'read': key})
        return self
    
    def addWrite(self, key, value):
        self.requests.append({'write': {key: _json_encode_value(value)}})
        return self
    
    def addCommit(self):
        self.requests.append({'commit': 'commit'})
        return self
    
    def getJSONRequests(self):
        return self.requests

class PubSub(object):
    def __init__(self, url = default_url, conn = None, timeout = default_timeout):
        if (conn == None):
            self._conn = getConnection(url, timeout)
        else:
            self._conn = conn

    def publish(self, topic, content):
        # note: do NOT encode the content, this is not decoded on the erlang side!
        # (only strings are allowed anyway)
        # content = _json_encode_value(content)
        result = _json_call(self._conn, 'publish', [topic, content])
        # results: {'status': 'ok'}
        if result == {'status': 'ok'}:
            return None
        raise UnknownException(result)

    def subscribe(self, topic, url):
        # note: do NOT encode the URL, this is not decoded on the erlang side!
        # (only strings are allowed anyway)
        # url = _json_encode_value(url)
        result = _json_call(self._conn, 'subscribe', [topic, url])
        # results: {'status': 'ok'} or
        #          {'status': 'fail', 'reason': 'timeout' or 'abort'}
        _process_result_commit(result)

    def unsubscribe(self, topic, url):
        # note: do NOT encode the URL, this is not decoded on the erlang side!
        # (only strings are allowed anyway)
        # url = _json_encode_value(url)
        result = _json_call(self._conn, 'unsubscribe', [topic, url])
        # results: {'status': 'ok'} or
        #          {'status': 'fail', 'reason': 'timeout' or 'abort' or 'not_found'}
        if isinstance(result, dict) and 'status' in result:
            if result == {'status': 'ok'}:
                return None
            elif result['status'] == 'fail' and 'reason' in result and len(result) == 2:
                if result['reason'] == 'timeout':
                    raise TimeoutException(result)
                elif result['reason'] == 'abort':
                    raise AbortException(result)
                elif result['reason'] == 'not_found':
                    raise NotFoundException(result)
        raise UnknownException(result)

    def getSubscribers(self, topic):
        result = _json_call(self._conn, 'get_subscribers', [topic])
        # results: [urls=str()]
        if isinstance(result, list):
            return result
        else:
            raise UnknownException(result)

    def nop(self, value):
        value = _json_encode_value(value)
        _json_call(self._conn, 'nop', [value])
        # results: 'ok'
    
    def closeConnection(self):
        self._conn.close()

class ReplicatedDHT(object):
    def __init__(self, url = default_url, conn = None, timeout = default_timeout):
        if (conn == None):
            self._conn = getConnection(url, timeout)
        else:
            self._conn = conn

    # returns: (Success::boolean(), ok::integer(), results:[ok | locks_set | undef])
    def delete(self, key, timeout = 2000):
        result = _json_call(self._conn, 'delete', [key, timeout])
        # results: {'ok': xxx, 'results': [ok | locks_set | undef]} or
        #          {'failure': 'timeout', 'ok': xxx, 'results': [ok | locks_set | undef]}
        if isinstance(result, dict) and 'ok' in result and 'results' in result:
            if 'failure' not in result:
                return (True, result['ok'], result['results'])
            elif result['failure'] == 'timeout':
                return (False, result['ok'], result['results'])
        raise UnknownException(result)

    def nop(self, value):
        value = _json_encode_value(value)
        _json_call(self._conn, 'nop', [value])
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
    if (len(sys.argv) == 3 and sys.argv[1] in ["--read", "-r"]):
        sc = TransactionSingleOp()
        key = sys.argv[2]
        try:
            value = sc.read(key)
            print 'read(' + key + ') = ' + repr(value)
        except ConnectionException as instance:
            print 'read(' + key + ') failed with connection error'
            sys.exit(1)
        except TimeoutException as instance:
            print 'read(' + key + ') failed with timeout'
            sys.exit(1)
        except NotFoundException as instance:
            print 'read(' + key + ') failed with not_found'
            sys.exit(1)
        except UnknownException as instance:
            print 'read(' + key + ') failed with unknown: ' + str(instance)
            sys.exit(1)
    elif (len(sys.argv) == 4 and sys.argv[1] in ["--write", "-w"]):
        sc = TransactionSingleOp()
        key = sys.argv[2]
        value = sys.argv[3]
        try:
            sc.write(key, value)
            print 'write(' + key + ', ' + value + '): ok'
        except ConnectionException as instance:
            print 'write(' + key + ', ' + value + ') failed with connection error'
            sys.exit(1)
        except TimeoutException as instance:
            print 'write(' + key + ', ' + value + ') failed with timeout'
            sys.exit(1)
        except AbortException as instance:
            print 'write(' + key + ', ' + value + ') failed with abort'
            sys.exit(1)
        except UnknownException as instance:
            print 'write(' + key + ', ' + value + ') failed with unknown: ' + str(instance)
            sys.exit(1)
    elif (len(sys.argv) == 3 and sys.argv[1] in ["--delete", "-d"]):
        sc = ReplicatedDHT()
        key = sys.argv[2]
        if len(sys.argv) >= 4:
            timeout = sys.argv[3]
        else:
            timeout = 2000
        
        try:
            (success, ok, results) = sc.delete(key)
            if (success):
                print 'delete(' + key + ', ' + str(timeout) + '): ok, deleted: ' + str(ok) + ' (' + repr(results) + ')'
            else:
                print 'delete(' + key + ', ' + str(timeout) + '): failed, deleted: ' + str(ok) + ' (' + repr(results) + ')'
        except UnknownException as instance:
            print 'delete(' + key + ') failed with unknown: ' + str(instance)
            sys.exit(1)
    elif (len(sys.argv) == 4 and sys.argv[1] in ["--publish", "-p"]):
        sc = PubSub()
        topic = sys.argv[2]
        content = sys.argv[3]
        try:
            sc.publish(topic, content)
            print 'publish(' + topic + ', ' + content + '): ok'
        except ConnectionException as instance:
            print 'publish(' + topic + ', ' + content + ') failed with connection error'
            sys.exit(1)
        except UnknownException as instance:
            print 'publish(' + topic + ', ' + content + ') failed with unknown: ' + str(instance)
            sys.exit(1)
    elif (len(sys.argv) == 4 and sys.argv[1] in ["--subscribe", "-s"]):
        sc = PubSub()
        topic = sys.argv[2]
        url = sys.argv[3]
        try:
            sc.subscribe(topic, url)
            print 'subscribe(' + topic + ', ' + url + '): ok'
        except ConnectionException as instance:
            print 'subscribe(' + topic + ', ' + url + ') failed with connection error'
            sys.exit(1)
        except TimeoutException as instance:
            print 'subscribe(' + topic + ', ' + url + ') failed with timeout'
            sys.exit(1)
        except AbortException as instance:
            print 'subscribe(' + topic + ', ' + url + ') failed with abort'
            sys.exit(1)
        except UnknownException as instance:
            print 'subscribe(' + topic + ', ' + url + ') failed with unknown: ' + str(instance)
            sys.exit(1)
    elif (len(sys.argv) == 4 and sys.argv[1] in ["--unsubscribe", "-u"]):
        sc = PubSub()
        topic = sys.argv[2]
        url = sys.argv[3]
        try:
            sc.unsubscribe(topic, url)
            print 'unsubscribe(' + topic + ', ' + url + '): ok'
        except ConnectionException as instance:
            print 'unsubscribe(' + topic + ', ' + url + ') failed with connection error'
            sys.exit(1)
        except TimeoutException as instance:
            print 'unsubscribe(' + topic + ', ' + url + ') failed with timeout'
            sys.exit(1)
        except NotFoundException as instance:
            print 'unsubscribe(' + topic + ', ' + url + ') failed with not found'
            sys.exit(1)
        except AbortException as instance:
            print 'unsubscribe(' + topic + ', ' + url + ') failed with abort'
            sys.exit(1)
        except UnknownException as instance:
            print 'unsubscribe(' + topic + ', ' + url + ') failed with unknown: ' + str(instance)
            sys.exit(1)
    elif (len(sys.argv) == 3 and sys.argv[1] in ["--getsubscribers", "-g"]):
        sc = PubSub()
        topic = sys.argv[2]
        try:
            value = sc.getSubscribers(topic)
            print 'getSubscribers(' + topic + ') = ' + repr(value)
        except ConnectionException as instance:
            print 'getSubscribers(' + topic + ') failed with connection error'
            sys.exit(1)
        except UnknownException as instance:
            print 'getSubscribers(' + topic + ') failed with unknown: ' + str(instance)
            sys.exit(1)
    else:
        print 'usage: Scalaris.py [Options]'
        print ' -r,--read <key>                      read an item'
        print ' -w,--write <key> <value>             write an item'
        print ' -d,--delete <key> [<timeout>]        delete an item (default timeout:'
        print '                                      2000ms)'
        print '                                      WARNING: This function can lead to'
        print '                                      inconsistent data (e.g. deleted'
        print '                                      items can re-appear). Also when'
        print '                                      re-creating an item the version'
        print '                                      before the delete can re-appear.'
        print ' -p,--publish <topic> <message>       publish a new message for the given'
        print ' -s,--subscribe <topic> <url>         subscribe to a topic'
        print ' -g,--getsubscribers <topic>          get subscribers of a topic'
        print ' -u,--unsubscribe <topic> <url>       unsubscribe from a topic'
        print ' -h,--help                            print this message'
        print '                                      topic'
