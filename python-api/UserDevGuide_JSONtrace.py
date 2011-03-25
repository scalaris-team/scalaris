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
import Scalaris

def _json_call(conn, function, params):
    params = {'version': '1.1',
              'method': function,
              'params': params,
              'id': 0}
    #print params
    # use compact JSON encoding:
    params_json = json.dumps(params, separators=(',',':'))
    print ''
    print 'request:'
    print json.dumps(params, indent=1)
    # no need to quote - we already encode to json:
    headers = {"Content-type": "application/json"}
    try:
        #conn.request("POST", default_path, urllib.quote(params_json), headers)
        conn.request("POST", Scalaris.default_path, params_json, headers)
        response = conn.getresponse()
        #print response.status, response.reason
        if (response.status < 200 or response.status >= 300):
            raise ConnectionException(response)
        data = response.read()
    except socket.timeout as instance:
        raise ConnectionException(instance)
    print ''
    print 'response:'
    print json.dumps(json.loads(data), indent=1)
    response_json = json.loads(data)
    #print response_json
    return response_json['result']

class Transaction(Scalaris.Transaction):
    def __init__(self):
       Scalaris.Transaction.__init__(self)
    
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

if __name__ == "__main__":
    import sys
    sc1 = Transaction()
    sc1.req_list(Scalaris.ReqList().addWrite("keyA", "valueA").addWrite("keyB", "valueB").addCommit())
    sc1.closeConnection()
    sc2 = Transaction()
    sc2.req_list(Scalaris.ReqList().addRead("keyA").addRead("keyB"))
    sc2.req_list(Scalaris.ReqList().addWrite("keyA", "valueA2").addCommit())
