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

class ConnectionWithTrace(Scalaris.JSONConnection):
    def __init__(self, url = Scalaris.default_url, timeout = Scalaris.default_timeout):
        Scalaris.JSONConnection.__init__(self, url = Scalaris.default_url, timeout = Scalaris.default_timeout)

    def call(self, function, params):
        params = {'version': '1.1',
                  'method': function,
                  'params': params,
                  'id': 0}
        # use compact JSON encoding:
        params_json = json.dumps(params, separators=(',',':'))
        print ''
        print 'request:'
        print json.dumps(params, indent=1)
        headers = {"Content-type": "application/json"}
        try:
            self._conn.request("POST", Scalaris.default_path, params_json, headers)
            response = self._conn.getresponse()
            if (response.status < 200 or response.status >= 300):
                raise ConnectionException(response)
            data = response.read()
        except socket.timeout as instance:
            raise ConnectionException(instance)
        print ''
        print 'response:'
        print json.dumps(json.loads(data), indent=1)
        response_json = json.loads(data)
        return response_json['result']

if __name__ == "__main__":
    import sys
    sc1 = Scalaris.Transaction(conn = ConnectionWithTrace())
    sc1.req_list(sc1.newReqList().addWrite("keyA", "valueA").addWrite("keyB", "valueB").addCommit())
    sc1.closeConnection()
    sc2 = Scalaris.Transaction(conn = ConnectionWithTrace())
    sc2.req_list(sc2.newReqList().addRead("keyA").addRead("keyB"))
    sc2.req_list(sc2.newReqList().addWrite("keyA", "valueA2").addCommit())
