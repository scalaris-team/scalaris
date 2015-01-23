# Copyright 2011-2015 Zuse Institute Berlin
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

import json, socket
import scalaris

class ConnectionWithTrace(scalaris.JSONConnection):
    def __init__(self, url = scalaris.DEFAULT_URL, timeout = socket.getdefaulttimeout()):
        scalaris.JSONConnection.__init__(self, url = scalaris.DEFAULT_URL, timeout = timeout)

    def call(self, function, params):
        params = {'jsonrpc': '2.0',
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
            self._conn.request("POST", scalaris.DEFAULT_PATH, params_json, headers)
            response = self._conn.getresponse()
            if (response.status < 200 or response.status >= 300):
                raise scalaris.ConnectionError(response)
            data = response.read()
        except Exception as instance:
            raise scalaris.ConnectionError(instance)
        print ''
        print 'response:'
        print json.dumps(json.loads(data), indent=1)
        response_json = json.loads(data)
        return response_json['result']

if __name__ == "__main__":
    sc1 = scalaris.Transaction(conn = ConnectionWithTrace())
    sc1.req_list(sc1.new_req_list().add_write("keyA", "valueA").add_write("keyB", "valueB").add_commit())
    sc1.close_connection()
    sc2 = scalaris.Transaction(conn = ConnectionWithTrace())
    sc2.req_list(sc2.new_req_list().add_read("keyA").add_read("keyB"))
    sc2.req_list(sc2.new_req_list().add_write("keyA", "valueA2").add_commit())
