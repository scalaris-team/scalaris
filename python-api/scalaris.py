# Copyright 2011-2014 Zuse Institute Berlin
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

import httplib, urlparse, base64, urllib
import os, threading, numbers, socket
from datetime import datetime, timedelta
try: import simplejson as json
except ImportError: import json

if 'SCALARIS_JSON_URL' in os.environ and os.environ['SCALARIS_JSON_URL'] != '':
    DEFAULT_URL = os.environ['SCALARIS_JSON_URL']
else:
    DEFAULT_URL = 'http://localhost:8000'
"""default URL and port to a scalaris node"""
DEFAULT_PATH = '/jsonrpc.yaws'
"""path to the json rpc page"""

class JSONConnection(object):
    """
    Abstracts connections to scalaris using JSON
    """

    def __init__(self, url = DEFAULT_URL, timeout = socket.getdefaulttimeout()):
        """
        Creates a JSON connection to the given URL using the given TCP timeout
        """
        try:
            uri = urlparse.urlparse(url)
            self._conn = httplib.HTTPConnection(uri.hostname, uri.port,
                                                timeout = timeout)
        except Exception as instance:
            raise ConnectionError(instance)

    def callp(self, path, function, params, retry_if_bad_status = True):
        return self.call(function, params, path = path, retry_if_bad_status = retry_if_bad_status)

    def call(self, function, params, path = DEFAULT_PATH, retry_if_bad_status = True):
        """
        Calls the given function with the given parameters via the JSON
        interface of scalaris.
        """
        params2 = {'jsonrpc': '2.0',
                  'method': function,
                  'params': params,
                  'id': 0}
        try:
            data = None
            response = None
            # use compact JSON encoding:
            params_json = json.dumps(params2, separators=(',',':'))
            headers = {"Content-type": "application/json; charset=utf-8"}
            # note: we need to quote, e.g. if a '%' is in the value string:
            self._conn.request("POST", path, urllib.quote(params_json), headers)
            response = self._conn.getresponse()
            #print response.status, response.reason
            data = response.read().decode('utf-8')
            if (response.status < 200 or response.status >= 300):
                raise ConnectionError(data, response = response)
            response_json = json.loads(data)
            return response_json['result']
        except httplib.BadStatusLine as instance:
            #print 'HTTP STATUS:', response.status, response.reason, params_json
            self.close()
            if retry_if_bad_status:
                return self.call(function, params, path = path, retry_if_bad_status = False)
            else:
                raise ConnectionError(data, response = response, error = instance)
        except ConnectionError:
            #print 'HTTP STATUS:', response.status, response.reason, params_json
            self.close()
            raise
        except Exception as instance:
            #print 'HTTP STATUS:', response.status, response.reason, params_json
            self.close()
            raise ConnectionError(data, response = response, error = instance)

    @staticmethod
    def encode_value(value):
        """
        Encodes the value to the form required by the scalaris JSON API
        """
        if isinstance(value, bytearray):
            return {'type': 'as_bin', 'value': (base64.b64encode(bytes(value))).decode('ascii')}
        else:
            return {'type': 'as_is', 'value': value}

    @staticmethod
    def decode_value(value):
        """
        Decodes the value from the scalaris JSON API form to a native type
        """
        if ('type' not in value) or ('value' not in value):
            raise UnknownError(value)
        if value['type'] == 'as_bin':
            return bytearray(base64.b64decode(value['value'].encode('ascii')))
        else:
            return value['value']

    # result: {'status': 'ok'} or
    #         {'status': 'fail', 'reason': 'timeout'}
    @staticmethod
    def check_fail_abort(result):
        """
        Processes the result of some Scalaris operation and raises a
        TimeoutError if found.
        """
        if result == {'status': 'fail', 'reason': 'timeout'}:
            raise TimeoutError(result)

    # result: {'status': 'ok', 'value': xxx} or
    #         {'status': 'fail', 'reason': 'timeout' or 'not_found'}
    @staticmethod
    def process_result_read(result):
        """
        Processes the result of a read operation.
        Returns the read value on success.
        Raises the appropriate exception if the operation failed.
        """
        if isinstance(result, dict) and 'status' in result and len(result) == 2:
            if result['status'] == 'ok' and 'value' in result:
                return JSONConnection.decode_value(result['value'])
            elif result['status'] == 'fail' and 'reason' in result:
                if result['reason'] == 'timeout':
                    raise TimeoutError(result)
                elif result['reason'] == 'not_found':
                    raise NotFoundError(result)
        raise UnknownError(result)

    # result: {'status': 'ok'} or
    #         {'status': 'fail', 'reason': 'timeout'}
    @staticmethod
    def process_result_write(result):
        """
        Processes the result of a write operation.
        Raises the appropriate exception if the operation failed.
        """
        if isinstance(result, dict):
            if result == {'status': 'ok'}:
                return None
            elif result == {'status': 'fail', 'reason': 'timeout'}:
                raise TimeoutError(result)
        raise UnknownError(result)

    # result: {'status': 'ok'} or
    #         {'status': 'fail', 'reason': 'abort', 'keys': <list>} or
    #         {'status': 'fail', 'reason': 'timeout'}
    @staticmethod
    def process_result_commit(result):
        """
        Processes the result of a commit operation.
        Raises the appropriate exception if the operation failed.
        """
        if isinstance(result, dict) and 'status' in result:
            if result == {'status': 'ok'}:
                return None
            elif result['status'] == 'fail' and 'reason' in result:
                if len(result) == 2 and result['reason'] == 'timeout':
                    raise TimeoutError(result)
                elif len(result) == 3 and result['reason'] == 'abort' and 'keys' in result:
                    raise AbortError(result, result['keys'])
        raise UnknownError(result)

    # results: {'status': 'ok'} or
    #          {'status': 'fail', 'reason': 'timeout' or 'not_a_list'} or
    @staticmethod
    def process_result_add_del_on_list(result):
        """
        Processes the result of a add_del_on_list operation.
        Raises the appropriate exception if the operation failed.
        """
        if isinstance(result, dict) and 'status' in result:
            if result == {'status': 'ok'}:
                return None
            elif result['status'] == 'fail' and 'reason' in result:
                if len(result) == 2:
                    if result['reason'] == 'timeout':
                        raise TimeoutError(result)
                    elif result['reason'] == 'not_a_list':
                        raise NotAListError(result)
        raise UnknownError(result)

    # results: {'status': 'ok'} or
    #          {'status': 'fail', 'reason': 'timeout' or 'not_a_number'} or
    @staticmethod
    def process_result_add_on_nr(result):
        """
        Processes the result of a add_on_nr operation.
        Raises the appropriate exception if the operation failed.
        """
        if isinstance(result, dict) and 'status' in result:
            if result == {'status': 'ok'}:
                return None
            elif result['status'] == 'fail' and 'reason' in result:
                if len(result) == 2:
                    if result['reason'] == 'timeout':
                        raise TimeoutError(result)
                    elif result['reason'] == 'not_a_number':
                        raise NotANumberError(result)
        raise UnknownError(result)

    # results: {'status': 'ok'} or
    #          {'status': 'fail', 'reason': 'timeout' or 'not_found'} or
    #          {'status': 'fail', 'reason': 'key_changed', 'value': xxx}
    @staticmethod
    def process_result_test_and_set(result):
        """
        Processes the result of a test_and_set operation.
        Raises the appropriate exception if the operation failed.
        """
        if isinstance(result, dict) and 'status' in result:
            if result == {'status': 'ok'}:
                return None
            elif result['status'] == 'fail' and 'reason' in result:
                if len(result) == 2:
                    if result['reason'] == 'timeout':
                        raise TimeoutError(result)
                    elif result['reason'] == 'not_found':
                        raise NotFoundError(result)
                elif result['reason'] == 'key_changed' and 'value' in result and len(result) == 3:
                    raise KeyChangedError(result, JSONConnection.decode_value(result['value']))
        raise UnknownError(result)

    # results: {'ok': xxx, 'results': ['ok' or 'locks_set' or 'undef']} or
    #          {'failure': 'timeout', 'ok': xxx, 'results': ['ok' or 'locks_set' or 'undef']}
    @staticmethod
    def process_result_delete(result):
        """
        Processes the result of a delete operation.
        Returns the tuple
        (<success (True | 'timeout')>, <number of deleted items>, <detailed results>) on success.
        Raises the appropriate exception if the operation failed.
        """
        if isinstance(result, dict) and 'ok' in result and 'results' in result:
            if 'failure' not in result:
                return (True, result['ok'], result['results'])
            elif result['failure'] == 'timeout':
                return ('timeout', result['ok'], result['results'])
        raise UnknownError(result)

    # results: ['ok' or 'locks_set' or 'undef']
    @staticmethod
    def create_delete_result(result):
        """
        Creates a new DeleteResult from the given result list.
        """
        ok = 0
        locks_set = 0
        undefined = 0
        if isinstance(result, list):
            for element in result:
                if element == 'ok':
                    ok += 1
                elif element == 'locks_set':
                    locks_set += 1
                elif element == 'undef':
                    undefined += 1
                else:
                    raise UnknownError('Unknown reason ' + element + 'in ' + result)
            return DeleteResult(ok, locks_set, undefined)
        raise UnknownError('Unknown result ' + result)

    # results: {'tlog': xxx,
    #           'results': [{'status': 'ok'} or {'status': 'ok', 'value': xxx} or
    #                       {'status': 'fail', 'reason': 'timeout' or 'abort' or 'not_found'}]}
    @staticmethod
    def process_result_req_list_t(result):
        """
        Processes the result of a req_list operation of the Transaction class.
        Returns the tuple (<tlog>, <result>) on success.
        Raises the appropriate exception if the operation failed.
        """
        if 'tlog' not in result or 'results' not in result or \
            not isinstance(result['results'], list):
            raise UnknownError(result)
        return (result['tlog'], result['results'])

    # results: [{'status': 'ok'} or {'status': 'ok', 'value': xxx} or
    #           {'status': 'fail', 'reason': 'timeout' or 'abort' or 'not_found'}]
    @staticmethod
    def process_result_req_list_tso(result):
        """
        Processes the result of a req_list operation of the TransactionSingleOp class.
        Returns <result> on success.
        Raises the appropriate exception if the operation failed.
        """
        if not isinstance(result, list):
            raise UnknownError(result)
        return result

    # results: {'status': 'ok', 'value': xxx}
    @staticmethod
    def process_result_vm_get_version(result):
        """
        Processes the result of a api_vm/get_version operation.
        Raises the appropriate exception if the operation failed.
        """
        if isinstance(result, dict) and 'status' in result and 'value' in result:
            if result['status'] == 'ok':
                return result['value']
        raise UnknownError(result)

    # value: {'scalaris_version': xxx,
    #         'erlang_version': xxx,
    #         'mem_total': xxx,
    #         'uptime': xxx,
    #         'erlang_node': xxx,
    #         'ip': xxx,
    #         'port': xxx,
    #         'yaws_port': xxx}
    # results: {'status': 'ok', 'value': <value>
    @staticmethod
    def process_result_vm_get_info(result):
        """
        Processes the result of a api_vm/get_info operation.
        Raises the appropriate exception if the operation failed.
        """
        if isinstance(result, dict) and 'status' in result and 'value' in result:
            value = result['value']
            if result['status'] == 'ok' and \
               'scalaris_version' in value and 'erlang_version' in value and \
               'mem_total' in value and 'uptime' in value and \
               'erlang_node' in value and 'ip' in value and \
               'port' in value and 'yaws_port' in value:
                try:
                    return ScalarisVM.GetInfoResult(value['scalaris_version'],
                                                    value['erlang_version'],
                                                    int(value['mem_total']),
                                                    int(value['uptime']),
                                                    value['erlang_node'],
                                                    value['ip'],
                                                    int(value['port']),
                                                    int(value['yaws_port']))
                except:
                    pass
        raise UnknownError(result)

    # results: {'status': 'ok', 'value': xxx}
    @staticmethod
    def process_result_vm_get_number_of_nodes(result):
        """
        Processes the result of a api_vm/number_of_nodes operation.
        Raises the appropriate exception if the operation failed.
        """
        if isinstance(result, dict) and 'status' in result and 'value' in result:
            if result['status'] == 'ok':
                try:
                    return int(result['value'])
                except:
                    pass
        raise UnknownError(result)

    # results: {'status': 'ok', 'value': [xxx]}
    @staticmethod
    def process_result_vm_get_nodes(result):
        """
        Processes the result of a api_vm/get_nodes operation.
        Raises the appropriate exception if the operation failed.
        """
        if isinstance(result, dict) and 'status' in result and 'value' in result:
            if result['status'] == 'ok' and isinstance(result['value'], list):
                return result['value']
        raise UnknownError(result)

    # results: {'status': 'ok', 'ok': [xxx], 'failed': [xxx]}
    @staticmethod
    def process_result_vm_add_nodes(result):
        """
        Processes the result of a api_vm/add_nodes operation.
        Raises the appropriate exception if the operation failed.
        """
        if isinstance(result, dict) and 'status' in result and 'ok' in result and 'failed' in result:
            if result['status'] == 'ok' and isinstance(result['ok'], list) and isinstance(result['failed'], list):
                return (result['ok'], result['failed'])
        raise UnknownError(result)

    # results: {'status': 'ok' | 'not_found'}
    @staticmethod
    def process_result_vm_delete_node(result):
        """
        Processes the result of a api_vm/shutdown_node and api_vm/kill_node operations.
        Raises the appropriate exception if the operation failed.
        """
        if result == {'status': 'ok'}:
            return True
        if result == {'status': 'not_found'}:
            return False
        raise UnknownError(result)

    # results: {'status': 'ok', 'ok': [xxx]}
    @staticmethod
    def process_result_vm_delete_nodes(result):
        """
        Processes the result of a api_vm/shutdown_nodes and api_vm/kill_nodes operations.
        Raises the appropriate exception if the operation failed.
        """
        if isinstance(result, dict) and 'status' in result and 'ok' in result and \
           result['status'] == 'ok' and isinstance(result['ok'], list):
            return result['ok']
        raise UnknownError(result)

    # results: {'status': 'ok', 'ok': [xxx], 'not_found': [xxx]}
    @staticmethod
    def process_result_vm_delete_nodes_by_name(result):
        """
        Processes the result of a api_vm/shutdown_nodes_by_name and api_vm/kill_nodes_by_name operations.
        Raises the appropriate exception if the operation failed.
        """
        if isinstance(result, dict) and 'status' in result and 'ok' in result and 'not_found' in result:
            if result['status'] == 'ok' and isinstance(result['ok'], list) and isinstance(result['not_found'], list):
                return (result['ok'], result['not_found'])
        raise UnknownError(result)

    # results: {'status': 'ok'}
    @staticmethod
    def process_result_vm_delete_vm(result):
        """
        Processes the result of a api_vm/shutdown_vm and api_vm/kill_vm operations.
        Raises the appropriate exception if the operation failed.
        """
        if result == {'status': 'ok'}:
            return None
        raise UnknownError(result)

    # VM: {'erlang_node': xxx,
    #      'ip': xxx,
    #      'port': xxx,
    #      'yaws_port': xxx}
    # results: {'status': 'ok', 'value': [<VM>]
    @staticmethod
    def process_result_vm_get_other_vms(result):
        """
        Processes the result of a api_vm/get_other_vms operation.
        Raises the appropriate exception if the operation failed.
        """
        if isinstance(result, dict) and 'status' in result and 'value' in result:
            value = result['value']
            if result['status'] == 'ok' and isinstance(value, list):
                vms = []
                try:
                    for vm in value:
                        if 'erlang_node' in vm and 'ip' in vm and \
                           'port' in vm and 'yaws_port' in vm:
                            vms.append('http://' + vm['ip'] + ':' + str(int(vm['yaws_port'])))
                        else:
                            raise UnknownError(result)
                    return vms
                except:
                    pass
        raise UnknownError(result)

    # results: {'status': 'ok' | 'error'}
    @staticmethod
    def process_result_autoscale_check_config(result):
        if isinstance(result, dict) and 'status' in result:
            if result['status'] == 'ok':
                return True
            else:
                return False
        raise UnknownError(result)

    # <reason>: 'resp_timeout' | 'autoscale_false'
    # results: {'status': 'ok', 'value' : <number>} or
    #          {'status': 'error', 'reason': <reason>}
    @staticmethod
    def process_result_autoscale_pull_scale_req(result):
        if isinstance(result, dict) and 'status' in result:
            if result['status'] == 'ok' and 'value' in result and isinstance(result['value'], int):
                return result['value']

            if result['status'] == 'error' and 'reason' in result:
                if result['reason'] == 'resp_timeout':
                    raise TimeoutError(result)
                elif result['reason'] == 'autoscale_false':
                    raise ConfigError(result)
        raise UnknownError(result)

    # <reason>: 'locked' | 'resp_timeout' | 'autoscale_false'
    # results: {'status' : 'ok'} or
    #          {'status': 'error', 'reason': <reason>}
    @staticmethod
    def process_result_autoscale_lock_scale_req(result):
        if isinstance(result, dict) and 'status' in result:
            if result['status'] == 'ok':
                return result['status']

            if result['status'] == 'error':
                if result['reason'] == 'locked':
                    raise LockError(result)
                elif result['reason'] == 'resp_timeout':
                    raise TimeoutError(result)
                elif result['reason'] == 'autoscale_false':
                    raise ConfigError(result)

        return UnknownError(result)

    # <reason>: 'not_locked' | 'resp_timeout' | 'autoscale_false'
    # results: {'status' : 'ok'} or
    #          {'status': 'error', 'reason': <reason>}
    @staticmethod
    def process_result_autoscale_unlock_scale_req(result):
        if isinstance(result, dict) and 'status' in result:
            if result['status'] == 'ok':
                return result['status']

            if result['status'] == 'error':
                if result['reason'] == 'not_locked':
                    raise LockError(result)
                elif result['reason'] == 'resp_timeout':
                    raise TimeoutError(result)
                elif result['reason'] == 'autoscale_false':
                    raise ConfigError(result)

        return UnknownError(result)

    # result: 'ok'
    @staticmethod
    def process_result_nop(result):
        """
        Processes the result of a nop operation.
        Raises the appropriate exception if the operation failed.
        """
        if result != 'ok':
            raise UnknownError(result)

    @staticmethod
    def new_req_list_t(other = None):
        """
        Returns a new ReqList object allowing multiple parallel requests for
        the Transaction class.
        """
        return _JSONReqListTransaction(other)

    @staticmethod
    def new_req_list_tso(other = None):
        """
        Returns a new ReqList object allowing multiple parallel requests for
        the TransactionSingleOp class.
        """
        return _JSONReqListTransactionSingleOp(other)

    def close(self):
        self._conn.close()

class ScalarisError(Exception):
    """Base class for errors in the scalaris package."""

class AbortError(ScalarisError):
    """
    Exception that is thrown if a the commit of a write operation on a scalaris
    ring fails.
    """

    def __init__(self, raw_result, failed_keys):
        self.raw_result = raw_result
        self.failed_keys = failed_keys
    def __str__(self):
        return repr(self.raw_result)

class ConnectionError(ScalarisError):
    """
    Exception that is thrown if an operation on a scalaris ring fails because
    a connection does not exist or has been disconnected.
    """

    def __init__(self, raw_result, response = None, error = None):
        self.raw_result = raw_result
        self.response = response
        self.error = error
    def __str__(self):
        result_str = ''
        if self.response is not None:
            result_str += 'status: ' + str(self.response.status)
            result_str += ', reason: ' + self.response.reason + '\n'
        if self.error is not None:
            result_str += 'error: ' + repr(self.error) + '\n'
        result_str += 'data: ' + repr(self.raw_result)
        return result_str

class KeyChangedError(ScalarisError):
    """
    Exception that is thrown if a test_and_set operation on a scalaris ring
    fails because the old value did not match the expected value.
    """

    def __init__(self, raw_result, old_value):
        self.raw_result = raw_result
        self.old_value = old_value
    def __str__(self):
        return repr(self.raw_result) + ', old value: ' + repr(self.old_value)

class NodeNotFoundError(ScalarisError):
    """
    Exception that is thrown if a delete operation on a scalaris ring fails
    because no scalaris node was found.
    """

    def __init__(self, raw_result):
        self.raw_result = raw_result
    def __str__(self):
        return repr(self.raw_result)

class NotFoundError(ScalarisError):
    """
    Exception that is thrown if a read operation on a scalaris ring fails
    because the key did not exist before.
    """

    def __init__(self, raw_result):
        self.raw_result = raw_result
    def __str__(self):
        return repr(self.raw_result)

class NotAListError(ScalarisError):
    """
    Exception that is thrown if a add_del_on_list operation on a scalaris ring
    fails because the participating values are not lists.
    """

    def __init__(self, raw_result):
        self.raw_result = raw_result
    def __str__(self):
        return repr(self.raw_result)

class NotANumberError(ScalarisError):
    """
    Exception that is thrown if a add_del_on_list operation on a scalaris ring
    fails because the participating values are not numbers.
    """

    def __init__(self, raw_result):
        self.raw_result = raw_result
    def __str__(self):
        return repr(self.raw_result)

class TimeoutError(ScalarisError):
    """
    Exception that is thrown if a read or write operation on a scalaris ring
    fails due to a timeout.
    """

    def __init__(self, raw_result):
        self.raw_result = raw_result
    def __str__(self):
        return repr(self.raw_result)

class ConfigError(ScalarisError):
    """
    Exception that is thrown if a autoscale operation fails, because it was not
    configured correctly.
    """
    def __init__(self, raw_result):
        self.raw_result = raw_result
    def __str__(self):
        return repr(self.raw_result)

class LockError(ScalarisError):
    """
    Exception that is thrown if a autoscale lock/unlock operation fails,
    because of a wrong lock state, i.e. lock when is already locked or unlock
    when not locked.
    """
    def __init__(self, raw_result):
        self.raw_result = raw_result
    def __str__(self):
        return repr(self.raw_result)

class UnknownError(ScalarisError):
    """
    Generic exception that is thrown during operations on a scalaris ring, e.g.
    if an unknown result has been returned.
    """

    def __init__(self, raw_result):
        self.raw_result = raw_result
    def __str__(self):
        return repr(self.raw_result)

class DeleteResult(object):
    """
    Stores the result of a delete operation.
    """
    def __init__(self, ok, locks_set, undefined):
        self.ok = ok
        self.locks_set = locks_set
        self.undefined = undefined

class ConnectionPool(object):
    """
    Implements a simple (thread-safe) connection pool for Scalaris connections.
    """

    def __init__(self, max_connections):
        """
        Create a new connection pool with the given maximum number of connections.
        """
        self._max_connections = max_connections
        self._available_conns = []
        self._checked_out_sema = threading.BoundedSemaphore(value=max_connections)
        self._wait_cond = threading.Condition()

    def _new_connection(self):
        """
        Creates a new connection for the pool. Override this to use some other
        connection class than JSONConnection.
        """
        return JSONConnection()

    def _get_connection(self):
        """
        Gets a connection from the pool. Creates a new connection if necessary.
        Returns <tt>None</tt> if the maximum number of connections has already
        been hit.
        """
        conn = None
        if self._max_connections == 0:
            conn = self._new_connection()
        elif self._checked_out_sema.acquire(False):
            try:
                conn = self._available_conns.pop(0)
            except IndexError:
                conn = self._new_connection()
        return conn

    def get_connection(self, timeout = None):
        """
        Tries to get a valid connection from the pool waiting at most
        the given timeout. If timeout is an integer, it will be interpreted as
        a number of milliseconds. Alternatively, timeout can be given as a
        datetime.timedelta. Creates a new connection if necessary
        and the maximum number of connections has not been hit yet.
        If the timeout is hit and no connection is available, <tt>None</tt> is
        returned.
        """
        if timeout == None:
            return self._get_connection()
        else:
            if isinstance(timeout, numbers.Integral ):
                timeout = timedelta(milliseconds=timeout)
            start = datetime.now()
            while True:
                conn = self._get_connection()
                if not conn is None:
                    return conn
                self._wait_cond.wait(timeout.microseconds / 1000.0)
                end = datetime.now()
                if end - start > timeout:
                    return None

    def release_connection(self, connection):
        """
        Puts the given connection back into the pool.
        """
        self._available_conns.append(connection)
        self._checked_out_sema.release()
        self._wait_cond.notify_all()

    def close_all(self):
        """
        Close all connections to scalaris.
        """
        for conn in self._available_conns:
            conn.close()
        self._available_conns = []

class TransactionSingleOp(object):
    """
    Single write or read operations on scalaris.
    """

    def __init__(self, conn = None):
        """
        Create a new object using the given connection
        """
        if conn is None:
            conn = JSONConnection()
        self._conn = conn

    def new_req_list(self, other = None):
        """
        Returns a new ReqList object allowing multiple parallel requests.
        """
        return self._conn.new_req_list_tso(other)

    def req_list(self, reqlist):
        """
        Issues multiple parallel requests to scalaris; each will be committed.
        NOTE: The execution order of multiple requests on the same key is
        undefined!
        Request lists can be created using new_req_list().
        The returned list has the following form:
        [{'status': 'ok'} or {'status': 'ok', 'value': xxx} or
        {'status': 'fail', 'reason': 'timeout' or 'abort' or 'not_found'}].
        Elements of this list can be processed with process_result_read() and
        process_result_write().
        """
        result = self._conn.callp('/api/tx.yaws', 'req_list_commit_each', [reqlist.get_requests()])
        result = self._conn.process_result_req_list_tso(result)
        return result

    def process_result_read(self, result):
        """
        Processes a result element from the list returned by req_list() which
        originated from a read operation.
        Returns the read value on success.
        Raises the appropriate exceptions if a failure occurred during the
        operation.
        Beware: lists of (small) integers may be (falsely) returned as a string -
        use str_to_list() to convert such strings.
        """
        return self._conn.process_result_read(result)

    def process_result_write(self, result):
        """
        Processes a result element from the list returned by req_list() which
        originated from a write operation.
        Raises the appropriate exceptions if a failure occurred during the
        operation.
        """
        self._conn.check_fail_abort(result)
        return self._conn.process_result_write(result)

    def process_result_add_del_on_list(self, result):
        """
        Processes a result element from the list returned by req_list() which
        originated from a add_del_on_list operation.
        Raises the appropriate exceptions if a failure occurred during the
        operation.
        """
        self._conn.check_fail_abort(result)
        self._conn.process_result_add_del_on_list(result)

    def process_result_add_on_nr(self, result):
        """
        Processes a result element from the list returned by req_list() which
        originated from a add_on_nr operation.
        Raises the appropriate exceptions if a failure occurred during the
        operation.
        """
        self._conn.check_fail_abort(result)
        self._conn.process_result_add_on_nr(result)

    def process_result_test_and_set(self, result):
        """
        Processes a result element from the list returned by req_list() which
        originated from a test_and_set operation.
        Raises the appropriate exceptions if a failure occurred during the
        operation.
        """
        self._conn.check_fail_abort(result)
        self._conn.process_result_test_and_set(result)

    def read(self, key):
        """
        Read the value at key.
        Beware: lists of (small) integers may be (falsely) returned as a string -
        use str_to_list() to convert such strings.
        """
        result = self._conn.callp('/api/tx.yaws', 'read', [key])
        return self._conn.process_result_read(result)

    def write(self, key, value):
        """
        Write the value to key.
        """
        value = self._conn.encode_value(value)
        result = self._conn.callp('/api/tx.yaws', 'write', [key, value])
        self._conn.check_fail_abort(result)
        self._conn.process_result_write(result)

    def add_del_on_list(self, key, to_add, to_remove):
        """
        Changes the list stored at the given key, i.e. first adds all items in
        to_add then removes all items in to_remove.
        Both, to_add and to_remove, must be lists.
        Assumes en empty list if no value exists at key.
        """
        result = self._conn.callp('/api/tx.yaws', 'add_del_on_list', [key, to_add, to_remove])
        self._conn.check_fail_abort(result)
        self._conn.process_result_add_del_on_list(result)

    def add_on_nr(self, key, to_add):
        """
        Changes the number stored at the given key, i.e. adds some value.
        Assumes 0 if no value exists at key.
        """
        result = self._conn.callp('/api/tx.yaws', 'add_on_nr', [key, to_add])
        self._conn.check_fail_abort(result)
        self._conn.process_result_add_on_nr(result)

    def test_and_set(self, key, old_value, new_value):
        """
        Atomic test and set, i.e. if the old value at key is old_value, then
        write new_value.
        """
        old_value = self._conn.encode_value(old_value)
        new_value = self._conn.encode_value(new_value)
        result = self._conn.callp('/api/tx.yaws', 'test_and_set', [key, old_value, new_value])
        self._conn.check_fail_abort(result)
        self._conn.process_result_test_and_set(result)

    def nop(self, value):
        """
        No operation (may be used for measuring the JSON overhead).
        """
        value = self._conn.encode_value(value)
        result = self._conn.callp('/api/tx.yaws', 'nop', [value])
        self._conn.process_result_nop(result)

    def close_connection(self):
        """
        Close the connection to scalaris
        (it will automatically be re-opened on the next request).
        """
        self._conn.close()

class Transaction(object):
    """
    Write or read operations on scalaris inside a transaction.
    """

    def __init__(self, conn = None):
        """
        Create a new object using the given connection
        """
        if conn is None:
            conn = JSONConnection()
        self._conn = conn
        self._tlog = None

    def new_req_list(self, other = None):
        """
        Returns a new ReqList object allowing multiple parallel requests.
        """
        return self._conn.new_req_list_t(other)

    def req_list(self, reqlist):
        """
        Issues multiple parallel requests to scalaris.
        Request lists can be created using new_req_list().
        The returned list has the following form:
        [{'status': 'ok'} or {'status': 'ok', 'value': xxx} or
        {'status': 'fail', 'reason': 'timeout' or 'abort' or 'not_found'}].
        Elements of this list can be processed with process_result_read() and
        process_result_write().
        A commit (at the end of the request list) will be automatically checked
        for its success.
        """
        if self._tlog is None:
            result = self._conn.callp('/api/tx.yaws', 'req_list', [reqlist.get_requests()])
        else:
            result = self._conn.callp('/api/tx.yaws', 'req_list', [self._tlog, reqlist.get_requests()])
        (tlog, result) = self._conn.process_result_req_list_t(result)
        self._tlog = tlog
        if reqlist.is_commit():
            self._process_result_commit(result[-1])
            # transaction was successful: reset transaction log
            self._tlog = None
        return result

    def process_result_read(self, result):
        """
        Processes a result element from the list returned by req_list() which
        originated from a read operation.
        Returns the read value on success.
        Raises the appropriate exceptions if a failure occurred during the
        operation.
        Beware: lists of (small) integers may be (falsely) returned as a string -
        use str_to_list() to convert such strings.
        """
        return self._conn.process_result_read(result)

    def process_result_write(self, result):
        """
        Processes a result element from the list returned by req_list() which
        originated from a write operation.
        Raises the appropriate exceptions if a failure occurred during the
        operation.
        """
        return self._conn.process_result_write(result)

    def process_result_add_del_on_list(self, result):
        """
        Processes a result element from the list returned by req_list() which
        originated from a add_del_on_list operation.
        Raises the appropriate exceptions if a failure occurred during the
        operation.
        """
        self._conn.process_result_add_del_on_list(result)

    def process_result_add_on_nr(self, result):
        """
        Processes a result element from the list returned by req_list() which
        originated from a add_on_nr operation.
        Raises the appropriate exceptions if a failure occurred during the
        operation.
        """
        self._conn.process_result_add_on_nr(result)

    def process_result_test_and_set(self, result):
        """
        Processes a result element from the list returned by req_list() which
        originated from a test_and_set operation.
        Raises the appropriate exceptions if a failure occurred during the
        operation.
        """
        self._conn.process_result_test_and_set(result)

    def _process_result_commit(self, result):
        """
        Processes a result element from the list returned by req_list() which
        originated from a commit operation.
        Raises the appropriate exceptions if a failure occurred during the
        operation.
        """
        return self._conn.process_result_commit(result)

    def commit(self):
        """
        Issues a commit operation to scalaris validating the previously
        created operations inside the transaction.
        """
        result = self.req_list(self.new_req_list().add_commit())[0]
        self._process_result_commit(result)
        # reset tlog (minor optimization which is not done in req_list):
        self._tlog = None

    def abort(self):
        """
        Aborts all previously created operations inside the transaction.
        """
        self._tlog = None

    def read(self, key):
        """
        Issues a read operation to scalaris, adds it to the current
        transaction and returns the result.
        Beware: lists of (small) integers may be (falsely) returned as a string -
        use str_to_list() to convert such strings.
        """
        result = self.req_list(self.new_req_list().add_read(key))[0]
        return self.process_result_read(result)

    def write(self, key, value):
        """
        Issues a write operation to scalaris and adds it to the current
        transaction.
        """
        result = self.req_list(self.new_req_list().add_write(key, value))[0]
        self.process_result_write(result)

    def add_del_on_list(self, key, to_add, to_remove):
        """
        Issues a add_del_on_list operation to scalaris and adds it to the
        current transaction.
        Changes the list stored at the given key, i.e. first adds all items in
        to_add then removes all items in to_remove.
        Both, to_add and to_remove, must be lists.
        Assumes en empty list if no value exists at key.
        """
        result = self.req_list(self.new_req_list().add_add_del_on_list(key, to_add, to_remove))[0]
        self.process_result_add_del_on_list(result)

    def add_on_nr(self, key, to_add):
        """
        Issues a add_on_nr operation to scalaris and adds it to the
        current transaction.
        Changes the number stored at the given key, i.e. adds some value.
        Assumes 0 if no value exists at key.
        """
        result = self.req_list(self.new_req_list().add_add_on_nr(key, to_add))[0]
        self.process_result_add_on_nr(result)

    def test_and_set(self, key, old_value, new_value):
        """
        Issues a test_and_set operation to scalaris and adds it to the
        current transaction.
        Atomic test and set, i.e. if the old value at key is old_value, then
        write new_value.
        """
        result = self.req_list(self.new_req_list().add_test_and_set(key, old_value, new_value))[0]
        self.process_result_test_and_set(result)

    def nop(self, value):
        """
        No operation (may be used for measuring the JSON overhead).
        """
        value = self._conn.encode_value(value)
        result = self._conn.callp('/api/tx.yaws', 'nop', [value])
        self._conn.process_result_nop(result)

    def close_connection(self):
        """
        Close the connection to scalaris
        (it will automatically be re-opened on the next request).
        """
        self._conn.close()

class _JSONReqList(object):
    """
    Generic request list.
    """

    def __init__(self, other = None):
        """
        Create a new object using a JSON connection.
        """
        self._requests = []
        self._is_commit = False
        if other is not None:
            self.extend(other)

    def add_read(self, key):
        """
        Adds a read operation to the request list.
        """
        if (self._is_commit):
            raise RuntimeError("No further request supported after a commit!")
        self._requests.append({'read': key})
        return self

    def add_write(self, key, value):
        """
        Adds a write operation to the request list.
        """
        if (self._is_commit):
            raise RuntimeError("No further request supported after a commit!")
        self._requests.append({'write': {key: JSONConnection.encode_value(value)}})
        return self

    def add_add_del_on_list(self, key, to_add, to_remove):
        """
        Adds a add_del_on_list operation to the request list.
        """
        if (self._is_commit):
            raise RuntimeError("No further request supported after a commit!")
        self._requests.append({'add_del_on_list': {'key': key, 'add': to_add, 'del': to_remove}})
        return self

    def add_add_on_nr(self, key, to_add):
        """
        Adds a add_on_nr operation to the request list.
        """
        if (self._is_commit):
            raise RuntimeError("No further request supported after a commit!")
        self._requests.append({'add_on_nr': {key: to_add}})
        return self

    def add_test_and_set(self, key, old_value, new_value):
        """
        Adds a test_and_set operation to the request list.
        """
        if (self._is_commit):
            raise RuntimeError("No further request supported after a commit!")
        self._requests.append({'test_and_set': {'key': key, 'old': old_value, 'new': new_value}})
        return self

    def add_commit(self):
        """
        Adds a commit operation to the request list.
        """
        if (self._is_commit):
            raise RuntimeError("Only one commit per request list allowed!")
        self._requests.append({'commit': ''})
        self._is_commit = True
        return self

    def get_requests(self):
        """
        Gets the collected requests.
        """
        return self._requests

    def is_commit(self):
        """
        Returns whether the transactions contains a commit or not.
        """
        return self._is_commit

    def is_empty(self):
        """
        Checks whether the request list is empty.
        """
        return self._requests == []

    def size(self):
        """
        Gets the number of requests in the list.
        """
        return len(self._requests)

    def extend(self, other):
        """
        Adds all requests of the other request list to the end of this list.
        """
        self._requests.extend(other._requests)
        return self

class _JSONReqListTransaction(_JSONReqList):
    """
    Request list for use with Transaction.req_list().
    """

    def __init__(self, other = None):
        _JSONReqList.__init__(self, other)

class _JSONReqListTransactionSingleOp(_JSONReqList):
    """
    Request list for use with TransactionSingleOp.req_list() which does not
    support commits.
    """

    def __init__(self, other = None):
        _JSONReqList.__init__(self, other)

    def add_commit(self):
        """
        Adds a commit operation to the request list.
        """
        raise RuntimeError("No commit allowed in TransactionSingleOp.req_list()!")

class ReplicatedDHT(object):
    """
    Non-transactional operations on the replicated DHT of scalaris
    """

    def __init__(self, conn = None):
        """
        Create a new object using the given connection.
        """
        if conn is None:
            conn = JSONConnection()
        self._conn = conn

    # returns the number of successfully deleted items
    # use get_last_delete_result() to get more details
    def delete(self, key, timeout = 2000):
        """
        Tries to delete the value at the given key.

        WARNING: This function can lead to inconsistent data (e.g. deleted items
        can re-appear). Also when re-creating an item the version before the
        delete can re-appear.
        """
        result = self._conn.callp('/api/rdht.yaws', 'delete', [key, timeout])
        (success, ok, results) = self._conn.process_result_delete(result)
        self._lastDeleteResult = results
        if success == True:
            return ok
        elif success == 'timeout':
            raise TimeoutError(result)
        else:
            raise UnknownError(result)

    def get_last_delete_result(self):
        """
        Returns the result of the last call to delete().

        NOTE: This function traverses the result list returned by scalaris and
        therefore takes some time to process. It is advised to store the returned
        result object once generated.
        """
        return self._conn.create_delete_result(self._lastDeleteResult)

    def nop(self, value):
        """
        No operation (may be used for measuring the JSON overhead).
        """
        value = self._conn.encode_value(value)
        result = self._conn.callp('/api/rdht.yaws', 'nop', [value])
        self._conn.process_result_nop(result)

    def close_connection(self):
        """
        Close the connection to scalaris
        (it will automatically be re-opened on the next request).
        """
        self._conn.close()

class RoutingTable(object):
    """
    API for using routing tables
    """

    def __init__(self, conn = None):
        """
        Create a new object using the given connection.
        """
        if conn is None:
            conn = JSONConnection()
        self._conn = conn

    def get_replication_factor(self):
      result = self._conn.callp('/api/rt.yaws', 'get_replication_factor', [])
      if isinstance(result, dict) and 'status' in result and len(result) == 2 and result['status'] == 'ok' and 'value' in result:
          return result['value']
      else:
        raise UnknownError(result)

class ScalarisVM(object):
    """
    Provides methods to interact with a specific Scalaris (Erlang) VM.
    """

    class GetInfoResult(object):
        def __init__(self, scalarisVersion, erlangVersion, memTotal, uptime,
                     erlangNode, ip, port, yawsPort):
            self.scalarisVersion = scalarisVersion
            self.erlangVersion = erlangVersion
            self.memTotal = memTotal
            self.uptime = uptime
            self.erlangNode = erlangNode
            self.ip = ip
            self.port = port
            self.yawsPort = yawsPort

    def __init__(self, conn = None):
        """
        Create a new object using the given connection.
        """
        if conn is None:
            conn = JSONConnection()
        self._conn = conn

    def getVersion(self):
        """
        Gets the version of the Scalaris VM of the current connection.
        """
        result = self._conn.callp('/api/vm.yaws', 'get_version', [])
        return self._conn.process_result_vm_get_version(result)

    def getInfo(self):
        """
        Gets some information about the VM and Scalaris.
        """
        result = self._conn.callp('/api/vm.yaws', 'get_info', [])
        return self._conn.process_result_vm_get_info(result)

    def getNumberOfNodes(self):
        """
        Gets the number of nodes in the Scalaris VM of the current connection.
        """
        result = self._conn.callp('/api/vm.yaws', 'number_of_nodes', [])
        return self._conn.process_result_vm_get_number_of_nodes(result)

    def getNodes(self):
        """
        Gets the names of the nodes in the Scalaris VM of the current connection.
        """
        result = self._conn.callp('/api/vm.yaws', 'get_nodes', [])
        return self._conn.process_result_vm_get_nodes(result)

    def addNodes(self, number):
        """
        Adds Scalaris nodes to the Scalaris VM of the current connection.
        """
        result = self._conn.callp('/api/vm.yaws', 'add_nodes', [number])
        return self._conn.process_result_vm_add_nodes(result)

    def shutdownNode(self, name):
        """
        Shuts down the given node (graceful leave) in the Scalaris VM of the current connection.
        """
        result = self._conn.callp('/api/vm.yaws', 'shutdown_node', [name])
        return self._conn.process_result_vm_delete_node(result)

    def killNode(self, name):
        """
        Kills the given node in the Scalaris VM of the current connection.
        """
        result = self._conn.callp('/api/vm.yaws', 'kill_node', [name])
        return self._conn.process_result_vm_delete_node(result)

    def shutdownNodes(self, number):
        """
        Shuts down the given number of nodes (graceful leave) in the Scalaris VM of the current connection.
        """
        result = self._conn.callp('/api/vm.yaws', 'shutdown_nodes', [number])
        return self._conn.process_result_vm_delete_nodes(result)

    def killNodes(self, number):
        """
        Kills the given number of nodes in the Scalaris VM of the current connection.
        """
        result = self._conn.callp('/api/vm.yaws', 'kill_nodes', [number])
        return self._conn.process_result_vm_delete_nodes(result)

    def shutdownNodesByName(self, names):
        """
        Shuts down the given nodes (graceful leave) in the Scalaris VM of the current connection.
        """
        result = self._conn.callp('/api/vm.yaws', 'shutdown_nodes_by_name', [names])
        return self._conn.process_result_vm_delete_nodes(result)

    def killNodesByName(self, names):
        """
        Kills the given nodes in the Scalaris VM of the current connection.
        """
        result = self._conn.callp('/api/vm.yaws', 'kill_nodes_by_name', [names])
        return self._conn.process_result_vm_delete_nodes(result)

    def getOtherVMs(self, maxVMs):
        """
        Retrieves additional nodes from the Scalaris VM of the current
        connection for use as URLs in JSONConnection.
        """
        if maxVMs <= 0:
            raise ValueError("max must be an integer > 0")
        result = self._conn.callp('/api/vm.yaws', 'get_other_vms', [maxVMs])
        return self._conn.process_result_vm_get_other_vms(result)

    def shutdownVM(self):
        """
        Tells the Scalaris VM of the current connection to shut down gracefully.
        """
        result = self._conn.callp('/api/vm.yaws', 'shutdown_vm', [])
        return self._conn.process_result_vm_delete_vm(result)

    def killVM(self):
        """
        Kills the Scalaris VM of the current connection.
        """
        result = self._conn.callp('/api/vm.yaws', 'kill_vm', [])
        return self._conn.process_result_vm_delete_vm(result)

    def nop(self, value):
        """
        No operation (may be used for measuring the JSON overhead).
        """
        value = self._conn.encode_value(value)
        result = self._conn.callp('/api/vm.yaws', 'nop', [value])
        self._conn.process_result_nop(result)

    def close_connection(self):
        """
        Close the connection to scalaris
        (it will automatically be re-opened on the next request).
        """
        self._conn.close()

class Autoscale(object):
    """
    Provides methods to interact with autoscale API.
    """

    api = '/api/autoscale.yaws'

    """
    Create a new object using the given connection.
    """
    def __init__(self, conn = None):
        if conn is None:
            conn = JSONConnection()
        self._conn = conn

    def process_result_check_config(self, result):
        return self._conn.process_result_autoscale_check_config(result)

    def process_result_pull_scale_req(self, result):
        return self._conn.process_result_autoscale_pull_scale_req(result)

    def process_result_lock_scale_req(self, result):
        return self._conn.process_result_autoscale_lock_scale_req(result)

    def process_result_unlock_scale_req(self, result):
        return self._conn.process_result_autoscale_unlock_scale_req(result)

    """ API calls """
    def check_config(self):
        result = self._conn.callp(Autoscale.api, 'check_config', [])
        return self.process_result_check_config(result)

    def pull_scale_req(self):
        result = self._conn.callp(Autoscale.api, 'pull_scale_req', [])
        return self.process_result_pull_scale_req(result)

    def lock_scale_req(self):
        result = self._conn.callp(Autoscale.api, 'lock_scale_req', [])
        return self.process_result_lock_scale_req(result)

    def unlock_scale_req(self):
        result = self._conn.callp(Autoscale.api, 'unlock_scale_req', [])
        return self.process_result_unlock_scale_req(result)

    def close_connection(self):
        """
        Close the connection to scalaris
        (it will automatically be re-opened on the next request).
        """
        self._conn.close()

def str_to_list(value):
    """
    Converts a string to a list of integers.
    If the expected value of a read operation is a list, the returned value
    could be (mistakenly) a string if it is a list of integers.
    """
    if (isinstance(value, str) or isinstance(value, unicode)):
        chars = list(value)
        return [ord(char) for char in chars]
    else:
        return value
