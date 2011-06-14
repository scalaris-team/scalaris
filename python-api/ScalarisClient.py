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

from Scalaris import TransactionSingleOp, ReplicatedDHT, PubSub
from Scalaris import ConnectionException, TimeoutException, NotFoundException, AbortException, UnknownException
import sys

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
        rdht = ReplicatedDHT()
        key = sys.argv[2]
        if len(sys.argv) >= 4:
            timeout = sys.argv[3]
        else:
            timeout = 2000
        
        try:
            ok = rdht.delete(key)
            results = rdht.getLastDeleteResult()
            print 'delete(' + key + ', ' + str(timeout) + '): ok, deleted: ' + str(ok) + ' (' + repr(results) + ')'
        except TimeoutException as instance:
            results = rdht.getLastDeleteResult()
            print 'delete(' + key + ', ' + str(timeout) + '): failed (timeout), deleted: ' + str(ok) + ' (' + repr(results) + ')'
        except UnknownException as instance:
            print 'delete(' + key + ') failed with unknown: ' + str(instance)
            sys.exit(1)
    elif (len(sys.argv) == 4 and sys.argv[1] in ["--publish", "-p"]):
        ps = PubSub()
        topic = sys.argv[2]
        content = sys.argv[3]
        try:
            ps.publish(topic, content)
            print 'publish(' + topic + ', ' + content + '): ok'
        except ConnectionException as instance:
            print 'publish(' + topic + ', ' + content + ') failed with connection error'
            sys.exit(1)
        except UnknownException as instance:
            print 'publish(' + topic + ', ' + content + ') failed with unknown: ' + str(instance)
            sys.exit(1)
    elif (len(sys.argv) == 4 and sys.argv[1] in ["--subscribe", "-s"]):
        ps = PubSub()
        topic = sys.argv[2]
        url = sys.argv[3]
        try:
            ps.subscribe(topic, url)
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
        ps = PubSub()
        topic = sys.argv[2]
        url = sys.argv[3]
        try:
            ps.unsubscribe(topic, url)
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
        ps = PubSub()
        topic = sys.argv[2]
        try:
            value = ps.getSubscribers(topic)
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
