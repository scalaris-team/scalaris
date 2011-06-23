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

from scalaris import TransactionSingleOp, ReplicatedDHT, PubSub
from scalaris import ConnectionError, TimeoutError, NotFoundError, AbortError, UnknownError
import scalaris_bench
import sys

if __name__ == "__main__":
    if (len(sys.argv) == 3 and sys.argv[1] in ["--read", "-r"]):
        sc = TransactionSingleOp()
        key = sys.argv[2]
        try:
            value = sc.read(key)
            print 'read(' + key + ') = ' + repr(value)
        except ConnectionError as instance:
            print 'read(' + key + ') failed with connection error'
            sys.exit(1)
        except TimeoutError as instance:
            print 'read(' + key + ') failed with timeout'
            sys.exit(1)
        except NotFoundError as instance:
            print 'read(' + key + ') failed with not_found'
            sys.exit(1)
        except UnknownError as instance:
            print 'read(' + key + ') failed with unknown: ' + str(instance)
            sys.exit(1)
    elif (len(sys.argv) == 4 and sys.argv[1] in ["--write", "-w"]):
        sc = TransactionSingleOp()
        key = sys.argv[2]
        value = sys.argv[3]
        try:
            sc.write(key, value)
            print 'write(' + key + ', ' + value + '): ok'
        except ConnectionError as instance:
            print 'write(' + key + ', ' + value + ') failed with connection error'
            sys.exit(1)
        except TimeoutError as instance:
            print 'write(' + key + ', ' + value + ') failed with timeout'
            sys.exit(1)
        except AbortError as instance:
            print 'write(' + key + ', ' + value + ') failed with abort'
            sys.exit(1)
        except UnknownError as instance:
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
            results = rdht.get_last_delete_result()
            print 'delete(' + key + ', ' + str(timeout) + '): ok, deleted: ' + str(ok) + ' (' + repr(results) + ')'
        except TimeoutError as instance:
            results = rdht.get_last_delete_result()
            print 'delete(' + key + ', ' + str(timeout) + '): failed (timeout), deleted: ' + str(ok) + ' (' + repr(results) + ')'
        except UnknownError as instance:
            print 'delete(' + key + ') failed with unknown: ' + str(instance)
            sys.exit(1)
    elif (len(sys.argv) == 4 and sys.argv[1] in ["--publish", "-p"]):
        ps = PubSub()
        topic = sys.argv[2]
        content = sys.argv[3]
        try:
            ps.publish(topic, content)
            print 'publish(' + topic + ', ' + content + '): ok'
        except ConnectionError as instance:
            print 'publish(' + topic + ', ' + content + ') failed with connection error'
            sys.exit(1)
        except UnknownError as instance:
            print 'publish(' + topic + ', ' + content + ') failed with unknown: ' + str(instance)
            sys.exit(1)
    elif (len(sys.argv) == 4 and sys.argv[1] in ["--subscribe", "-s"]):
        ps = PubSub()
        topic = sys.argv[2]
        url = sys.argv[3]
        try:
            ps.subscribe(topic, url)
            print 'subscribe(' + topic + ', ' + url + '): ok'
        except ConnectionError as instance:
            print 'subscribe(' + topic + ', ' + url + ') failed with connection error'
            sys.exit(1)
        except TimeoutError as instance:
            print 'subscribe(' + topic + ', ' + url + ') failed with timeout'
            sys.exit(1)
        except AbortError as instance:
            print 'subscribe(' + topic + ', ' + url + ') failed with abort'
            sys.exit(1)
        except UnknownError as instance:
            print 'subscribe(' + topic + ', ' + url + ') failed with unknown: ' + str(instance)
            sys.exit(1)
    elif (len(sys.argv) == 4 and sys.argv[1] in ["--unsubscribe", "-u"]):
        ps = PubSub()
        topic = sys.argv[2]
        url = sys.argv[3]
        try:
            ps.unsubscribe(topic, url)
            print 'unsubscribe(' + topic + ', ' + url + '): ok'
        except ConnectionError as instance:
            print 'unsubscribe(' + topic + ', ' + url + ') failed with connection error'
            sys.exit(1)
        except TimeoutError as instance:
            print 'unsubscribe(' + topic + ', ' + url + ') failed with timeout'
            sys.exit(1)
        except NotFoundError as instance:
            print 'unsubscribe(' + topic + ', ' + url + ') failed with not found'
            sys.exit(1)
        except AbortError as instance:
            print 'unsubscribe(' + topic + ', ' + url + ') failed with abort'
            sys.exit(1)
        except UnknownError as instance:
            print 'unsubscribe(' + topic + ', ' + url + ') failed with unknown: ' + str(instance)
            sys.exit(1)
    elif (len(sys.argv) == 3 and sys.argv[1] in ["--getsubscribers", "-g"]):
        ps = PubSub()
        topic = sys.argv[2]
        try:
            value = ps.get_subscribers(topic)
            print 'get_subscribers(' + topic + ') = ' + repr(value)
        except ConnectionError as instance:
            print 'get_subscribers(' + topic + ') failed with connection error'
            sys.exit(1)
        except UnknownError as instance:
            print 'get_subscribers(' + topic + ') failed with unknown: ' + str(instance)
            sys.exit(1)
    elif ((len(sys.argv) == 2 or len(sys.argv) >= 4) and sys.argv[1] in ["--minibench", "-b"]):
        if (len(sys.argv) == 2):
            scalaris_bench.minibench(100, xrange(1, 10, 1))
        elif (len(sys.argv) >= 4):
            testruns = int(sys.argv[2])
            benchmarks = []
            for i in xrange(3, min(12, len(sys.argv))):
                if sys.argv[i] == 'all':
                    benchmarks = xrange(1, 10, 1)
                else:
                    benchmarks.append(int(sys.argv[i]))
            scalaris_bench.minibench(testruns, benchmarks)
    else:
        print 'usage: scalaris.py [Options]'
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
        print '                                      topic'
        print ' -s,--subscribe <topic> <url>         subscribe to a topic'
        print ' -g,--getsubscribers <topic>          get subscribers of a topic'
        print ' -u,--unsubscribe <topic> <url>       unsubscribe from a topic'
        print ' -h,--help                            print this message'
        print ' -b,--minibench <runs> <benchmarks>   run selected mini benchmark(s)'
        print '                                      [1|...|9|all] (default: all'
        print '                                      benchmarks, 100 test runs)'
