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

import scalaris
from datetime import datetime
from threading import Thread
import time, threading
import random, string
import os, sys, traceback

_BENCH_DATA_SIZE = 1000
"""The size of a single data item that is send to scalaris."""
_benchTime = 0
"""This is used to create different erlang keys for each run."""
_PERCENT_TO_REMOVE = 5
"""Cut 5% off of both ends of the result list."""
_TESTRUNS = 1;
"""Number of test runs (accumulates results over all test runs)."""

if 'SCALARIS_JSON_URLS' in os.environ and os.environ['SCALARIS_JSON_URLS'] != '':
    DEFAULT_URLS = os.environ['SCALARIS_JSON_URLS'].split()
else:
    DEFAULT_URLS = [scalaris.DEFAULT_URL]

def minibench(operations, threads_per_node, benchmarks):
    """
    Default minimal benchmark.
    
    Tests some strategies for writing key/value pairs to scalaris:
    1) writing binary objects (random data, size = _BENCH_DATA_SIZE)
    2) writing string objects (random data, size = _BENCH_DATA_SIZE)
    each with the given number of consecutive operations and parallel
    threads per Scalaris node,
    * first using a new Transaction or TransactionSingleOp for each test,
    * then using a new Transaction or TransactionSingleOp but re-using a single connection,
    * and finally re-using a single Transaction or TransactionSingleOp object.
    """
    # The time when the (whole) benchmark suite was started.
    # This is used to create different erlang keys for each run.
    _benchTime = _getCurrentMillis()
    
    parallel_runs = len(DEFAULT_URLS) * threads_per_node;
    print 'Number of available nodes: ' + str(len(DEFAULT_URLS))
    print '-> Using ' + str(parallel_runs) + ' parallel instances per test run...'
    sys.stdout.flush()

    print 'Benchmark of scalaris.TransactionSingleOp:'
    sys.stdout.flush()
    test_types = ['binary', 'string']
    test_types_str = ['B', 'S']
    columns = ['TransactionSingleOp.write(string, bytearray)',
               'TransactionSingleOp.write(string, string)']
    test_bench = [TransSingleOpBench1, TransSingleOpBench2, TransSingleOpBench3]
    rows = ['separate connection', 're-use connection', 're-use object']
    test_group = 'transsinglebench';
    results = _getResultArray(rows, columns)
    _runBenchAndPrintResults(benchmarks, results, columns, rows, test_types,
            test_types_str, test_bench, test_group, 1, operations, parallel_runs)

    print '-----'
    print 'Benchmark of scalaris.Transaction:'
    sys.stdout.flush()
    test_types = ['binary', 'string']
    test_types_str = ['B', 'S']
    columns = ['Transaction.write(string, bytearray)',
               'Transaction.write(string, string)']
    test_bench = [TransBench1, TransBench2, TransBench3]
    rows = ['separate connection', 're-use connection', 're-use object']
    test_group = 'transbench';
    results = _getResultArray(rows, columns)
    _runBenchAndPrintResults(benchmarks, results, columns, rows, test_types,
            test_types_str, test_bench, test_group, 1, operations, parallel_runs)

    print '-----'
    print 'Benchmark incrementing an integer key (read+write):'
    sys.stdout.flush()
    test_types = ['int']
    test_types_str = ['I']
    columns = ['Transaction.add_add_on_nr(string, int)']
    test_bench = [TransIncrementBench1, TransIncrementBench2, TransIncrementBench3]
    rows = ['separate connection', 're-use connection', 're-use object']
    test_group = 'transbench_inc';
    results = _getResultArray(rows, columns)
    _runBenchAndPrintResults(benchmarks, results, columns, rows, test_types,
            test_types_str, test_bench, test_group, 7, operations, parallel_runs)

    print '-----'
    print 'Benchmark read 5 + write 5:'
    sys.stdout.flush()
    test_types = ['binary', 'string']
    test_types_str = ['B', 'S']
    columns = ['Transaction.read(string) + Transaction.write(string, binary)',
               'Transaction.read(string) + Transaction.write(string, string)']
    test_bench = [TransRead5Write5Bench1, TransRead5Write5Bench2, TransRead5Write5Bench3]
    rows = ['separate connection', 're-use connection', 're-use object']
    test_group = 'transbench_r5w5';
    results = _getResultArray(rows, columns)
    _runBenchAndPrintResults(benchmarks, results, columns, rows, test_types,
            test_types_str, test_bench, test_group, 10, operations, parallel_runs)

    print '-----'
    print 'Benchmark appending to a String list (read+write):'
    sys.stdout.flush()
    test_types = ['string']
    test_types_str = ['S']
    columns = ['Transaction.add_add_del_on_list(string, [string], [])']
    test_bench = [TransAppendToListBench1, TransAppendToListBench2, TransAppendToListBench3]
    rows = ['separate connection', 're-use connection', 're-use object']
    test_group = 'transbench_append';
    results = _getResultArray(rows, columns)
    _runBenchAndPrintResults(benchmarks, results, columns, rows, test_types,
            test_types_str, test_bench, test_group, 16, operations, parallel_runs)

class BenchRunnable(Thread):
    """
    Abstract base class of a test run that is to be run in a thread.
    """
    
    def __init__(self, key, value, operations):
        """
        Create a new runnable.
        """
        Thread.__init__(self)
        self._key = key
        self._value = value
        self._operations = operations
        
        self._shouldStop = False
        self._timeAtStart = 0
        self._speed = -1

    def _testBegin(self):
        """
         Call this method when a benchmark is started.
         Sets the time the benchmark was started.
        """
        self._timeAtStart = _getCurrentMillis()

    def _testEnd(self, testRuns):
        """
        Call this method when a benchmark is finished.
        Calculates the time the benchmark took and the number of transactions
        performed during this time.
         """
        timeTaken = _getCurrentMillis() - self._timeAtStart
        speed = (testRuns * 1000) / timeTaken
        return speed
    
    def pre_init(self, j = None):
        """
        Will be called before the benchmark starts with all possible
        variations of "j" in the operation() call.
        "j" with None is the overall initialisation run at first.
        """
        pass
    
    def init(self):
        """
        Will be called at the start of the benchmark.
        """
        pass
    
    def cleanup(self):
        """
        Will be called before the end of the benchmark.
        """
        pass
    
    def operation(self, j):
        """
        The operation to execute during the benchmark.
        """
        pass
    
    def run(self):
        threading.currentThread().name = "BenchRunnable-" + self._key
        retry = 0
        while (retry < 3) and (not self._shouldStop):
            try:
                self.pre_init()
                for j in xrange(self._operations):
                    self.pre_init(j)
                self._testBegin()
                self.init()
                for j in xrange(self._operations):
                    self.operation(j)
                self.cleanup()
                self._speed = self._testEnd(self._operations)
                break
            except:
                # _printException()
                pass
            retry += 1

    def getSpeed(self):
        return self._speed

    def shouldStop(self):
        self._shouldStop = True

class BenchRunnable2(BenchRunnable):
    def __init__(self, key, value, operations):
        """
        Create a new runnable.
        """
        BenchRunnable.__init__(self, key, value, operations)
    
    def init(self):
        """
        Will be called at the start of the benchmark.
        """
        self._connection = _getConnection()
    
    def cleanup(self):
        """
        Will be called before the end of the benchmark.
        """
        self._connection.close()

class TransSingleOpBench1(BenchRunnable):
    """
    Performs a benchmark writing objects using a new TransactionSingleOp object
    for each test.
    """
    def __init__(self, key, value, operations):
        BenchRunnable.__init__(self, key, value, operations)
    
    def operation(self, j):
        tx = scalaris.TransactionSingleOp(conn = _getConnection())
        tx.write(self._key + '_' + str(j), self._value)
        tx.close_connection()

class TransSingleOpBench2(BenchRunnable2):
    """
    Performs a benchmark writing objects using a new TransactionSingleOp but
    re-using a single connection for each test.
    """
    def __init__(self, key, value, operations):
        BenchRunnable2.__init__(self, key, value, operations)

    def operation(self, j):
        tx = scalaris.TransactionSingleOp(conn = self._connection)
        tx.write(self._key + '_' + str(j), self._value)

class TransSingleOpBench3(BenchRunnable):
    """
    Performs a benchmark writing objects using a single TransactionSingleOp
    object for all tests.
    """
    def __init__(self, key, value, operations):
        BenchRunnable.__init__(self, key, value, operations)

    def init(self):
        self._tx = scalaris.TransactionSingleOp(conn = _getConnection())

    def cleanup(self):
        self._tx.close_connection()

    def operation(self, j):
        self._tx.write(self._key + '_' + str(j), self._value)

class TransBench1(BenchRunnable):
    """
    Performs a benchmark writing objects using a new Transaction for each test.
    """
    def __init__(self, key, value, operations):
        BenchRunnable.__init__(self, key, value, operations)

    def operation(self, j):
        tx = scalaris.Transaction(conn = _getConnection())
        tx.write(self._key + '_' + str(j), self._value)
        tx.commit()
        tx.close_connection()

class TransBench2(BenchRunnable2):
    """
    Performs a benchmark writing objects using a new Transaction but re-using a
    single connection for each test.
    """
    def __init__(self, key, value, operations):
        BenchRunnable2.__init__(self, key, value, operations)

    def operation(self, j):
        tx = scalaris.Transaction(conn = self._connection)
        tx.write(self._key + '_' + str(j), self._value)
        tx.commit()

class TransBench3(BenchRunnable):
    """
    Performs a benchmark writing objects using a single Transaction object
    for all tests.
    """
    def __init__(self, key, value, operations):
        BenchRunnable.__init__(self, key, value, operations)

    def init(self):
        self._tx = scalaris.Transaction(conn = _getConnection())

    def cleanup(self):
        self._tx.close_connection()

    def operation(self, j):
        self._tx.write(self._key + '_' + str(j), self._value)
        self._tx.commit()

class TransIncrementBench(BenchRunnable):
    """
    Performs a benchmark writing integer numbers on a single key and
    increasing them.
    Provides convenience methods for the full increment benchmark
    implementations.
    """
    def __init__(self, key, value, operations):
        BenchRunnable.__init__(self, key, value, operations)
    
    def pre_init(self, j = None):
        tx_init = scalaris.Transaction(conn = _getConnection())
        tx_init.write(self._key, 0)
        tx_init.commit()
        tx_init.close_connection()

    def operation2(self, tx, j):
        reqs = tx.new_req_list()
        reqs.add_add_on_nr(self._key, 1).add_commit()
        # value_old = tx.read(self._key)
        # reqs.add_write(key, value_old + 1).add_commit()
        results = tx.req_list(reqs)
        # tx.process_result_write(results[0])
        tx.process_result_add_on_nr(results[0])

class TransIncrementBench1(TransIncrementBench):
    """
    Performs a benchmark writing integer numbers on a single key and
    increasing them using a new Transaction for each test.
    """
    def __init__(self, key, value, operations):
        TransIncrementBench.__init__(self, key, value, operations)

    def operation(self, j):
        tx = scalaris.Transaction(conn = _getConnection())
        self.operation2(tx, j)
        tx.close_connection()

class TransIncrementBench2(TransIncrementBench):
    """
    Performs a benchmark writing integer numbers on a single key and
    increasing them using a new Transaction but re-using a single
    connection for each test.
    """
    def __init__(self, key, value, operations):
        TransIncrementBench.__init__(self, key, value, operations)
    
    def init(self):
        self._connection = _getConnection()
    
    def cleanup(self):
        self._connection.close()

    def operation(self, j):
        tx = scalaris.Transaction(conn = self._connection)
        self.operation2(tx, j)

class TransIncrementBench3(TransIncrementBench):
    """
    Performs a benchmark writing objects using a single Transaction
    object for all tests.
    """
    def __init__(self, key, value, operations):
        TransIncrementBench.__init__(self, key, value, operations)
    
    def init(self):
        self._tx = scalaris.Transaction(conn = _getConnection())
    
    def cleanup(self):
        self._tx.close_connection()

    def operation(self, j):
        self.operation2(self._tx, j)

class TransReadXWriteXBench(BenchRunnable):
    """
    Performs a benchmark reading X values and overwriting them afterwards
    inside a transaction.
    Provides convenience methods for the full read-x, write-x benchmark
    implementations.
    """
    def __init__(self, key, value, nr_keys, operations):
        BenchRunnable.__init__(self, key, value, operations)
        self._keys = []
        self._value_write = []
        for i in xrange(nr_keys):
            self._keys.append(key + "_" + str(i))
            self._value_write.append(_getRandom(_BENCH_DATA_SIZE, type(value).__name__))
    
    def pre_init(self, j = None):
        value_init = []
        for i in xrange(len(self._keys)):
            value_init.append(_getRandom(_BENCH_DATA_SIZE, type(self._value).__name__))
        
        tx_init = scalaris.Transaction(conn = _getConnection())
        reqs = tx_init.new_req_list()
        for i in xrange(len(self._keys)):
            reqs.add_write(self._keys[i], value_init[i])
        reqs.add_commit()
        results = tx_init.req_list(reqs)
        for i in xrange(len(self._keys)):
            tx_init.process_result_write(results[i])
        tx_init.close_connection()

    def operation2(self, tx, j):
        reqs = tx.new_req_list()
        # read old values into the transaction
        for i in xrange(len(self._keys)):
            reqs.add_read(self._keys[i])
        reqs.add_commit()
        results = tx.req_list(reqs)
        for i in xrange(len(self._keys)):
            tx.process_result_read(results[i])
        
        # write new values... 
        reqs = tx.new_req_list()
        for i in xrange(len(self._keys)):
            value = self._value_write[j % len(self._value_write)]
            reqs.add_write(self._keys[i], value)
        reqs.add_commit()
        results = tx.req_list(reqs)
        for i in xrange(len(self._keys)):
            tx.process_result_write(results[i])

class TransRead5Write5Bench1(TransReadXWriteXBench):
    """
    Performs a benchmark reading 5 values and overwriting them afterwards
    inside a transaction using a new Transaction for each test.
    """
    def __init__(self, key, value, operations):
        TransReadXWriteXBench.__init__(self, key, value, 5, operations)

    def operation(self, j):
        tx = scalaris.Transaction(conn = _getConnection())
        self.operation2(tx, j)
        tx.close_connection()

class TransRead5Write5Bench2(TransReadXWriteXBench):
    """
    Performs a benchmark reading 5 values and overwriting them afterwards
    inside a transaction using a new Transaction but re-using a single
    connection for each test.
    """
    def __init__(self, key, value, operations):
        TransReadXWriteXBench.__init__(self, key, value, 5, operations)
    
    def init(self):
        self._connection = _getConnection()
    
    def cleanup(self):
        self._connection.close()

    def operation(self, j):
        tx = scalaris.Transaction(conn = self._connection)
        self.operation2(tx, j)

class TransRead5Write5Bench3(TransReadXWriteXBench):
    """
    Performs a benchmark reading 5 values and overwriting them afterwards
    inside a transaction using a single Transaction object for all tests.
    """
    def __init__(self, key, value, operations):
        TransReadXWriteXBench.__init__(self, key, value, 5, operations)
    
    def init(self):
        self._tx = scalaris.Transaction(conn = _getConnection())
    
    def cleanup(self):
        self._tx.close_connection()

    def operation(self, j):
        self.operation2(self._tx, j)

class TransAppendToListBench(BenchRunnable):
    """
    Performs a benchmark adding values to a list inside a transaction.
    Provides convenience methods for the full append-to-list benchmark
    implementations.
    """
    def __init__(self, key, value, nr_keys, operations):
        BenchRunnable.__init__(self, key, value, operations)
        self._value_init = []
        for _i in xrange(nr_keys):
            self._value_init.append(_getRandom(_BENCH_DATA_SIZE, 'string'))
    
    def pre_init(self, j = None):
        if j is None:
            return
        tx_init = scalaris.Transaction(conn = _getConnection())
        reqs = tx_init.new_req_list()
        reqs.add_write(self._key + '_' + str(j), self._value_init).add_commit()
        results = tx_init.req_list(reqs)
        tx_init.process_result_write(results[0])
        tx_init.close_connection()

    def operation2(self, tx, j):
        reqs = tx.new_req_list()
        reqs.add_add_del_on_list(self._key + '_' + str(j), [self._value], []).add_commit()
        # read old list into the transaction
        # list = scalaris.str_to_list(tx.read(self._key + '_' + str(j)))
        # write new list ...
        # list.append(self._value)
        # reqs.add_write(self._key + '_' + str(j), list).add_commit())
        
        results = tx.req_list(reqs)
        # tx.process_result_write(results[0])
        tx.process_result_add_del_on_list(results[0])

class TransAppendToListBench1(TransAppendToListBench):
    """
    Performs a benchmark adding values to a list inside a transaction
    using a new Transaction for each test.
    """
    def __init__(self, key, value, operations):
        TransAppendToListBench.__init__(self, key, value, 5, operations)

    def operation(self, j):
        tx = scalaris.Transaction(conn = _getConnection())
        self.operation2(tx, j)
        tx.close_connection()

class TransAppendToListBench2(TransAppendToListBench):
    """
    Performs a benchmark adding values to a list inside a transaction using a
    new Transaction but re-using a single connection for each test.
    """
    def __init__(self, key, value, operations):
        TransAppendToListBench.__init__(self, key, value, 5, operations)
    
    def init(self):
        self._connection = _getConnection()
    
    def cleanup(self):
        self._connection.close()

    def operation(self, j):
        tx = scalaris.Transaction(conn = self._connection)
        self.operation2(tx, j)

class TransAppendToListBench3(TransAppendToListBench):
    """
    Performs a benchmark adding values to a list inside a transaction using a
    single Transaction object for all tests.
    """
    def __init__(self, key, value, operations):
        TransAppendToListBench.__init__(self, key, value, 5, operations)
    
    def init(self):
        self._tx = scalaris.Transaction(conn = _getConnection())
    
    def cleanup(self):
        self._tx.close_connection()

    def operation(self, j):
        self.operation2(self._tx, j)

def _getCurrentMillis():
    """
    Gets the number of milliseconds since epoch.
    """
    now = datetime.now()
    return int(time.mktime(now.timetuple())) * 1000 + (now.microsecond // 1000)

def _testBegin():
    """
    Call this method when a benchmark is started.
    Sets the time the benchmark was started.
    """
    global _timeAtStart
    _timeAtStart = _getCurrentMillis()

def _testEnd(testruns):
    """
    Call this method when a benchmark is finished.
    Calculates the time the benchmark took and the number of transactions
    performed during this time.
    Returns the number of achieved transactions per second.
    """
    global _timeAtStart
    timeTaken = _getCurrentMillis() - _timeAtStart
    speed = (testruns * 1000) // timeTaken
    return speed

def _getConnection():
    url = random.choice(DEFAULT_URLS)
    return scalaris.JSONConnection(url = url)

def _getResultArray(rows, columns):
    """
    Returns a pre-initialized results array with values <tt>-1</tt>.
    """
    results = {}
    for row in rows:
        results[row] = {}
        for column in columns:
            results[row][column] = -1
    return results

def _getRandom(size, mytype):
    """
    Creates an random string or binary object from <size> random characters/bytes.
    """
    if mytype == 'int':
        return random.randint(0, 2147483647)
    elif mytype == 'string' or mytype == 'str':
        return ''.join(random.choice(string.printable) for _x in xrange(size))
    elif mytype == 'binary':
        return bytearray(random.randrange(0, 256) for _x in xrange(size))

def _integrateResults(results, i, worker, failed):
    """
    Integrates the workers' results into the result array.
    """
    try:
        for bench_thread in worker:
            if failed >= 3:
                bench_thread.shouldStop()
                try:
                    while(bench_thread.isAlive()): # non-blocking join so we are able to receive CTRL-C
                        bench_thread.join(1)
                except RuntimeError:
                    pass
            else:
                try:
                    while(bench_thread.isAlive()): # non-blocking join so we are able to receive CTRL-C
                        bench_thread.join(1)
                    speed = bench_thread.getSpeed()
                except RuntimeError:
                    speed = -1
                
                if speed < 0:
                    failed += 1
                else:
                    results[i] += speed
        return failed
    except KeyboardInterrupt:
        print 'CTRL-C received, aborting...'
        for bench_thread in worker:
            bench_thread.shouldStop()
        sys.exit(1)
    
def _getAvgSpeed(results):
    """
    Calculates the average number of transactions per second from the results
    of executing 10 transactions per test run. Will remove the top and bottom
    _PERCENT_TO_REMOVE percent of the sorted results array.
    Returns the average number of transactions per second.
    """
    results.sort()
    toRemove = int((len(results) * _PERCENT_TO_REMOVE) // 100);
    avgSpeed = 0;
    for i in xrange(toRemove, (len(results) - toRemove)):
        avgSpeed += results[i]
    
    avgSpeed //= len(results) - 2 * toRemove
    return avgSpeed

def _runBenchAndPrintResults(benchmarks, results, columns, rows, test_types,
                             test_types_str, test_bench, test_group,
                             first_bench_id, operations, parallel_runs):
    """
    Runs the given benchmarks and prints a results table.
    """
    # assume non-empty results dict:
    for test in xrange(len(results) * len(results[list(results.keys())[0]])):
        try:
            i = test % len(results);
            j = test // len(results);
            if (test + first_bench_id) in benchmarks:
                results[rows[i]][columns[j]] = _runBench(operations,
                        _getRandom(_BENCH_DATA_SIZE, test_types[j]),
                        test_group + "_" + test_types_str[j] + "_" + str(i + 1),
                        test_bench[i], parallel_runs)
                time.sleep(1)
            else:
                results[rows[i]][columns[j]] = -2
        except Exception: # do not catch SystemExit
            _printException()
    
    _printResults(columns, rows, results, operations, parallel_runs)

def _runBench(operations, value, name, clazz, parallel_runs):
    """
    Runs the given benchmark.
    """
    key = str(_benchTime) + name
    results = [-1]*_TESTRUNS

    for i in xrange(_TESTRUNS):
        worker = []
        for thread in xrange(parallel_runs):
            new_worker = clazz(key + '_' + str(i) + '_' + str(thread), value, operations)
            worker.append(new_worker) 
            new_worker.start()
        failed = 0
        failed = _integrateResults(results, i, worker, failed);
        if failed >= 3:
            return -1

    return _getAvgSpeed(results)

def _printResults(columns, rows, results, operations, parallel_runs):
    """
    Prints a result table.
    """
    print 'Concurrent threads: ' + str(parallel_runs) + ', each using ' + str(operations) + ' transactions'
    colLen = 25
    emptyFirstColumn = ''.join([' ']*colLen)
    print emptyFirstColumn + '\tspeed (transactions / second)'
    print emptyFirstColumn, 
    i = 1
    for column in columns:
        print '\t(' + str(i) + ')', 
        i += 1
    print ''
    for row in rows:
        print row + ''.join([' ']*(colLen - len(row))),
        for column in columns:
            value = results[row][column]
            if (value == -2):
                print '\tn/a', 
            elif (value == -1):
                print '\tfailed', 
            else:
                print '\t' + str(int(value)),
        print ''
    
    i = 1
    for column in columns:
        print '(' + str(i) + ') ' + column 
        i += 1
    sys.stdout.flush()

def _printException():
    mytype, message, trace = sys.exc_info()
    print str(mytype) + str(message)
    traceback.print_tb(trace)

def run_from_cmd(argv):
    nr_operations = 500
    threads_per_node = 10
    allBenchs = False
    if (len(argv) == 1):
        allBenchs = True
    elif (len(argv) == 2):
        allBenchs = True
        nr_operations = int(argv[1])
    elif (len(argv) == 3):
        allBenchs = True
        nr_operations = int(argv[1])
        threads_per_node = int(argv[2])
    elif (len(argv) >= 4):
        nr_operations = int(argv[1])
        threads_per_node = int(argv[2])
        benchmarks = []
        for i in xrange(3, len(argv)):
            if argv[i] == 'all':
                allBenchs = True
            else:
                benchmarks.append(int(argv[i]))
    if allBenchs:
        benchmarks = xrange(1, 19, 1)
    minibench(nr_operations, threads_per_node, benchmarks)

if __name__ == "__main__":
    run_from_cmd(sys.argv)
