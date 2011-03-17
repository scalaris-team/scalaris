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

import Scalaris
from datetime import datetime
import time
import random,  string
import sys,  traceback

# The size of a single data item that is send to scalaris.
_BENCH_DATA_SIZE = 1000
# The time when the (whole) benchmark suite was started.
_now = datetime.now()
#This is used to create different erlang keys for each run.
_benchTime = int(time.mktime(_now.timetuple()) * 1000 + (_now.microsecond / 1000.0))
# The time at the start of a single benchmark.
_timeAtStart = 0
# Cut 5% off of both ends of the result list.
_percentToRemove = 5
# Number of transactions per test run.
_transactionsPerTestRun = 10;

# Returns a pre-initialized results array with values <tt>-1</tt>.
def _getResultArray(rows,  columns):
    results = {}
    for row in rows:
        results[row] = {}
        for column in columns:
            results[row][column] = -1
    return results

# Creates an random string or binary object from <size> random characters/bytes.
def _getRandom(size,  type):
    if type == 'string':
        return ''.join(random.choice(string.ascii_uppercase + string.digits) for x in range(size))
    elif type == 'binary':
        return bytearray(random.randrange(0, 256) for x in range(size))

# Prints a result table.
def _printResults(columns,  rows,  results,  testruns):
    print 'Test runs: ' + str(testruns) + ', each using ' + str(_transactionsPerTestRun) + ' transactions'
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
            if (value == -1):
                print '\tn/a', 
            else:
                print '\t' + str(value), 
        print ''
    
    i = 1
    for column in columns:
        print '(' + str(i) + ') ' + column 
        i += 1

def _printException():
    type,  message,  trace = sys.exc_info()
    print str(type) + str(message)
    traceback.print_tb(trace)

# Default minimal benchmark.
#
# Tests some strategies for writing key/value pairs to scalaris:
# 1) writing binary objects (random data, size = _BENCH_DATA_SIZE)
# 2) writing string objects (random data, size = _BENCH_DATA_SIZE)
# each <testruns> times
# * first using a new Transaction or TransactionSingleOp for each test,
# * then using a new Transaction or TransactionSingleOp but re-using a single connection,
# * and finally re-using a single Transaction or TransactionSingleOp object.
def minibench(testruns, benchmarks):
    rows = ['separate connection',  're-use connection',  're-use object']
    columns = ['binary', 'string']
    results = _getResultArray(rows, columns)
    
    print 'Benchmark of TransactionSingleOp:'
    
    try:
        if 1 in benchmarks:
            results['separate connection']['binary'] = \
                _transSingleOpBench1(testruns, _getRandom(_BENCH_DATA_SIZE, 'binary'), "transsinglebench_B_1")
            time.sleep(1)
    except:
        # _printException()
        pass
    
    try:
        if 2 in benchmarks:
            results['re-use connection']['binary'] = \
                _transSingleOpBench2(testruns, _getRandom(_BENCH_DATA_SIZE, 'binary'), "transsinglebench_B_2")
            time.sleep(1)
    except:
        # _printException()
        pass
    
    try:
        if 3 in benchmarks:
            results['re-use object']['binary'] = \
                _transSingleOpBench3(testruns, _getRandom(_BENCH_DATA_SIZE, 'binary'), "transsinglebench_B_3")
            time.sleep(1)
    except:
        # _printException()
        pass
    
    try:
        if 4 in benchmarks:
            results['separate connection']['string'] = \
                _transSingleOpBench1(testruns, _getRandom(_BENCH_DATA_SIZE, 'string'), "transsinglebench_S_1")
            time.sleep(1)
    except:
        # _printException()
        pass
    
    try:
        if 5 in benchmarks:
            results['re-use connection']['string'] = \
                _transSingleOpBench2(testruns, _getRandom(_BENCH_DATA_SIZE, 'string'), "transsinglebench_S_2")
            time.sleep(1)
    except:
        # _printException()
        pass
    
    try:
        if 6 in benchmarks:
            results['re-use object']['string'] = \
                _transSingleOpBench3(testruns, _getRandom(_BENCH_DATA_SIZE, 'string'), "transsinglebench_S_3")
            time.sleep(1)
    except:
        # _printException()
        pass
    
    _printResults(columns,  rows,  results,  testruns)
    
    
    results = _getResultArray(rows, columns)
    
    print '-----'
    print 'Benchmark of Transaction:'
    
    try:
        if 1 in benchmarks:
            results['separate connection']['binary'] = \
                _transBench1(testruns, _getRandom(_BENCH_DATA_SIZE, 'binary'), "transbench_B_1")
            time.sleep(1)
    except:
        # _printException()
        pass
    
    try:
        if 2 in benchmarks:
            results['re-use connection']['binary'] = \
                _transBench2(testruns, _getRandom(_BENCH_DATA_SIZE, 'binary'), "transbench_B_2")
            time.sleep(1)
    except:
        # _printException()
        pass
    
    try:
        if 3 in benchmarks:
            results['re-use object']['binary'] = \
                _transBench3(testruns, _getRandom(_BENCH_DATA_SIZE, 'binary'), "transbench_B_3")
            time.sleep(1)
    except:
        # _printException()
        pass
    
    try:
        if 4 in benchmarks:
            results['separate connection']['string'] = \
                _transBench1(testruns, _getRandom(_BENCH_DATA_SIZE, 'string'), "transbench_S_1")
            time.sleep(1)
    except:
        # _printException()
        pass
    
    try:
        if 5 in benchmarks:
            results['re-use connection']['string'] = \
                _transBench2(testruns, _getRandom(_BENCH_DATA_SIZE, 'string'), "transbench_S_2")
            time.sleep(1)
    except:
        # _printException()
        pass
    
    try:
        if 6 in benchmarks:
            results['re-use object']['string'] = \
                _transBench3(testruns, _getRandom(_BENCH_DATA_SIZE, 'string'), "transbench_S_3")
            time.sleep(1)
    except:
        # _printException()
        pass
    
    _printResults(columns,  rows,  results,  testruns)
    
    
    rows = ['separate connection',  're-use connection',  're-use object']
    columns = ['read+write']
    results = _getResultArray(rows, columns)
    
    print '-----'
    print 'Benchmark incrementing an integer key (read+write):'
    
    try:
        if 7 in benchmarks:
            results['separate connection']['read+write'] = \
                _transIncrementBench1(testruns, "transbench_inc_1")
            time.sleep(1)
    except:
        # _printException()
        pass
    
    try:
        if 8 in benchmarks:
            results['re-use connection']['read+write'] = \
                _transIncrementBench2(testruns, "transbench_inc_2")
            time.sleep(1)
    except:
        # _printException()
        pass
    
    try:
        if 9 in benchmarks:
            results['re-use object']['read+write'] = \
                _transIncrementBench3(testruns, "transbench_inc_3")
            time.sleep(1)
    except:
        # _printException()
        pass
    
    _printResults(columns,  rows,  results,  testruns)

# Call this method when a benchmark is started.
# Sets the time the benchmark was started.
def _testBegin():
    global _timeAtStart
    now = datetime.now()
    _timeAtStart = int(time.mktime(now.timetuple())) * 1000 + (now.microsecond / 1000)

# Call this method when a benchmark is finished.
# Calculates the time the benchmark took and the number of transactions
# performed during this time.
# Returns the number of achieved transactions per second.
def _testEnd(testruns):
    global _timeAtStart
    now = datetime.now()
    timeTaken = int(time.mktime(now.timetuple())) * 1000 + (now.microsecond / 1000) - _timeAtStart
    speed = (testruns * 1000) / timeTaken
    return speed

# Calculates the average number of transactions per second from the results
# of executing 10 transactions per test run. Will remove the top and bottom
# _percentToRemove percent of the sorted results array.
# Returns the average number of transactions per second.
def _getAvgSpeed(results):
    results.sort()
    toRemove = int((len(results) * _percentToRemove) / 100);
    avgSpeed = 0;
    for i in range(toRemove,  (len(results) - toRemove)):
        avgSpeed += results[i]
    
    avgSpeed /= len(results) - 2 * toRemove
    return avgSpeed

# Performs a benchmark writing objects using a new TransactionSingleOp object for each test.
# Returns the number of achieved transactions per second.
def _transSingleOpBench1(testruns, value, name):
    key = str(_benchTime) + name
    results = []
    
    for i in range(testruns):
        for retry in range(3):
            try:
                _testBegin()
                
                for j in range(_transactionsPerTestRun):
                    tx = Scalaris.TransactionSingleOp()
                    tx.write(key + str(i) + str(j),  value)
                    tx.closeConnection()
                
                results.append(_testEnd(_transactionsPerTestRun))
                break
            except:
                # _printException()
                if (retry == 2):
                    return -1
    return _getAvgSpeed(results)

# Performs a benchmark writing objects using a new TransactionSingleOp but
# re-using a single connection for each test.
# Returns the number of achieved transactions per second.
def _transSingleOpBench2(testruns, value, name):
    key = str(_benchTime) + name
    results = []
    
    for i in range(testruns):
        for retry in range(3):
            try:
                _testBegin()
                conn = Scalaris.getConnection(Scalaris.default_url)
                
                for j in range(_transactionsPerTestRun):
                    tx = Scalaris.TransactionSingleOp(Scalaris.default_url,  conn)
                    tx.write(key + str(i) + str(j),  value)
                
                conn.close()
                results.append(_testEnd(_transactionsPerTestRun))
                break
            except:
                # _printException()
                if (retry == 2):
                    return -1
    return _getAvgSpeed(results)

# Performs a benchmark writing objects using a single TransactionSingleOp
# object for all tests.
# Returns the number of achieved transactions per second.
def _transSingleOpBench3(testruns, value, name):
    key = str(_benchTime) + name
    results = []
    
    for i in range(testruns):
        for retry in range(3):
            try:
                _testBegin()
                tx = Scalaris.TransactionSingleOp()
                
                for j in range(_transactionsPerTestRun):
                    tx.write(key + str(i) + str(j),  value)
                
                tx.closeConnection()
                results.append(_testEnd(_transactionsPerTestRun))
                break
            except:
                # _printException()
                if (retry == 2):
                    return -1
    return _getAvgSpeed(results)

# Performs a benchmark writing objects using a new Transaction for each test.
# Returns the number of achieved transactions per second.
def _transBench1(testruns, value, name):
    key = str(_benchTime) + name
    results = []
    
    for i in range(testruns):
        for retry in range(3):
            try:
                _testBegin()
                
                for j in range(_transactionsPerTestRun):
                    tx = Scalaris.Transaction()
                    tx.write(key + str(i) + str(j),  value)
                    tx.commit()
                    tx.closeConnection()
                
                results.append(_testEnd(_transactionsPerTestRun))
                break
            except:
                # _printException()
                if (retry == 2):
                    return -1
    return _getAvgSpeed(results)

# Performs a benchmark writing objects using a new Transaction but re-using a
# single connection for each test.
# Returns the number of achieved transactions per second.
def _transBench2(testruns, value, name):
    key = str(_benchTime) + name
    results = []
    
    for i in range(testruns):
        for retry in range(3):
            try:
                _testBegin()
                conn = Scalaris.getConnection(Scalaris.default_url)
                
                for j in range(_transactionsPerTestRun):
                    tx = Scalaris.Transaction(Scalaris.default_url,  conn)
                    tx.write(key + str(i) + str(j),  value)
                    tx.commit()
                
                conn.close()
                results.append(_testEnd(_transactionsPerTestRun))
                break
            except:
                # _printException()
                if (retry == 2):
                    return -1
    return _getAvgSpeed(results)

# Performs a benchmark writing objects using a single Transaction object
# for all tests.
# Returns the number of achieved transactions per second.
def _transBench3(testruns, value, name):
    key = str(_benchTime) + name
    results = []
    
    for i in range(testruns):
        for retry in range(3):
            try:
                _testBegin()
                tx = Scalaris.Transaction()
                
                for j in range(_transactionsPerTestRun):
                    tx.write(key + str(i) + str(j),  value)
                    tx.commit()
                
                tx.closeConnection()
                results.append(_testEnd(_transactionsPerTestRun))
                break
            except:
                # _printException()
                if (retry == 2):
                    return -1
    return _getAvgSpeed(results)

# Performs a benchmark writing integer numbers on a single key and
# increasing them using a new Transaction for each test.
# Returns the number of achieved transactions per second.
def _transIncrementBench1(testruns, name):
    key = str(_benchTime) + name
    results = []
    
    for i in range(testruns):
        for retry in range(3):
            try:
                key_i = key + str(i)
                tx_init = Scalaris.Transaction()
                tx_init.write(key_i, 0);
                tx_init.commit();
                tx_init.closeConnection();
                _testBegin()
                
                for j in range(_transactionsPerTestRun):
                    tx = Scalaris.Transaction()
                    value_old = tx.read(key_i)
                    tx.write(key_i, value_old + 1)
                    tx.commit();
                    tx.closeConnection();
                
                results.append(_testEnd(_transactionsPerTestRun))
                break
            except:
                # _printException()
                if (retry == 2):
                    return -1
    return _getAvgSpeed(results)

# Performs a benchmark writing integer numbers on a single key and
# increasing them using a new Transaction but re-using a single
# connection for each test.
# Returns the number of achieved transactions per second.
def _transIncrementBench2(testruns, name):
    key = str(_benchTime) + name
    results = []
    
    for i in range(testruns):
        for retry in range(3):
            try:
                key_i = key + str(i)
                tx_init = Scalaris.Transaction()
                tx_init.write(key_i, 0);
                tx_init.commit();
                tx_init.closeConnection();
                _testBegin()
                conn = Scalaris.getConnection(Scalaris.default_url)
                
                for j in range(_transactionsPerTestRun):
                    tx = Scalaris.Transaction(Scalaris.default_url,  conn)
                    value_old = tx.read(key_i)
                    tx.write(key_i, value_old + 1)
                    tx.commit();
                
                conn.close()
                results.append(_testEnd(_transactionsPerTestRun))
                break
            except:
                # _printException()
                if (retry == 2):
                    return -1
    return _getAvgSpeed(results)

# Performs a benchmark writing objects using a single Transaction
# object for all tests.
# Returns the number of achieved transactions per second.
def _transIncrementBench3(testruns, name):
    key = str(_benchTime) + name
    results = []
    
    for i in range(testruns):
        for retry in range(3):
            try:
                key_i = key + str(i)
                tx_init = Scalaris.Transaction()
                tx_init.write(key_i, 0);
                tx_init.commit();
                tx_init.closeConnection();
                _testBegin()
                tx = Scalaris.Transaction()
                
                for j in range(_transactionsPerTestRun):
                    value_old = tx.read(key_i)
                    tx.write(key_i, value_old + 1)
                    tx.commit();
                    
                tx.closeConnection();
                results.append(_testEnd(_transactionsPerTestRun))
                break
            except:
                # _printException()
                if (retry == 2):
                    return -1
    return _getAvgSpeed(results)

if __name__ == "__main__":
    if (len(sys.argv) == 1):
        minibench(100,  range(1,  10,  1))
    elif (len(sys.argv) >= 3):
        testruns = int(sys.argv[1])
        benchmarks = []
        for i in range(2,  min(11,  len(sys.argv))):
            if sys.argv[i] == 'all':
                benchmarks = range(1,  10,  1)
            else:
                benchmarks.append(int(sys.argv[i]))
        minibench(testruns,  benchmarks)
