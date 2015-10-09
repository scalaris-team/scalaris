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

from scalaris import TransactionSingleOp, Transaction, ReplicatedDHT, ScalarisVM,\
    JSONConnection
import scalaris
import time, threading, json, socket
from datetime import datetime
from BaseHTTPServer import HTTPServer, BaseHTTPRequestHandler
from random import shuffle
import unittest

_TEST_DATA = [
             "ahz2ieSh", "wooPhu8u", "quai9ooK", "Oquae4ee", "Airier1a", "Boh3ohv5", "ahD3Saog", "EM5ooc4i", 
             "Epahrai8", "laVahta7", "phoo6Ahj", "Igh9eepa", "aCh4Lah6", "ooT0ath5", "uuzau4Ie", "Iup6mae6", 
#             "xie7iSie", "ail8yeeP", "ooZ4eesi", "Ahn7ohph", "Ohy5moo6", "xooSh9Oo", "ieb6eeS7", "Thooqu9h", 
#             "eideeC9u", "phois3Ie", "EimaiJ2p", "sha6ahR1", "Pheih3za", "bai4eeXe", "rai0aB7j", "xahXoox6", 
#             "Xah4Okeg", "cieG8Yae", "Pe9Ohwoo", "Eehig6ph", "Xe7rooy6", "waY2iifu", "kemi8AhY", "Che7ain8", 
#             "ohw6seiY", "aegh1oBa", "thoh9IeG", "Kee0xuwu", "Gohng8ee", "thoh9Chi", "aa4ahQuu", "Iesh5uge", 
#             "Ahzeil8n", "ieyep5Oh", "xah3IXee", "Eefa5qui", "kai8Muuf", "seeCe0mu", "cooqua5Y", "Ci3ahF6z", 
#             "ot0xaiNu", "aewael8K", "aev3feeM", "Fei7ua5t", "aeCa6oph", "ag2Aelei", "Shah1Pho", "ePhieb0N", 
#             "Uqu7Phup", "ahBi8voh", "oon3aeQu", "Koopa0nu", "xi0quohT", "Oog4aiph", "Aip2ag5D", "tirai7Ae", 
#             "gi0yoePh", "uay7yeeX", "aeb6ahC1", "OoJeic2a", "ieViom1y", "di0eeLai", "Taec2phe", "ID2cheiD", 
#             "oi6ahR5M", "quaiGi8W", "ne1ohLuJ", "DeD0eeng", "yah8Ahng", "ohCee2ie", "ecu1aDai", "oJeijah4", 
#             "Goo9Una1", "Aiph3Phi", "Ieph0ce5", "ooL6cae7", "nai0io1H", "Oop2ahn8", "ifaxae7O", "NeHai1ae", 
#             "Ao8ooj6a", "hi9EiPhi", "aeTh9eiP", "ao8cheiH", "Yieg3sha", "mah7cu2D", "Uo5wiegi", "Oowei0ya", 
#             "efeiDee7", "Oliese6y", "eiSh1hoh", "Joh6hoh9", "zib6Ooqu", "eejiJie4", "lahZ3aeg", "keiRai1d", 
#             "Fei0aewe", "aeS8aboh", "hae3ohKe", "Een9ohQu", "AiYeeh7o", "Yaihah4s", "ood4Giez", "Oumai7te", 
#             "hae2kahY", "afieGh4v", "Ush0boo0", "Ekootee5", "Ya8iz6Ie", "Poh6dich", "Eirae4Ah", "pai8Eeme", 
#             "uNah7dae", "yo3hahCh", "teiTh7yo", "zoMa5Cuv", "ThiQu5ax", "eChi5caa", "ii9ujoiV", "ge7Iekui",
             "sai2aiTa", "ohKi9rie", "ei2ioChu", "aaNgah9y", "ooJai1Ie", "shoh0oH9", "Ool4Ahya", "poh0IeYa", 
             "Uquoo0Il", "eiGh4Oop", "ooMa0ufe", "zee6Zooc", "ohhao4Ah", "Uweekek5", "aePoos9I", "eiJ9noor", 
             "phoong1E", "ianieL2h", "An7ohs4T", "Eiwoeku3", "sheiS3ao", "nei5Thiw", "uL5iewai", "ohFoh9Ae"]

_TOO_LARGE_REQUEST_SIZE = 1024*1024*10 # number of bytes

class TestTransactionSingleOp(unittest.TestCase):
    def setUp(self):
        # The time when the test suite was started.
        now = datetime.now()
        # This is used to create different erlang keys for each run.
        self._testTime = int(time.mktime(now.timetuple()) * 1000 + (now.microsecond / 1000.0))

    # Test method for TransactionSingleOp()
    def testTransactionSingleOp1(self):
        conn = TransactionSingleOp()
        conn.close_connection()

    # Test method for TransactionSingleOp(conn)
    def testTransactionSingleOp2(self):
        conn = TransactionSingleOp(conn = scalaris.JSONConnection(url = scalaris.DEFAULT_URL))
        conn.close_connection()

    # Test method for TransactionSingleOp.close_connection() trying to close the connection twice.
    def testDoubleClose(self):
        conn = TransactionSingleOp()
        conn.close_connection()
        conn.close_connection()

    # Test method for TransactionSingleOp.read(key)
    def testRead_NotFound(self):
        key = "_Read_NotFound"
        conn = TransactionSingleOp()
        self.assertRaises(scalaris.NotFoundError, conn.read, str(self._testTime) + key)
        conn.close_connection()

    # Test method for TransactionSingleOp.read(key) with a closed connection.
    def testRead_NotConnected(self):
        key = "_Read_NotConnected"
        conn = TransactionSingleOp()
        conn.close_connection()
        #self.assertRaises(scalaris.ConnectionError, conn.read, str(self._testTime) + key)
        self.assertRaises(scalaris.NotFoundError, conn.read, str(self._testTime) + key)
        conn.close_connection()

    # Test method for TransactionSingleOp.write(key, value=str()) with a closed connection.
    def testWriteString_NotConnected(self):
        key = "_WriteString_NotConnected"
        conn = TransactionSingleOp()
        conn.close_connection()
        #self.assertRaises(scalaris.ConnectionError, conn.write, str(self._testTime) + key, _TEST_DATA[0])
        conn.write(str(self._testTime) + key, _TEST_DATA[0])
        conn.close_connection()

    # Test method for TransactionSingleOp.write(key, value=str()) and TransactionSingleOp.read(key).
    # Writes strings and uses a distinct key for each value. Tries to read the data afterwards.
    def testWriteString1(self):
        key = "_WriteString1_"
        conn = TransactionSingleOp()
        
        for i in xrange(len(_TEST_DATA)):
            conn.write(str(self._testTime) + key + str(i), _TEST_DATA[i])
        
        # now try to read the data:
        for i in xrange(len(_TEST_DATA)):
            actual = conn.read(str(self._testTime) + key + str(i))
            self.assertEqual(actual, _TEST_DATA[i])
        
        conn.close_connection()

    # Test method for TransactionSingleOp.write(key, value=str()) and TransactionSingleOp.read(key).
    # Writes strings and uses a single key for all the values. Tries to read the data afterwards.
    def testWriteString2(self):
        key = "_WriteString2"
        conn = TransactionSingleOp()
        
        for i in xrange(len(_TEST_DATA)):
            conn.write(str(self._testTime) + key, _TEST_DATA[i])
        
        # now try to read the data:
        actual = conn.read(str(self._testTime) + key)
        self.assertEqual(actual, _TEST_DATA[len(_TEST_DATA) - 1])
        conn.close_connection()

    # Test method for TransactionSingleOp.write(key, value=list()) with a closed connection.
    def testWriteList_NotConnected(self):
        key = "_WriteList_NotConnected"
        conn = TransactionSingleOp()
        conn.close_connection()
        #self.assertRaises(scalaris.ConnectionError, conn.write, str(self._testTime) + key, [_TEST_DATA[0], _TEST_DATA[1]])
        conn.write(str(self._testTime) + key, [_TEST_DATA[0], _TEST_DATA[1]])
        conn.close_connection()

    # Test method for TransactionSingleOp.write(key, value=list()) and TransactionSingleOp.read(key).
    # Writes strings and uses a distinct key for each value. Tries to read the data afterwards.
    def testWriteList1(self):
        key = "_WriteList1_"
        conn = TransactionSingleOp()
        
        for i in xrange(0, len(_TEST_DATA) - 1, 2):
            conn.write(str(self._testTime) + key + str(i), [_TEST_DATA[i], _TEST_DATA[i + 1]])
        
        # now try to read the data:
        for i in xrange(0, len(_TEST_DATA), 2):
            actual = conn.read(str(self._testTime) + key + str(i))
            self.assertEqual(actual, [_TEST_DATA[i], _TEST_DATA[i + 1]])
        
        conn.close_connection()

    # Test method for TransactionSingleOp.write(key, value=list()) and TransactionSingleOp.read(key).
    # Writes strings and uses a single key for all the values. Tries to read the data afterwards.
    def testWriteList2(self):
        key = "_WriteList2"
        conn = TransactionSingleOp()
        
        mylist = []
        for i in xrange(0, len(_TEST_DATA) - 1, 2):
            mylist = [_TEST_DATA[i], _TEST_DATA[i + 1]]
            conn.write(str(self._testTime) + key, mylist)
        
        # now try to read the data:
        actual = conn.read(str(self._testTime) + key)
        self.assertEqual(actual, mylist)
        conn.close_connection()

    # Test method for TransactionSingleOp.test_and_set(key, oldvalue=str(), newvalue=str()) with a closed connection.
    def testTestAndSetString_NotConnected(self):
        key = "_TestAndSetString_NotConnected"
        conn = TransactionSingleOp()
        conn.close_connection()
        #self.assertRaises(scalaris.ConnectionError, conn.test_and_set, str(self._testTime) + key, _TEST_DATA[0], _TEST_DATA[1])
        self.assertRaises(scalaris.NotFoundError, conn.test_and_set, str(self._testTime) + key, _TEST_DATA[0], _TEST_DATA[1])
        conn.close_connection()
    
    # Test method for TransactionSingleOp.test_and_set(key, oldvalue=str(), newvalue=str()).
    # Tries test_and_set with a non-existing key.
    def testTestAndSetString_NotFound(self):
        key = "_TestAndSetString_NotFound"
        conn = TransactionSingleOp()
        self.assertRaises(scalaris.NotFoundError, conn.test_and_set, str(self._testTime) + key, _TEST_DATA[0], _TEST_DATA[1])
        conn.close_connection()

    # Test method for TransactionSingleOp.test_and_set(key, oldvalue=str(), newvalue=str()),
    # TransactionSingleOp.read(key) and TransactionSingleOp.write(key, value=str()).
    # Writes a string and tries to overwrite it using test_and_set
    # knowing the correct old value. Tries to read the string afterwards.
    def testTestAndSetString1(self):
        key = "_TestAndSetString1"
        conn = TransactionSingleOp()
        
        # first write all values:
        for i in xrange(0, len(_TEST_DATA) - 1, 2):
            conn.write(str(self._testTime) + key + str(i), _TEST_DATA[i])
        
        # now try to overwrite them using test_and_set:
        for i in xrange(0, len(_TEST_DATA) - 1, 2):
            conn.test_and_set(str(self._testTime) + key + str(i), _TEST_DATA[i], _TEST_DATA[i + 1])
        
        # now try to read the data:
        for i in xrange(0, len(_TEST_DATA), 2):
            actual = conn.read(str(self._testTime) + key + str(i))
            self.assertEqual(actual, _TEST_DATA[i + 1])
        
        conn.close_connection()

    # Test method for TransactionSingleOp.test_and_set(key, oldvalue=str(), newvalue=str()),
    # TransactionSingleOp.read(key) and TransactionSingleOp.write(key, value=str()).
    # Writes a string and tries to overwrite it using test_and_set
    # knowing the wrong old value. Tries to read the string afterwards.
    def testTestAndSetString2(self):
        key = "_TestAndSetString2"
        conn = TransactionSingleOp()
        
        # first write all values:
        for i in xrange(0, len(_TEST_DATA) - 1, 2):
            conn.write(str(self._testTime) + key + str(i), _TEST_DATA[i])
        
        # now try to overwrite them using test_and_set:
        for i in xrange(0, len(_TEST_DATA) - 1, 2):
            try:
                conn.test_and_set(str(self._testTime) + key + str(i), _TEST_DATA[i + 1], "fail")
                self.fail('expected a KeyChangedError')
            except scalaris.KeyChangedError as exception:
                self.assertEqual(exception.old_value, _TEST_DATA[i])
        
        # now try to read the data:
        for i in xrange(0, len(_TEST_DATA), 2):
            actual = conn.read(str(self._testTime) + key + str(i))
            self.assertEqual(actual, _TEST_DATA[i])
        
        conn.close_connection()

    # Test method for TransactionSingleOp.test_and_set(key, oldvalue=str(), newvalue=list()) with a closed connection.
    def testTestAndSetList_NotConnected(self):
        key = "_TestAndSetList_NotConnected"
        conn = TransactionSingleOp()
        conn.close_connection()
        #self.assertRaises(scalaris.ConnectionError, conn.test_and_set, str(self._testTime) + key, "fail", [_TEST_DATA[0], _TEST_DATA[1]])
        self.assertRaises(scalaris.NotFoundError, conn.test_and_set, str(self._testTime) + key, "fail", [_TEST_DATA[0], _TEST_DATA[1]])
        conn.close_connection()
    
    # Test method for TransactionSingleOp.test_and_set(key, oldvalue=str(), newvalue=list()).
    # Tries test_and_set with a non-existing key.
    def testTestAndSetList_NotFound(self):
        key = "_TestAndSetList_NotFound"
        conn = TransactionSingleOp()
        self.assertRaises(scalaris.NotFoundError, conn.test_and_set, str(self._testTime) + key, "fail", [_TEST_DATA[0], _TEST_DATA[1]])
        conn.close_connection()
    
    # Test method for TransactionSingleOp.test_and_set(key, oldvalue=str(), newvalue=list()),
    # TransactionSingleOp.read(key) and TransactionSingleOp.write(key, value=list()).
    # Writes a list and tries to overwrite it using test_and_set
    # knowing the correct old value. Tries to read the string afterwards.
    def testTestAndSetList1(self):
        key = "_TestAndSetList1"
        conn = TransactionSingleOp()
        
        # first write all values:
        for i in xrange(0, len(_TEST_DATA) - 1, 2):
            conn.write(str(self._testTime) + key + str(i), [_TEST_DATA[i], _TEST_DATA[i + 1]])
        
        # now try to overwrite them using test_and_set:
        for i in xrange(0, len(_TEST_DATA) - 1, 2):
            conn.test_and_set(str(self._testTime) + key + str(i), [_TEST_DATA[i], _TEST_DATA[i + 1]], [_TEST_DATA[i + 1], _TEST_DATA[i]])
        
        # now try to read the data:
        for i in xrange(0, len(_TEST_DATA) - 1, 2):
            actual = conn.read(str(self._testTime) + key + str(i))
            self.assertEqual(actual, [_TEST_DATA[i + 1], _TEST_DATA[i]])
        
        conn.close_connection()

    # Test method for TransactionSingleOp.test_and_set(key, oldvalue=str(), newvalue=list()),
    # TransactionSingleOp.read(key) and TransactionSingleOp.write(key, value=list()).
    # Writes a string and tries to overwrite it using test_and_set
    # knowing the wrong old value. Tries to read the string afterwards.
    def testTestAndSetList2(self):
        key = "_TestAndSetList2"
        conn = TransactionSingleOp()
        
        # first write all values:
        for i in xrange(0, len(_TEST_DATA) - 1, 2):
            conn.write(str(self._testTime) + key + str(i), [_TEST_DATA[i], _TEST_DATA[i + 1]])
        
        # now try to overwrite them using test_and_set:
        for i in xrange(0, len(_TEST_DATA) - 1, 2):
            try:
                conn.test_and_set(str(self._testTime) + key + str(i), "fail", 1)
                self.fail('expected a KeyChangedError')
            except scalaris.KeyChangedError as exception:
                self.assertEqual(exception.old_value, [_TEST_DATA[i], _TEST_DATA[i + 1]])
        
        # now try to read the data:
        for i in xrange(0, len(_TEST_DATA) - 1, 2):
            actual = conn.read(str(self._testTime) + key + str(i))
            self.assertEqual(actual, [_TEST_DATA[i], _TEST_DATA[i + 1]])
        
        conn.close_connection()

    # Test method for TransactionSingleOp.req_list(RequestList) with an
    # empty request list.
    def testReqList_Empty(self):
        conn = TransactionSingleOp()
        conn.req_list(conn.new_req_list())
        conn.close_connection()

    # Test method for TransactionSingleOp.req_list(RequestList) with a
    # mixed request list.
    def testReqList1(self):
        key = "_ReqList1_"
        conn = TransactionSingleOp()
        
        readRequests = conn.new_req_list()
        firstWriteRequests = conn.new_req_list()
        writeRequests = conn.new_req_list()
        for i in xrange(0, len(_TEST_DATA)):
            if (i % 2) == 0:
                firstWriteRequests.add_write(str(self._testTime) + key + str(i), "first_" + _TEST_DATA[i])
            writeRequests.add_write(str(self._testTime) + key + str(i), "second_" + _TEST_DATA[i])
            readRequests.add_read(str(self._testTime) + key + str(i))
        
        results = conn.req_list(firstWriteRequests)
        # evaluate the first write results:
        for i in xrange(0, firstWriteRequests.size()):
            conn.process_result_write(results[i])

        results = conn.req_list(readRequests)
        self.assertEqual(readRequests.size(), len(results))
        # now evaluate the read results:
        for i in xrange(0, readRequests.size()):
            if (i % 2) == 0:
                actual = conn.process_result_read(results[i])
                self.assertEqual("first_" + _TEST_DATA[i], actual)
            else:
                try:
                    result = conn.process_result_read(results[i])
                    # a not found exception must be thrown
                    self.fail('expected a NotFoundError, got: ' + str(result))
                except scalaris.NotFoundError:
                    pass

        results = conn.req_list(writeRequests)
        self.assertEqual(writeRequests.size(), len(results))
        # now evaluate the write results:
        for i in xrange(0, writeRequests.size()):
            conn.process_result_write(results[i])

        # once again test reads - now all reads should be successful
        results = conn.req_list(readRequests)
        self.assertEqual(readRequests.size(), len(results))

        # now evaluate the read results:
        for i in xrange(0, readRequests.size()):
            actual = conn.process_result_read(results[i])
            self.assertEqual("second_" + _TEST_DATA[i], actual)
        
        conn.close_connection();

    # Test method for TransactionSingleOp.write(key, value=bytearray()) with a
    # request that is too large.
    def testReqTooLarge(self):
        conn = TransactionSingleOp()
        data = '0' * _TOO_LARGE_REQUEST_SIZE
        key = "_ReqTooLarge"
        try:
            conn.write(str(self._testTime) + key, data)
            self.fail('The write should have failed unless yaws_max_post_data was set larger than ' + str(_TOO_LARGE_REQUEST_SIZE))
        except scalaris.ConnectionError:
            pass
        
        conn.close_connection()

class TestTransaction(unittest.TestCase):
    def setUp(self):
        # The time when the test suite was started.
        now = datetime.now()
        # This is used to create different erlang keys for each run.
        self._testTime = int(time.mktime(now.timetuple()) * 1000 + (now.microsecond / 1000.0))

    # Test method for Transaction()
    def testTransaction1(self):
        t = Transaction()
        t.close_connection()

    # Test method for Transaction(conn)
    def testTransaction3(self):
        t = Transaction(conn = scalaris.JSONConnection(url = scalaris.DEFAULT_URL))
        t.close_connection()

    # Test method for Transaction.close_connection() trying to close the connection twice.
    def testDoubleClose(self):
        t = Transaction()
        t.close_connection()
        t.close_connection()

    # Test method for Transaction.commit() with a closed connection.
    def testCommit_NotConnected(self):
        t = Transaction()
        t.close_connection()
        #self.assertRaises(scalaris.ConnectionError, t.commit)
        t.commit()
        t.close_connection()

    # Test method for Transaction.commit() which commits an empty transaction.
    def testCommit_Empty(self):
        t = Transaction()
        t.commit()
        t.close_connection()

    # Test method for Transaction.abort() with a closed connection.
    def testAbort_NotConnected(self):
        t = Transaction()
        t.close_connection()
        #self.assertRaises(scalaris.ConnectionError, t.abort)
        t.abort()
        t.close_connection()

    # Test method for Transaction.abort() which aborts an empty transaction.
    def testAbort_Empty(self):
        t = Transaction()
        t.abort()
        t.close_connection()

    # Test method for Transaction.read(key)
    def testRead_NotFound(self):
        key = "_Read_NotFound"
        t = Transaction()
        self.assertRaises(scalaris.NotFoundError, t.read, str(self._testTime) + key)
        t.close_connection()

    # Test method for Transaction.read(key) with a closed connection.
    def testRead_NotConnected(self):
        key = "_Read_NotConnected"
        t = Transaction()
        t.close_connection()
        #self.assertRaises(scalaris.ConnectionError, t.read, str(self._testTime) + key)
        self.assertRaises(scalaris.NotFoundError, t.read, str(self._testTime) + key)
        t.close_connection()

    # Test method for Transaction.write(key, value=str()) with a closed connection.
    def testWriteString_NotConnected(self):
        key = "_WriteString_NotConnected"
        t = Transaction()
        t.close_connection()
        #self.assertRaises(scalaris.ConnectionError, t.write, str(self._testTime) + key, _TEST_DATA[0])
        t.write(str(self._testTime) + key, _TEST_DATA[0])
        t.close_connection()

    # Test method for Transaction.read(key) and Transaction.write(key, value=str())
    # which should show that writing a value for a key for which a previous read
    # returned a NotFoundError is possible.
    def testWriteString_NotFound(self):
        key = "_WriteString_notFound"
        t = Transaction()
        notFound = False
        try:
            t.read(str(self._testTime) + key)
        except scalaris.NotFoundError:
            notFound = True
        
        self.assertTrue(notFound)
        t.write(str(self._testTime) + key, _TEST_DATA[0])
        self.assertEqual(t.read(str(self._testTime) + key), _TEST_DATA[0])
        t.close_connection()

    # Test method for Transaction.write(key, value=str()) and Transaction.read(key).
    # Writes strings and uses a distinct key for each value. Tries to read the data afterwards.
    def testWriteString(self):
        key = "_testWriteString1_"
        t = Transaction()
        
        for i in xrange(len(_TEST_DATA)):
            t.write(str(self._testTime) + key + str(i), _TEST_DATA[i])
        
        # now try to read the data:
        for i in xrange(len(_TEST_DATA)):
            actual = t.read(str(self._testTime) + key + str(i))
            self.assertEqual(actual, _TEST_DATA[i])
        
        # commit the transaction and try to read the data with a new one:
        t.commit()
        t.close_connection()
        t = Transaction()
        for i in xrange(len(_TEST_DATA)):
            actual = t.read(str(self._testTime) + key + str(i))
            self.assertEqual(actual, _TEST_DATA[i])
        
        t.close_connection()

    # Test method for Transaction.write(key, value=list()) and Transaction.read(key).
    # Writes a list and uses a distinct key for each value. Tries to read the data afterwards.
    def testWriteList1(self):
        key = "_testWriteList1_"
        t = scalaris.Transaction()
        
        for i in xrange(0, len(_TEST_DATA) - 1, 2):
            t.write(str(self._testTime) + key + str(i), [_TEST_DATA[i], _TEST_DATA[i + 1]])
        
        # now try to read the data:
        for i in xrange(0, len(_TEST_DATA), 2):
            actual = t.read(str(self._testTime) + key + str(i))
            self.assertEqual(actual, [_TEST_DATA[i], _TEST_DATA[i + 1]])
        
        # commit the transaction and try to read the data with a new one:
        t.commit()
        t.close_connection()
        t = Transaction()
        for i in xrange(0, len(_TEST_DATA), 2):
            actual = t.read(str(self._testTime) + key + str(i))
            self.assertEqual(actual, [_TEST_DATA[i], _TEST_DATA[i + 1]])
        
        t.close_connection()

    # Test method for Transaction.req_list(RequestList) with an
    # empty request list.
    def testReqList_Empty(self):
        conn = Transaction()
        conn.req_list(conn.new_req_list())
        conn.close_connection()

    # Test method for Transaction.req_list(RequestList) with a
    # mixed request list.
    def testReqList1(self):
        key = "_ReqList1_"
        conn = Transaction()
        
        readRequests = conn.new_req_list()
        firstWriteRequests = conn.new_req_list()
        writeRequests = conn.new_req_list()
        for i in xrange(0, len(_TEST_DATA)):
            if (i % 2) == 0:
                firstWriteRequests.add_write(str(self._testTime) + key + str(i), _TEST_DATA[i])
            writeRequests.add_write(str(self._testTime) + key + str(i), _TEST_DATA[i])
            readRequests.add_read(str(self._testTime) + key + str(i))
        
        results = conn.req_list(firstWriteRequests)
        # evaluate the first write results:
        for i in xrange(0, firstWriteRequests.size()):
            conn.process_result_write(results[i])

        requests = conn.new_req_list(readRequests).extend(writeRequests).add_commit()
        results = conn.req_list(requests)
        self.assertEqual(requests.size(), len(results))

        # now evaluate the read results:
        for i in xrange(0, readRequests.size()):
            if (i % 2) == 0:
                actual = conn.process_result_read(results[i])
                self.assertEqual(_TEST_DATA[i], actual)
            else:
                try:
                    conn.process_result_read(results[i])
                    # a not found exception must be thrown
                    self.fail('expected a NotFoundError')
                except scalaris.NotFoundError:
                    pass

        # now evaluate the write results:
        for i in xrange(0, writeRequests.size()):
            pos = readRequests.size() + i
            conn.process_result_write(results[pos])

        # once again test reads - now all reads should be successful
        results = conn.req_list(readRequests)
        self.assertEqual(readRequests.size(), len(results))

        # now evaluate the read results:
        for i in xrange(0, readRequests.size()):
            actual = conn.process_result_read(results[i])
            self.assertEqual(_TEST_DATA[i], actual)
        
        conn.close_connection();

    # Test method for Transaction.write(key, value=bytearray()) with a
    # request that is too large.
    def testReqTooLarge(self):
        conn = Transaction()
        data = '0' * _TOO_LARGE_REQUEST_SIZE
        key = "_ReqTooLarge"
        try:
            conn.write(str(self._testTime) + key, data)
            self.fail('The write should have failed unless yaws_max_post_data was set larger than ' + str(_TOO_LARGE_REQUEST_SIZE))
        except scalaris.ConnectionError:
            pass
        
        conn.close_connection()

    # Various tests.
    def testVarious(self):
        self._writeSingleTest("_0:\u0160arplaninac:page_", _TEST_DATA[0])

    # Helper function for single write tests.
    # Writes a strings to some key and tries to read it afterwards.
    def _writeSingleTest(self, key, data):
        t = Transaction()
        
        t.write(str(self._testTime) + key, data)
        # now try to read the data:
        self.assertEqual(t.read(str(self._testTime) + key), data)
        # commit the transaction and try to read the data with a new one:
        t.commit()
        t.close_connection()
        t = Transaction()
        self.assertEqual(t.read(str(self._testTime) + key), data)
        
        t.close_connection()

class TestReplicatedDHT(unittest.TestCase):
    def setUp(self):
        # The time when the test suite was started.
        now = datetime.now()
        # This is used to create different erlang keys for each run.
        self._testTime = int(time.mktime(now.timetuple()) * 1000 + (now.microsecond / 1000.0))

    # Test method for ReplicatedDHT()
    def testReplicatedDHT1(self):
        rdht = ReplicatedDHT()
        rdht.close_connection()

    # Test method for ReplicatedDHT(conn)
    def testReplicatedDHT2(self):
        rdht = ReplicatedDHT(conn = scalaris.JSONConnection(url = scalaris.DEFAULT_URL))
        rdht.close_connection()

    # Test method for ReplicatedDHT.close_connection() trying to close the connection twice.
    def testDoubleClose(self):
        rdht = ReplicatedDHT()
        rdht.close_connection()
        rdht.close_connection()
    
    # Tries to read the value at the given key and fails if this does
    # not fail with a NotFoundError.
    def _checkKeyDoesNotExist(self, key):
        conn = scalaris.TransactionSingleOp()
        try:
            conn.read(key)
            self.fail('the value at ' + key + ' should not exist anymore')
        except scalaris.NotFoundError:
            # nothing to do here
            pass
        conn.close_connection()

    # Test method for ReplicatedDHT.delete(key).
    # Tries to delete some not existing keys.
    def testDelete_notExistingKey(self):
        key = "_Delete_NotExistingKey"
        rdht = ReplicatedDHT()
        rt = scalaris.RoutingTable()
        r = rt.get_replication_factor()
        
        for i in xrange(len(_TEST_DATA)):
            ok = rdht.delete(str(self._testTime) + key + str(i))
            self.assertEqual(ok, 0)
            results = rdht.get_last_delete_result()
            self.assertEqual((results.ok, results.locks_set, results.undefined), (0, 0, r))
            self._checkKeyDoesNotExist(str(self._testTime) + key + str(i))
        
        rdht.close_connection()

    # Test method for ReplicatedDHT.delete(key) and TransactionSingleOp#write(key, value=str()).
    # Inserts some values, tries to delete them afterwards and tries the delete again.
    def testDelete1(self):
        key = "_Delete1"
        c = scalaris.JSONConnection(url = scalaris.DEFAULT_URL)
        rdht = ReplicatedDHT(conn = c)
        sc = scalaris.TransactionSingleOp(conn = c)
        rt = scalaris.RoutingTable(conn = c)
        r = rt.get_replication_factor()
        
        for i in xrange(len(_TEST_DATA)):
            sc.write(str(self._testTime) + key + str(i), _TEST_DATA[i])
        
        # now try to delete the data:
        for i in xrange(len(_TEST_DATA)):
            ok = rdht.delete(str(self._testTime) + key + str(i))
            self.assertEqual(ok, r)
            results = rdht.get_last_delete_result()
            self.assertEqual((results.ok, results.locks_set, results.undefined), (r, 0, 0))
            self._checkKeyDoesNotExist(str(self._testTime) + key + str(i))
            
            # try again (should be successful with 0 deletes)
            ok = rdht.delete(str(self._testTime) + key + str(i))
            self.assertEqual(ok, 0)
            results = rdht.get_last_delete_result()
            self.assertEqual((results.ok, results.locks_set, results.undefined), (0, 0, r))
            self._checkKeyDoesNotExist(str(self._testTime) + key + str(i))
        
        c.close()

    # Test method for ReplicatedDHT.delete(key) and TransactionSingleOp#write(key, value=str()).
    # Inserts some values, tries to delete them afterwards, inserts them again and tries to delete them again (twice).
    def testDelete2(self):
        key = "_Delete2"
        c = scalaris.JSONConnection(url = scalaris.DEFAULT_URL)
        rdht = ReplicatedDHT(conn = c)
        sc = scalaris.TransactionSingleOp(conn = c)
        rt = scalaris.RoutingTable(conn = c)
        r = rt.get_replication_factor()
        
        for i in xrange(len(_TEST_DATA)):
            sc.write(str(self._testTime) + key + str(i), _TEST_DATA[i])
        
        # now try to delete the data:
        for i in xrange(len(_TEST_DATA)):
            ok = rdht.delete(str(self._testTime) + key + str(i))
            self.assertEqual(ok, r)
            results = rdht.get_last_delete_result()
            self.assertEqual((results.ok, results.locks_set, results.undefined), (r, 0, 0))
            self._checkKeyDoesNotExist(str(self._testTime) + key + str(i))
        
        for i in xrange(len(_TEST_DATA)):
            sc.write(str(self._testTime) + key + str(i), _TEST_DATA[i])
        
        # now try to delete the data:
        for i in xrange(len(_TEST_DATA)):
            ok = rdht.delete(str(self._testTime) + key + str(i))
            self.assertEqual(ok, r)
            results = rdht.get_last_delete_result()
            self.assertEqual((results.ok, results.locks_set, results.undefined), (r, 0, 0))
            self._checkKeyDoesNotExist(str(self._testTime) + key + str(i))
            
            # try again (should be successful with 0 deletes)
            ok = rdht.delete(str(self._testTime) + key + str(i))
            self.assertEqual(ok, 0)
            results = rdht.get_last_delete_result()
            self.assertEqual((results.ok, results.locks_set, results.undefined), (0, 0, r))
            self._checkKeyDoesNotExist(str(self._testTime) + key + str(i))
        
        c.close()

class TestScalarisVM(unittest.TestCase):
    def setUp(self):
        # The time when the test suite was started.
        now = datetime.now()
        # This is used to create different erlang keys for each run.
        self._testTime = int(time.mktime(now.timetuple()) * 1000 + (now.microsecond / 1000.0))

    # Test method for ScalarisVM()
    def testScalarisVM1(self):
        rdht = ScalarisVM()
        rdht.close_connection()

    # Test method for ScalarisVM(conn)
    def testScalarisVM2(self):
        rdht = ScalarisVM(conn = scalaris.JSONConnection(url = scalaris.DEFAULT_URL))
        rdht.close_connection()

    # Test method for ScalarisVM.close_connection() trying to close the connection twice.
    def testDoubleClose(self):
        rdht = ScalarisVM()
        rdht.close_connection()
        rdht.close_connection()

    def testGetVersion_NotConnected(self):
        """Test method for ScalarisVM.getVersion() with a closed connection."""
        vm = ScalarisVM()
        vm.close_connection()
        #self.assertRaises(scalaris.ConnectionError, vm.getVersion())
        vm.getVersion()
        vm.close_connection()

    def testGetVersion1(self):
        """Test method for ScalarisVM.getVersion()."""
        vm = ScalarisVM()
        version = vm.getVersion()
        self.assertTrue(isinstance(version, basestring), msg = version)
        self.assertTrue(len(version) > 0)
        vm.close_connection()

    def testGetInfo_NotConnected(self):
        """Test method for ScalarisVM.getInfo() with a closed connection."""
        vm = ScalarisVM()
        vm.close_connection()
        #self.assertRaises(scalaris.ConnectionError, vm.getInfo())
        vm.getInfo()
        vm.close_connection()

    def testGetInfo1(self):
        """Test method for ScalarisVM.getInfo()."""
        vm = ScalarisVM()
        info = vm.getInfo()
        self.assertTrue(isinstance(info.scalarisVersion, basestring), msg = info.scalarisVersion)
        self.assertTrue(len(info.scalarisVersion) > 0, msg = "scalaris_version (" + info.scalarisVersion + ") != \"\"");
        self.assertTrue(isinstance(info.erlangVersion, basestring), msg = info.erlangVersion)
        self.assertTrue(len(info.erlangVersion) > 0, msg = "erlang_version (" + info.erlangVersion + ") != \"\"");
        self.assertTrue(isinstance(info.memTotal, int), msg = info.memTotal)
        self.assertTrue(info.memTotal >= 0, msg = "mem_total (" + str(info.memTotal) + ") >= 0");
        self.assertTrue(isinstance(info.uptime, int), msg = info.uptime)
        self.assertTrue(info.uptime >= 0, msg = "uptime (" + str(info.uptime) + ") >= 0");
        self.assertTrue(isinstance(info.erlangNode, basestring), msg = info.erlangNode)
        self.assertTrue(len(info.erlangNode) > 0, msg = "erlang_node (" + info.erlangNode + ") != \"\"");
        self.assertTrue(isinstance(info.port, int), msg = info.port)
        self.assertTrue(info.port >= 0 and info.port <= 65535, msg = "0 <= port (" + str(info.port) + ") <= 65535");
        self.assertTrue(isinstance(info.yawsPort, int), msg = info.yawsPort)
        self.assertTrue(info.yawsPort >= 0 and info.yawsPort <= 65535, msg = "0 <= yaws_port (" + str(info.yawsPort) + ") <= 65535");
        vm.close_connection()

    def testGetNumberOfNodes_NotConnected(self):
        """Test method for ScalarisVM.getNumberOfNodes() with a closed connection."""
        vm = ScalarisVM()
        vm.close_connection()
        #self.assertRaises(scalaris.ConnectionError, vm.getNumberOfNodes())
        vm.getNumberOfNodes()
        vm.close_connection()

    def testGetNumberOfNodes1(self):
        """Test method for ScalarisVM.getVersion()."""
        vm = ScalarisVM()
        number = vm.getNumberOfNodes()
        self.assertTrue(isinstance(number, int), msg = number)
        self.assertTrue(number >= 0)
        vm.close_connection()

    def testGetNodes_NotConnected(self):
        """Test method for ScalarisVM.getNodes() with a closed connection."""
        vm = ScalarisVM()
        vm.close_connection()
        #self.assertRaises(scalaris.ConnectionError, vm.getNodes())
        vm.getNodes()
        vm.close_connection()

    def testGetNodes1(self):
        """Test method for ScalarisVM.getNodes()."""
        vm = ScalarisVM()
        nodes = vm.getNodes()
        self.assertTrue(isinstance(nodes, list), msg = nodes)
        self.assertTrue(len(nodes) >= 0)
        self.assertEqual(len(nodes), vm.getNumberOfNodes())
        vm.close_connection()

    def testAddNodes_NotConnected(self):
        """Test method for ScalarisVM.addNodes() with a closed connection."""
        vm = ScalarisVM()
        vm.close_connection()
        #self.assertRaises(scalaris.ConnectionError, vm.addNodes(0))
        vm.addNodes(0)
        vm.close_connection()

    def testAddNodes0(self):
        """Test method for ScalarisVM.addNodes(0)."""
        self._testAddNodes(0)

    def testAddNodes1(self):
        """Test method for ScalarisVM.addNodes(1)."""
        self._testAddNodes(1)

    def testAddNodes3(self):
        """Test method for ScalarisVM.addNodes(3)."""
        self._testAddNodes(3)

    def _testAddNodes(self, nodesToAdd):
        """Test method for ScalarisVM.addNodes(nodesToAdd)."""
        vm = ScalarisVM()
        size = vm.getNumberOfNodes();
        (ok, failed) = vm.addNodes(nodesToAdd)
        size = size + nodesToAdd
        self.assertEqual(nodesToAdd, len(ok))
        self.assertEqual(len(failed), 0)
        self.assertEqual(size, vm.getNumberOfNodes())
        nodes = vm.getNodes()
        for name in ok:
            self.assertTrue(name in nodes, str(nodes) + " should contain " + name)
        for name in ok:
            vm.killNode(name)
        size = size - nodesToAdd
        self.assertEqual(size, vm.getNumberOfNodes())
        vm.close_connection()

    def testShutdownNode_NotConnected(self):
        """Test method for ScalarisVM.shutdownNode() with a closed connection."""
        vm = ScalarisVM()
        vm.close_connection()
        #self.assertRaises(scalaris.ConnectionError, vm.shutdownNode("test"))
        vm.shutdownNode("test")
        vm.close_connection()

    def testShutdownNode1(self):
        """Test method for ScalarisVM.shutdownNode()."""
        self._testDeleteNode('shutdown')

    def testKillNode_NotConnected(self):
        """Test method for ScalarisVM.killNode() with a closed connection."""
        vm = ScalarisVM()
        vm.close_connection()
        #self.assertRaises(scalaris.ConnectionError, vm.killNode("test"))
        vm.killNode("test")
        vm.close_connection()

    def testKillNode1(self):
        """Test method for ScalarisVM.killNode()."""
        self._testDeleteNode('kill')

    def _testDeleteNode(self, action):
        """Test method for ScalarisVM.shutdownNode() and ScalarisVM.killNode()."""
        vm = ScalarisVM()
        size = vm.getNumberOfNodes();
        (ok, _failed) = vm.addNodes(1)
        name = ok[0]
        self.assertEqual(size + 1, vm.getNumberOfNodes())
        if action == 'shutdown':
            result = vm.shutdownNode(name)
        elif action == 'kill':
            result = vm.killNode(name)
        self.assertTrue(result)
        self.assertEqual(size, vm.getNumberOfNodes())
        nodes = vm.getNodes()
        self.assertTrue(not name in nodes, str(nodes) + " should not contain " + name)
        vm.close_connection()

    def testShutdownNodes_NotConnected(self):
        """Test method for ScalarisVM.shutdownNodes() with a closed connection."""
        vm = ScalarisVM()
        vm.close_connection()
        #self.assertRaises(scalaris.ConnectionError, vm.shutdownNodes(0))
        vm.shutdownNodes(0)
        vm.close_connection()

    def testShutdownNodes0(self):
        """Test method for ScalarisVM.shutdownNodes(0)."""
        self._testDeleteNodes(0, 'shutdown')

    def testShutdownNodes1(self):
        """Test method for ScalarisVM.shutdownNodes(1)."""
        self._testDeleteNodes(1, 'shutdown')

    def testShutdownNodes3(self):
        """Test method for ScalarisVM.shutdownNodes(3)."""
        self._testDeleteNodes(3, 'shutdown')

    def testKillNodes_NotConnected(self):
        """Test method for ScalarisVM.killNodes() with a closed connection."""
        vm = ScalarisVM()
        vm.close_connection()
        #self.assertRaises(scalaris.ConnectionError, vm.killNodes(0))
        vm.killNodes(0)
        vm.close_connection()

    def testKillNodes0(self):
        """Test method for ScalarisVM.killNodes(0)."""
        self._testDeleteNodes(0, 'kill')

    def testKillNodes1(self):
        """Test method for ScalarisVM.killNodes(1)."""
        self._testDeleteNodes(1, 'kill')

    def testKillNodes3(self):
        """Test method for ScalarisVM.killNodes(3)."""
        self._testDeleteNodes(3, 'kill')

    def _testDeleteNodes(self, nodesToRemove, action):
        """Test method for ScalarisVM.shutdownNode() and ScalarisVM.killNode()."""
        vm = ScalarisVM()
        size = vm.getNumberOfNodes();
        if nodesToRemove >= 1:
            vm.addNodes(nodesToRemove)
            self.assertEqual(size + nodesToRemove, vm.getNumberOfNodes())
        if action == 'shutdown':
            result = vm.shutdownNodes(nodesToRemove)
        elif action == 'kill':
            result = vm.killNodes(nodesToRemove)
        self.assertEqual(nodesToRemove, len(result))
        self.assertEqual(size, vm.getNumberOfNodes())
        nodes = vm.getNodes()
        for name in result:
            self.assertTrue(not name in nodes, str(nodes) + " should not contain " + name)
        vm.close_connection()

    def testShutdownNodesByName_NotConnected(self):
        """Test method for ScalarisVM.shutdownNodesByName() with a closed connection."""
        vm = ScalarisVM()
        vm.close_connection()
        #self.assertRaises(scalaris.ConnectionError, vm.shutdownNodesByName(["test"]))
        vm.shutdownNodesByName(["test"])
        vm.close_connection()

    def testShutdownNodesByName0(self):
        """Test method for ScalarisVM.shutdownNodesByName(0)."""
        self._testDeleteNodes(0, 'shutdown')

    def testShutdownNodesByName1(self):
        """Test method for ScalarisVM.shutdownNodesByName(1)."""
        self._testDeleteNodes(1, 'shutdown')

    def testShutdownNodesByName3(self):
        """Test method for ScalarisVM.shutdownNodesByName(3)."""
        self._testDeleteNodes(3, 'shutdown')

    def testKillNodesByName_NotConnected(self):
        """Test method for ScalarisVM.killNodesByName() with a closed connection."""
        vm = ScalarisVM()
        vm.close_connection()
        #self.assertRaises(scalaris.ConnectionError, vm.killNodesByName(["test"]))
        vm.killNodesByName(["test"])
        vm.close_connection()

    def testKillNodesByName0(self):
        """Test method for ScalarisVM.killNodesByName(0)."""
        self._testDeleteNodes(0, 'kill')

    def testKillNodesByName1(self):
        """Test method for ScalarisVM.killNodesByName(1)."""
        self._testDeleteNodes(1, 'kill')

    def testKillNodesByName3(self):
        """Test method for ScalarisVM.killNodesByName(3)."""
        self._testDeleteNodes(3, 'shutdown')

    def _testDeleteNodesByName(self, nodesToRemove, action):
        """Test method for ScalarisVM.shutdownNode() and ScalarisVM.killNode()."""
        vm = ScalarisVM()
        size = vm.getNumberOfNodes();
        if nodesToRemove >= 1:
            vm.addNodes(nodesToRemove)
            self.assertEqual(size + nodesToRemove, vm.getNumberOfNodes())
        nodes = vm.getNodes()
        shuffle(nodes)
        removedNodes = nodes[:nodesToRemove]
        if action == 'shutdown':
            (ok, not_found) = vm.shutdownNodesByName(removedNodes)
        elif action == 'kill':
            (ok, not_found) = vm.killNodesByName(removedNodes)
        self.assertEqual(nodesToRemove, len(ok))
        self.assertEqual(nodesToRemove, len(not_found))
        list.sort(removedNodes)
        list.sort(ok)
        self.assertEqual(removedNodes, ok)
        self.assertEqual(size, vm.getNumberOfNodes())
        nodes = vm.getNodes()
        for name in ok:
            self.assertTrue(not name in nodes, str(nodes) + " should not contain " + name)
        vm.close_connection()
        
    def testGetOtherVMs_NotConnected(self):
        """Test method for ScalarisVM.getOtherVMs() with a closed connection."""
        vm = ScalarisVM()
        vm.close_connection()
        #self.assertRaises(scalaris.ConnectionError, vm.getOtherVMs(0))
        vm.getOtherVMs(1)
        vm.close_connection()
        
    def testGetOtherVMs1(self):
        """Test method for ScalarisVM.getOtherVMs(1)."""
        self._testGetOtherVMs(1)
        
    def testGetOtherVMs2(self):
        """Test method for ScalarisVM.getOtherVMs(2)."""
        self._testGetOtherVMs(2)
        
    def testGetOtherVMs3(self):
        """Test method for ScalarisVM.getOtherVMs(3)."""
        self._testGetOtherVMs(3)
        
    def _testGetOtherVMs(self, maxVMs):
        """Test method for ScalarisVM.getOtherVMs()."""
        vm = ScalarisVM()
        otherVMs = vm.getOtherVMs(maxVMs)
        self.assertTrue(len(otherVMs) <= maxVMs, "list too long: " + str(otherVMs))
        for otherVMUrl in otherVMs:
            otherVM = ScalarisVM(JSONConnection(otherVMUrl))
            otherVM.getInfo()
            otherVM.close_connection()
        vm.close_connection()
        
    # not tested because we still need the Scalaris Erlang VM:
    def _testShutdownVM_NotConnected(self):
        """Test method for ScalarisVM.shutdownVM() with a closed connection."""
        vm = ScalarisVM()
        vm.close_connection()
        #self.assertRaises(scalaris.ConnectionError, vm.shutdownVM())
        vm.shutdownVM()
        vm.close_connection()
        
    # not tested because we still need the Scalaris Erlang VM:
    def _testShutdownVM1(self):
        """Test method for ScalarisVM.shutdownVM()."""
        vm = ScalarisVM()
        vm.shutdownVM()
        vm.close_connection()

    # not tested because we still need the Scalaris Erlang VM:
    def _testKillVM_NotConnected(self):
        """Test method for ScalarisVM.killVM() with a closed connection."""
        vm = ScalarisVM()
        vm.close_connection()
        #self.assertRaises(scalaris.ConnectionError, vm.killVM())
        vm.killVM()
        vm.close_connection()

    # not tested because we still need the Scalaris Erlang VM:
    def _testKillVM1(self):
        """Test method for ScalarisVM.killVM()."""
        vm = ScalarisVM()
        vm.killVM()
        vm.close_connection()

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
