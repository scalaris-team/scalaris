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

from Scalaris import TransactionSingleOp
import Scalaris
from datetime import datetime
import time
import unittest

# The time when the (whole) test suite was started.
_now = datetime.now()
# This is used to create different erlang keys for each run.
_testTime = int(time.mktime(_now.timetuple()) * 1000 + (_now.microsecond / 1000.0))
_testData = [
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

class TestTransactionSingleOp(unittest.TestCase):
    def setUp(self):
        pass

    # Test method for TransactionSingleOp()
    def testTransactionSingleOp1(self):
        conn = TransactionSingleOp()
        conn.closeConnection()

    # Test method for TransactionSingleOp(url)
    def testTransactionSingleOp2(self):
        conn = TransactionSingleOp(url = Scalaris.default_url)
        conn.closeConnection()

    # Test method for TransactionSingleOp(conn)
    def testTransactionSingleOp3(self):
        conn = TransactionSingleOp(conn = Scalaris.getConnection(Scalaris.default_url))
        conn.closeConnection()

    # Test method for TransactionSingleOp.closeConnection() trying to close the connection twice.
    def testDoubleClose(self):
        conn = TransactionSingleOp()
        conn.closeConnection()
        conn.closeConnection()

    # Test method for TransactionSingleOp.read(key)
    def testRead_NotFound(self):
        key = "_Read_NotFound"
        conn = TransactionSingleOp()
        self.assertRaises(Scalaris.NotFoundException, conn.read, str(_testTime) + key)
        conn.closeConnection()

    # Test method for TransactionSingleOp.read(key) with a closed connection.
    def testRead_NotConnected(self):
        key = "_Read_NotConnected"
        conn = TransactionSingleOp()
        conn.closeConnection()
        #self.assertRaises(Scalaris.ConnectionException, conn.read, str(_testTime) + key)
        self.assertRaises(Scalaris.NotFoundException, conn.read, str(_testTime) + key)
        conn.closeConnection()

    # Test method for TransactionSingleOp.write(key, value=str()) with a closed connection.
    def testWriteString_NotConnected(self):
        key = "_WriteString_NotConnected"
        conn = TransactionSingleOp()
        conn.closeConnection()
        #self.assertRaises(Scalaris.ConnectionException, conn.write, str(_testTime) + key, _testData[0])
        conn.write(str(_testTime) + key, _testData[0])
        conn.closeConnection()

    # Test method for TransactionSingleOp.write(key, value=str()) and TransactionSingleOp.read(key).
    # Writes strings and uses a distinct key for each value. Tries to read the data afterwards.
    def testWriteString1(self):
        key = "_WriteString1_"
        conn = TransactionSingleOp()
        
        for i in xrange(len(_testData)):
            conn.write(str(_testTime) + key + str(i), _testData[i])
        
        # now try to read the data:
        for i in xrange(len(_testData)):
            actual = conn.read(str(_testTime) + key + str(i))
            self.assertEqual(actual, _testData[i])
        
        conn.closeConnection()

    # Test method for TransactionSingleOp.write(key, value=str()) and TransactionSingleOp.read(key).
    # Writes strings and uses a single key for all the values. Tries to read the data afterwards.
    def testWriteString2(self):
        key = "_WriteString2"
        conn = TransactionSingleOp()
        
        for i in xrange(len(_testData)):
            conn.write(str(_testTime) + key, _testData[i])
        
        # now try to read the data:
        actual = conn.read(str(_testTime) + key)
        self.assertEqual(actual, _testData[len(_testData) - 1])
        conn.closeConnection()

    # Test method for TransactionSingleOp.write(key, value=list()) with a closed connection.
    def testWriteList_NotConnected(self):
        key = "_WriteList_NotConnected"
        conn = TransactionSingleOp()
        conn.closeConnection()
        #self.assertRaises(Scalaris.ConnectionException, conn.write, str(_testTime) + key, [_testData[0], _testData[1]])
        conn.write(str(_testTime) + key, [_testData[0], _testData[1]])
        conn.closeConnection()

    # Test method for TransactionSingleOp.write(key, value=list()) and TransactionSingleOp.read(key).
    # Writes strings and uses a distinct key for each value. Tries to read the data afterwards.
    def testWriteList1(self):
        key = "_WriteList1_"
        conn = TransactionSingleOp()
        
        for i in xrange(0, len(_testData) - 1, 2):
            conn.write(str(_testTime) + key + str(i), [_testData[i], _testData[i + 1]])
        
        # now try to read the data:
        for i in xrange(0, len(_testData), 2):
            actual = conn.read(str(_testTime) + key + str(i))
            self.assertEqual(actual, [_testData[i], _testData[i + 1]])
        
        conn.closeConnection()

    # Test method for TransactionSingleOp.write(key, value=list()) and TransactionSingleOp.read(key).
    # Writes strings and uses a single key for all the values. Tries to read the data afterwards.
    def testWriteList2(self):
        key = "_WriteList2"
        conn = TransactionSingleOp()
        
        list = []
        for i in xrange(0, len(_testData) - 1, 2):
            list = [_testData[i], _testData[i + 1]]
            conn.write(str(_testTime) + key, list)
        
        # now try to read the data:
        actual = conn.read(str(_testTime) + key)
        self.assertEqual(actual, list)
        conn.closeConnection()

    # Test method for TransactionSingleOp.testAndSet(key, oldvalue=str(), newvalue=str()) with a closed connection.
    def testTestAndSetString_NotConnected(self):
        key = "_TestAndSetString_NotConnected"
        conn = TransactionSingleOp()
        conn.closeConnection()
        #self.assertRaises(Scalaris.ConnectionException, conn.testAndSet, str(_testTime) + key, _testData[0], _testData[1])
        self.assertRaises(Scalaris.NotFoundException, conn.testAndSet, str(_testTime) + key, _testData[0], _testData[1])
        conn.closeConnection()
    
    # Test method for TransactionSingleOp.testAndSet(key, oldvalue=str(), newvalue=str()).
    # Tries test_and_set with a non-existing key.
    def testTestAndSetString_NotFound(self):
        key = "_TestAndSetString_NotFound"
        conn = TransactionSingleOp()
        self.assertRaises(Scalaris.NotFoundException, conn.testAndSet, str(_testTime) + key, _testData[0], _testData[1])
        conn.closeConnection()

    # Test method for TransactionSingleOp.testAndSet(key, oldvalue=str(), newvalue=str()),
    # TransactionSingleOp.read(key) and TransactionSingleOp.write(key, value=str()).
    # Writes a string and tries to overwrite it using test_and_set
    # knowing the correct old value. Tries to read the string afterwards.
    def testTestAndSetString1(self):
        key = "_TestAndSetString1"
        conn = TransactionSingleOp()
        
        # first write all values:
        for i in xrange(0, len(_testData) - 1, 2):
            conn.write(str(_testTime) + key + str(i), _testData[i])
        
        # now try to overwrite them using test_and_set:
        for i in xrange(0, len(_testData) - 1, 2):
            conn.testAndSet(str(_testTime) + key + str(i), _testData[i], _testData[i + 1])
        
        # now try to read the data:
        for i in xrange(0, len(_testData), 2):
            actual = conn.read(str(_testTime) + key + str(i))
            self.assertEqual(actual, _testData[i + 1])
        
        conn.closeConnection()

    # Test method for TransactionSingleOp.testAndSet(key, oldvalue=str(), newvalue=str()),
    # TransactionSingleOp.read(key) and TransactionSingleOp.write(key, value=str()).
    # Writes a string and tries to overwrite it using test_and_set
    # knowing the wrong old value. Tries to read the string afterwards.
    def testTestAndSetString2(self):
        key = "_TestAndSetString2"
        conn = TransactionSingleOp()
        
        # first write all values:
        for i in xrange(0, len(_testData) - 1, 2):
            conn.write(str(_testTime) + key + str(i), _testData[i])
        
        # now try to overwrite them using test_and_set:
        for i in xrange(0, len(_testData) - 1, 2):
            with self.assertRaises(Scalaris.KeyChangedException) as cm:
                conn.testAndSet(str(_testTime) + key + str(i), _testData[i + 1], "fail")
            self.assertEqual(cm.exception.old_value, _testData[i])
        
        # now try to read the data:
        for i in xrange(0, len(_testData), 2):
            actual = conn.read(str(_testTime) + key + str(i))
            self.assertEqual(actual, _testData[i])
        
        conn.closeConnection()

    # Test method for TransactionSingleOp.testAndSet(key, oldvalue=str(), newvalue=list()) with a closed connection.
    def testTestAndSetList_NotConnected(self):
        key = "_TestAndSetList_NotConnected"
        conn = TransactionSingleOp()
        conn.closeConnection()
        #self.assertRaises(Scalaris.ConnectionException, conn.testAndSet, str(_testTime) + key, "fail", [_testData[0], _testData[1]])
        self.assertRaises(Scalaris.NotFoundException, conn.testAndSet, str(_testTime) + key, "fail", [_testData[0], _testData[1]])
        conn.closeConnection()
    
    # Test method for TransactionSingleOp.testAndSet(key, oldvalue=str(), newvalue=list()).
    # Tries test_and_set with a non-existing key.
    def testTestAndSetList_NotFound(self):
        key = "_TestAndSetList_NotFound"
        conn = TransactionSingleOp()
        self.assertRaises(Scalaris.NotFoundException, conn.testAndSet, str(_testTime) + key, "fail", [_testData[0], _testData[1]])
        conn.closeConnection()
    
    # Test method for TransactionSingleOp.testAndSet(key, oldvalue=str(), newvalue=list()),
    # TransactionSingleOp.read(key) and TransactionSingleOp.write(key, value=list()).
    # Writes a list and tries to overwrite it using test_and_set
    # knowing the correct old value. Tries to read the string afterwards.
    def testTestAndSetList1(self):
        key = "_TestAndSetList1"
        conn = TransactionSingleOp()
        
        # first write all values:
        for i in xrange(0, len(_testData) - 1, 2):
            conn.write(str(_testTime) + key + str(i), [_testData[i], _testData[i + 1]])
        
        # now try to overwrite them using test_and_set:
        for i in xrange(0, len(_testData) - 1, 2):
            conn.testAndSet(str(_testTime) + key + str(i), [_testData[i], _testData[i + 1]], [_testData[i + 1], _testData[i]])
        
        # now try to read the data:
        for i in xrange(0, len(_testData), 2):
            actual = conn.read(str(_testTime) + key + str(i))
            self.assertEqual(actual, [_testData[i + 1], _testData[i]])
        
        conn.closeConnection()

    # Test method for TransactionSingleOp.testAndSet(key, oldvalue=str(), newvalue=list()),
    # TransactionSingleOp.read(key) and TransactionSingleOp.write(key, value=list()).
    # Writes a string and tries to overwrite it using test_and_set
    # knowing the wrong old value. Tries to read the string afterwards.
    def testTestAndSetList2(self):
        key = "_TestAndSetList2"
        conn = TransactionSingleOp()
        
        # first write all values:
        for i in xrange(0, len(_testData) - 1, 2):
            conn.write(str(_testTime) + key + str(i), [_testData[i], _testData[i + 1]])
        
        # now try to overwrite them using test_and_set:
        for i in xrange(0, len(_testData) - 1, 2):
            with self.assertRaises(Scalaris.KeyChangedException) as cm:
                conn.testAndSet(str(_testTime) + key + str(i), "fail", 1)
            self.assertEqual(cm.exception.old_value, [_testData[i], _testData[i + 1]])
        
        # now try to read the data:
        for i in xrange(0, len(_testData), 2):
            actual = conn.read(str(_testTime) + key + str(i))
            self.assertEqual(actual, [_testData[i], _testData[i + 1]])
        
        conn.closeConnection()

if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(TestTransactionSingleOp)
    unittest.TextTestRunner(verbosity=2).run(suite)
