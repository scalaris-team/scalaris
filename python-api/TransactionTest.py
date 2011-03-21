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

from Scalaris import Transaction
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

class TestTransaction(unittest.TestCase):
    def setUp(self):
        pass

    # Test method for Transaction()
    def testTransaction1(self):
        t = Transaction()
        t.closeConnection()

    # Test method for Transaction(url, conn)
    def testTransaction2(self):
        t = Transaction(Scalaris.default_url, Scalaris.getConnection(Scalaris.default_url))
        t.closeConnection()

    # Test method for Transaction.closeConnection() trying to close the connection twice.
    def testDoubleClose(self):
        t = Transaction()
        t.closeConnection()
        t.closeConnection()

    # Test method for Transaction.commit() with a closed connection.
    def testCommit_NotConnected(self):
        t = Transaction()
        t.closeConnection()
        #self.assertRaises(Scalaris.ConnectionException, t.commit)
        t.commit()
        t.closeConnection()

    # Test method for Transaction.commit() which commits an empty transaction.
    def testCommit_Empty(self):
        t = Transaction()
        t.commit()
        t.closeConnection()

    # Test method for Transaction.abort() with a closed connection.
    def testAbort_NotConnected(self):
        t = Transaction()
        t.closeConnection()
        #self.assertRaises(Scalaris.ConnectionException, t.abort)
        t.abort()
        t.closeConnection()

    # Test method for Transaction.abort() which aborts an empty transaction.
    def testAbort_Empty(self):
        t = Transaction()
        t.abort()
        t.closeConnection()

    # Test method for Transaction.read(key)
    def testRead_NotFound(self):
        key = "_Read_NotFound"
        t = Transaction()
        self.assertRaises(Scalaris.NotFoundException, t.read, str(_testTime) + key)
        t.closeConnection()

    # Test method for Transaction.read(key) with a closed connection.
    def testRead_NotConnected(self):
        key = "_Read_NotConnected"
        t = Transaction()
        t.closeConnection()
        #self.assertRaises(Scalaris.ConnectionException, t.read, str(_testTime) + key)
        self.assertRaises(Scalaris.NotFoundException, t.read, str(_testTime) + key)
        t.closeConnection()

    # Test method for Transaction.write(key, value=str()) with a closed connection.
    def testWriteString_NotConnected(self):
        key = "_WriteString_NotConnected"
        t = Transaction()
        t.closeConnection()
        #self.assertRaises(Scalaris.ConnectionException, t.write, str(_testTime) + key, _testData[0])
        t.write(str(_testTime) + key, _testData[0])
        t.closeConnection()

    # Test method for Transaction.read(key) and Transaction.write(key, value=str())
    # which should show that writing a value for a key for which a previous read
    # returned a NotFoundException is possible.
    def testWriteString_NotFound(self):
        key = "_WriteString_notFound"
        t = Transaction()
        notFound = False
        try:
            t.read(str(_testTime) + key)
        except Scalaris.NotFoundException:
            notFound = True
        
        self.assertTrue(notFound)
        t.write(str(_testTime) + key, _testData[0])
        self.assertEqual(t.read(str(_testTime) + key), _testData[0])
        t.closeConnection()

    # Test method for Transaction.write(key, value=str()) and Transaction.read(key).
    # Writes strings and uses a distinct key for each value. Tries to read the data afterwards.
    def testWriteString(self):
        key = "_testWriteString1_"
        t = Transaction()
        
        for i in xrange(len(_testData)):
            t.write(str(_testTime) + key + str(i), _testData[i])
        
        # now try to read the data:
        for i in xrange(len(_testData)):
            actual = t.read(str(_testTime) + key + str(i))
            self.assertEqual(actual, _testData[i])
        
        # commit the transaction and try to read the data with a new one:
        t.commit()
        t = Transaction()
        for i in xrange(len(_testData)):
            actual = t.read(str(_testTime) + key + str(i))
            self.assertEqual(actual, _testData[i])
        
        t.closeConnection()

    # Test method for Transaction.write(key, value=list()) and Transaction.read(key).
    # Writes a list and uses a distinct key for each value. Tries to read the data afterwards.
    def testWriteList1(self):
        key = "_testWriteList1_"
        t = Scalaris.Transaction()
        
        for i in xrange(0, len(_testData) - 1, 2):
            t.write(str(_testTime) + key + str(i), [_testData[i], _testData[i + 1]])
        
        # now try to read the data:
        for i in xrange(0, len(_testData), 2):
            actual = t.read(str(_testTime) + key + str(i))
            self.assertEqual(actual, [_testData[i], _testData[i + 1]])
        
        t.closeConnection()
        
        # commit the transaction and try to read the data with a new one:
        t.commit()
        t = Transaction()
        for i in xrange(0, len(_testData), 2):
            actual = t.read(str(_testTime) + key + str(i))
            self.assertEqual(actual, [_testData[i], _testData[i + 1]])
        
        t.closeConnection()

if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(TestTransaction)
    unittest.TextTestRunner(verbosity=2).run(suite)
