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

from Scalaris import PubSub
import Scalaris
from datetime import datetime
import time
import unittest
from BaseHTTPServer import HTTPServer, BaseHTTPRequestHandler
import json
import threading
import sys

# The time when the (whole) test suite was started.
_now = datetime.now()
# This is used to create different erlang keys for each run.
_testTime = int(time.mktime(_now.timetuple()) * 1000 + (_now.microsecond / 1000.0))
# wait that long for notifications to arrive
_notifications_timeout = 60;
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

# checks if there are more elements in list than in expectedElements and returns one of those elements
def _getDiffElement(list, expectedElements):
    for e in expectedElements:
        list.remove(e)
    
    if len(list) > 0:
        return list[0]
    else:
        return None

class TestPubSub(unittest.TestCase):
    def setUp(self):
        pass

    # Test method for PubSub()
    def testPubSub1(self):
        conn = PubSub()
        conn.closeConnection()

    # Test method for PubSub(conn)
    def testPubSub2(self):
        conn = PubSub(conn = Scalaris.JSONConnection(url = Scalaris.default_url))
        conn.closeConnection()

    # Test method for PubSub.closeConnection() trying to close the connection twice.
    def testDoubleClose(self):
        conn = PubSub()
        conn.closeConnection()
        conn.closeConnection()

    # Test method for PubSub.publish(topic, content) with a closed connection.
    def testPublish_NotConnected(self):
        topic = "_Publish_NotConnected"
        conn = PubSub()
        conn.closeConnection()
        #self.assertRaises(Scalaris.ConnectionException, conn.publish, str(_testTime) + topic, _testData[0])
        conn.publish(str(_testTime) + topic, _testData[0])
        conn.closeConnection()

    # Test method for PubSub.publish(topic, content).
    # Publishes some topics and uses a distinct key for each value.
    def testPublish1(self):
        topic = "_Publish1_"
        conn = PubSub()
        
        for i in xrange(len(_testData)):
            conn.publish(str(_testTime) + topic + str(i), _testData[i])
        
        conn.closeConnection()

    # Test method for PubSub.publish(topic, content).
    # Publishes some topics and uses a single key for all the values.
    def testPublish2(self):
        topic = "_Publish2"
        conn = PubSub()
        
        for i in xrange(len(_testData)):
            conn.publish(str(_testTime) + topic, _testData[i])
        
        conn.closeConnection()

    # Test method for PubSub.getSubscribers(topic) with a closed connection.
    def testGetSubscribersOtp_NotConnected(self):
        topic = "_GetSubscribers_NotConnected"
        conn = PubSub()
        conn.closeConnection()
        #self.assertRaises(Scalaris.ConnectionException, conn.getSubscribers, str(_testTime) + topic)
        conn.getSubscribers(str(_testTime) + topic)
        conn.closeConnection()

    # Test method for PubSub.getSubscribers(topic).
    # Tries to get a subscriber list from an empty topic.
    def testGetSubscribers_NotExistingTopic(self):
        topic = "_GetSubscribers_NotExistingTopic"
        conn = PubSub()
        subscribers = conn.getSubscribers(str(_testTime) + topic)
        self.assertEqual(subscribers, [])
        conn.closeConnection()

    # Test method for PubSub.subscribe(topic url) with a closed connection.
    def testSubscribe_NotConnected(self):
        topic = "_Subscribe_NotConnected"
        conn = PubSub()
        conn.closeConnection()
        #self.assertRaises(Scalaris.ConnectionException, conn.subscribe, str(_testTime) + topic, _testData[0]) 
        conn.subscribe(str(_testTime) + topic, _testData[0])
        conn.closeConnection()

    # Test method for PubSub.subscribe(topic, url) and PubSub.getSubscribers(topic).
    # Subscribes some arbitrary URLs to arbitrary topics and uses a distinct topic for each URL.
    def testSubscribe1(self):
        topic = "_Subscribe1_"
        conn = PubSub()
        
        for i in xrange(len(_testData)):
            conn.subscribe(str(_testTime) + topic + str(i), _testData[i])
        
        # check if the subscribers were successfully saved:
        for i in xrange(len(_testData)):
            topic1 = topic + str(i)
            subscribers = conn.getSubscribers(str(_testTime) + topic1)
            self.assertTrue(_testData[i] in subscribers,
                            msg = "Subscriber \"" + _testData[i] + "\" does not exist for topic \"" + topic1 + "\"")
            self.assertEqual(len(subscribers), 1,
                            msg = "Subscribers of topic (" + topic1 + ") should only be [" + _testData[i] + "], but is: " + repr(subscribers))
        
        conn.closeConnection()

    # Test method for PubSub.subscribe(topic, url) and PubSub.getSubscribers(topic).
    # Subscribes some arbitrary URLs to arbitrary topics and uses a single topic for all URLs.
    def testSubscribe2(self):
        topic = "_Subscribe2"
        conn = PubSub()
        
        for i in xrange(len(_testData)):
            conn.subscribe(str(_testTime) + topic, _testData[i])
        
        # check if the subscribers were successfully saved:
        subscribers = conn.getSubscribers(str(_testTime) + topic)
        for i in xrange(len(_testData)):
            self.assertTrue(_testData[i] in subscribers,
                            msg = "Subscriber \"" + _testData[i] + "\" does not exist for topic \"" + topic + "\"")
        self.assertEqual(_getDiffElement(subscribers, _testData), None,
                         msg = "unexpected subscriber of topic \"" + topic + "\"")
        
        conn.closeConnection()

    # Test method for PubSub.unsubscribe(topic url) with a closed connection.
    def testUnsubscribe_NotConnected(self):
        topic = "_Unsubscribe_NotConnected"
        conn = PubSub()
        conn.closeConnection()
        #self.assertRaises(Scalaris.ConnectionException, conn.unsubscribe, str(_testTime) + topic, _testData[0])
        self.assertRaises(Scalaris.NotFoundException, conn.unsubscribe, str(_testTime) + topic, _testData[0])
        conn.closeConnection()

    # Test method for PubSub.unsubscribe(topic url) and PubSub.getSubscribers(topic).
    # Tries to unsubscribe an URL from a non-existing topic and tries to get the subscriber list afterwards.
    def testUnsubscribe_NotExistingTopic(self):
        topic = "_Unsubscribe_NotExistingTopic"
        conn = PubSub()
        # unsubscribe test "url":
        self.assertRaises(Scalaris.NotFoundException, conn.unsubscribe, str(_testTime) + topic, _testData[0])
        
        # check whether the unsubscribed urls were unsubscribed:
        subscribers = conn.getSubscribers(str(_testTime) + topic)
        self.assertFalse(_testData[0] in subscribers,
                        msg = "Subscriber \"" + _testData[0] + "\" should have been unsubscribed from topic \"" + topic + "\"")
        self.assertEqual(len(subscribers), 0,
                        msg = "Subscribers of topic (" + topic + ") should only be [], but is: " + repr(subscribers))
        
        conn.closeConnection()

    # Test method for PubSub.subscribe(topic url), PubSub.unsubscribe(topic url) and PubSub.getSubscribers(topic).
    # Tries to unsubscribe an unsubscribed URL from an existing topic and compares the subscriber list afterwards.
    def testUnsubscribe_NotExistingUrl(self):
        topic = "_Unsubscribe_NotExistingUrl"
        conn = PubSub()
        
        # first subscribe test "urls"...
        conn.subscribe(str(_testTime) + topic, _testData[0])
        conn.subscribe(str(_testTime) + topic, _testData[1])
        
        # then unsubscribe another "url":
        self.assertRaises(Scalaris.NotFoundException, conn.unsubscribe, str(_testTime) + topic, _testData[2])
        
        # check whether the subscribers were successfully saved:
        subscribers = conn.getSubscribers(str(_testTime) + topic)
        self.assertTrue(_testData[0] in subscribers,
                        msg = "Subscriber \"" + _testData[0] + "\" does not exist for topic \"" + topic + "\"")
        self.assertTrue(_testData[1] in subscribers,
                        msg = "Subscriber \"" + _testData[1] + "\" does not exist for topic \"" + topic + "\"")
        
        # check whether the unsubscribed urls were unsubscribed:
        self.assertFalse(_testData[2] in subscribers,
                        msg = "Subscriber \"" + _testData[2] + "\" should have been unsubscribed from topic \"" + topic + "\"")
        
        self.assertEqual(len(subscribers), 2,
                        msg = "Subscribers of topic (" + topic + ") should only be [\"" + _testData[0] + "\", \"" + _testData[1] + "\"], but is: " + repr(subscribers))
        
        conn.closeConnection()

    # Test method for PubSub.subscribe(topic url), PubSub.unsubscribe(topic url) and PubSub.getSubscribers(topic).
    # Subscribes some arbitrary URLs to arbitrary topics and uses a distinct topic for each URL.
    # Unsubscribes every second subscribed URL.
    def testUnsubscribe1(self):
        topic = "_UnsubscribeString1_"
        conn = PubSub()
        
        # first subscribe test "urls"...
        for i in xrange(len(_testData)):
            conn.subscribe(str(_testTime) + topic + str(i), _testData[i])
        
        # ... then unsubscribe every second url:
        for i in xrange(0, len(_testData), 2):
            conn.unsubscribe(str(_testTime) + topic + str(i), _testData[i])
        
        # check whether the subscribers were successfully saved:
        for i in xrange(1, len(_testData), 2):
            topic1 = topic + str(i)
            subscribers = conn.getSubscribers(str(_testTime) + topic1)
            self.assertTrue(_testData[i] in subscribers,
                            msg = "Subscriber \"" + _testData[i] + "\" does not exist for topic \"" + topic1 + "\"")
            self.assertEqual(len(subscribers), 1,
                            msg = "Subscribers of topic (" + topic1 + ") should only be [\"" + _testData[i] + "\"], but is: " + repr(subscribers))
        
        # check whether the unsubscribed urls were unsubscribed:
        for i in xrange(0, len(_testData), 2):
            topic1 = topic + str(i)
            subscribers = conn.getSubscribers(str(_testTime) + topic1)
            self.assertFalse(_testData[i] in subscribers,
                            msg = "Subscriber \"" + _testData[i] + "\" should have been unsubscribed from topic \"" + topic1 + "\"")
            self.assertEqual(len(subscribers), 0,
                            msg = "Subscribers of topic (" + topic1 + ") should only be [], but is: " + repr(subscribers))
        
        conn.closeConnection()

    # Test method for PubSub.subscribe(topic url), PubSub.unsubscribe(topic url) and PubSub.getSubscribers(topic).
    # Subscribes some arbitrary URLs to arbitrary topics and uses a single topic for all URLs.
    # Unsubscribes every second subscribed URL.
    def testUnsubscribe2(self):
        topic = "_UnubscribeString2"
        conn = PubSub()
        
        # first subscribe all test "urls"...
        for i in xrange(len(_testData)):
            conn.subscribe(str(_testTime) + topic, _testData[i])
        
        # ... then unsubscribe every second url:
        for i in xrange(0, len(_testData), 2):
            conn.unsubscribe(str(_testTime) + topic, _testData[i])
        
        # check whether the subscribers were successfully saved:
        subscribers = conn.getSubscribers(str(_testTime) + topic)
        subscribers_expected = []
        for i in xrange(1, len(_testData), 2):
            subscribers_expected.append(_testData[i])
            self.assertTrue(_testData[i] in subscribers,
                            msg = "Subscriber \"" + _testData[i] + "\" does not exist for topic \"" + topic + "\"")
        
        # check whether the unsubscribed urls were unsubscribed:
        for i in xrange(0, len(_testData), 2):
            self.assertFalse(_testData[i] in subscribers,
                            msg = "Subscriber \"" + _testData[i] + "\" should have been unsubscribed from topic \"" + topic + "\"")
        
        self.assertEqual(_getDiffElement(subscribers, subscribers_expected), None,
                         msg = "unexpected subscriber of topic \"" + topic + "\"")
                        
        conn.closeConnection()
    
    def _checkNotifications(self, notifications, expected):
        for (topic, contents) in expected.items():
            if topic not in notifications:
                notifications[topic] = []
            for content in contents:
                self.assertTrue(content in notifications[topic],
                                msg = "subscription (" + topic + ", " + content + ") not received by server)")
                notifications[topic].remove(content)
            if len(notifications[topic]) > 0:
                self.fail("Received element (" + topic + ", " + notifications[topic][0] + ") which is not part of the subscription.")
            del notifications[topic]
        # is there another (unexpected) topic we received content for?
        if len(notifications) > 0:
            for (topic, contents) in notifications.items():
                if len(contents) > 0:
                    self.fail("Received notification for topic (" + topic + ", " + contents[0] + ") which is not part of the subscription.")
                    break
    
    # Test method for the publish/subscribe system.
    # Single server, subscription to one topic, multiple publishs.
    def testSubscription1(self):
        topic = str(_testTime) + "_Subscription1"
        conn = PubSub()
        server1 = _newSubscriptionServer()
        notifications_server1_expected = {topic: []}
        ip1, port1 = server1.server_address
        
        conn.subscribe(topic, 'http://' + str(ip1) + ':' + str(port1))
        for i in xrange(len(_testData)):
            conn.publish(topic, _testData[i])
            notifications_server1_expected[topic].append(_testData[i])
        
        # wait max '_notifications_timeout' seconds for notifications:
        for i in xrange(_notifications_timeout):
            if topic not in server1.notifications or len(server1.notifications[topic]) < len(notifications_server1_expected[topic]):
                time.sleep(1)
            else:
                break
        
        server1.shutdown()
        
        # check that every notification arrived:
        self._checkNotifications(server1.notifications, notifications_server1_expected)
        conn.closeConnection()
    
    # Test method for the publish/subscribe system.
    # Three servers, subscription to one topic, multiple publishs.
    def testSubscription2(self):
        topic = str(_testTime) + "_Subscription2"
        conn = PubSub()
        server1 = _newSubscriptionServer()
        server2 = _newSubscriptionServer()
        server3 = _newSubscriptionServer()
        notifications_server1_expected = {topic: []}
        notifications_server2_expected = {topic: []}
        notifications_server3_expected = {topic: []}
        ip1, port1 = server1.server_address
        ip2, port2 = server2.server_address
        ip3, port3 = server3.server_address
        
        conn.subscribe(topic, 'http://' + str(ip1) + ':' + str(port1))
        conn.subscribe(topic, 'http://' + str(ip2) + ':' + str(port2))
        conn.subscribe(topic, 'http://' + str(ip3) + ':' + str(port3))
        for i in xrange(len(_testData)):
            conn.publish(topic, _testData[i])
            notifications_server1_expected[topic].append(_testData[i])
            notifications_server2_expected[topic].append(_testData[i])
            notifications_server3_expected[topic].append(_testData[i])
        
        # wait max '_notifications_timeout' seconds for notifications:
        for i in xrange(_notifications_timeout):
            if (topic not in server1.notifications or len(server1.notifications[topic]) < len(notifications_server1_expected[topic])) or \
               (topic not in server2.notifications or len(server2.notifications[topic]) < len(notifications_server2_expected[topic])) or \
               (topic not in server3.notifications or len(server3.notifications[topic]) < len(notifications_server3_expected[topic])):
                time.sleep(1)
            else:
                break
        
        server1.shutdown()
        server2.shutdown()
        server3.shutdown()
        
        # check that every notification arrived:
        self._checkNotifications(server1.notifications, notifications_server1_expected)
        self._checkNotifications(server2.notifications, notifications_server2_expected)
        self._checkNotifications(server3.notifications, notifications_server3_expected)
        conn.closeConnection()
    
    # Test method for the publish/subscribe system.
    # Three servers, subscription to different topics, multiple publishs, each
    # server receives a different number of elements.
    def testSubscription3(self):
        topic1 = str(_testTime) + "_Subscription3_1"
        topic2 = str(_testTime) + "_Subscription3_2"
        topic3 = str(_testTime) + "_Subscription3_3"
        conn = PubSub()
        server1 = _newSubscriptionServer()
        server2 = _newSubscriptionServer()
        server3 = _newSubscriptionServer()
        notifications_server1_expected = {topic1: []}
        notifications_server2_expected = {topic2: []}
        notifications_server3_expected = {topic3: []}
        ip1, port1 = server1.server_address
        ip2, port2 = server2.server_address
        ip3, port3 = server3.server_address
        
        conn.subscribe(topic1, 'http://' + str(ip1) + ':' + str(port1))
        conn.subscribe(topic2, 'http://' + str(ip2) + ':' + str(port2))
        conn.subscribe(topic3, 'http://' + str(ip3) + ':' + str(port3))
        for i in xrange(0, len(_testData), 2):
            conn.publish(topic1, _testData[i])
            notifications_server1_expected[topic1].append(_testData[i])
        for i in xrange(0, len(_testData), 3):
            conn.publish(topic2, _testData[i])
            notifications_server2_expected[topic2].append(_testData[i])
        for i in xrange(0, len(_testData), 5):
            conn.publish(topic3, _testData[i])
            notifications_server3_expected[topic3].append(_testData[i])
        
        # wait max '_notifications_timeout' seconds for notifications:
        for i in xrange(_notifications_timeout):
            if (topic1 not in server1.notifications or len(server1.notifications[topic1]) < len(notifications_server1_expected[topic1])) or \
               (topic2 not in server2.notifications or len(server2.notifications[topic2]) < len(notifications_server2_expected[topic2])) or \
               (topic3 not in server3.notifications or len(server3.notifications[topic3]) < len(notifications_server3_expected[topic3])):
                time.sleep(1)
            else:
                break
        
        server1.shutdown()
        server2.shutdown()
        server3.shutdown()
        
        # check that every notification arrived:
        self._checkNotifications(server1.notifications, notifications_server1_expected)
        self._checkNotifications(server2.notifications, notifications_server2_expected)
        self._checkNotifications(server3.notifications, notifications_server3_expected)
        conn.closeConnection()
    
    # Test method for the publish/subscribe system.
    # Like testSubscription3() but some subscribed urls will be unsubscribed.
    def testSubscription4(self):
        topic1 = str(_testTime) + "_Subscription4_1"
        topic2 = str(_testTime) + "_Subscription4_2"
        topic3 = str(_testTime) + "_Subscription4_3"
        conn = PubSub()
        server1 = _newSubscriptionServer()
        server2 = _newSubscriptionServer()
        server3 = _newSubscriptionServer()
        notifications_server1_expected = {topic1: []}
        notifications_server2_expected = {topic2: []}
        notifications_server3_expected = {topic3: []}
        ip1, port1 = server1.server_address
        ip2, port2 = server2.server_address
        ip3, port3 = server3.server_address
        
        conn.subscribe(topic1, 'http://' + str(ip1) + ':' + str(port1))
        conn.subscribe(topic2, 'http://' + str(ip2) + ':' + str(port2))
        conn.subscribe(topic3, 'http://' + str(ip3) + ':' + str(port3))
        conn.unsubscribe(topic2, 'http://' + str(ip2) + ':' + str(port2))
        for i in xrange(0, len(_testData), 2):
            conn.publish(topic1, _testData[i])
            notifications_server1_expected[topic1].append(_testData[i])
        for i in xrange(0, len(_testData), 3):
            conn.publish(topic2, _testData[i])
            # note: topic2 is unsubscribed
            # notifications_server2_expected[topic2].append(_testData[i])
        for i in xrange(0, len(_testData), 5):
            conn.publish(topic3, _testData[i])
            notifications_server3_expected[topic3].append(_testData[i])
        
        # wait max '_notifications_timeout' seconds for notifications:
        for i in xrange(_notifications_timeout):
            if (topic1 not in server1.notifications or len(server1.notifications[topic1]) < len(notifications_server1_expected[topic1])) or \
               (topic3 not in server3.notifications or len(server3.notifications[topic3]) < len(notifications_server3_expected[topic3])):
                time.sleep(1)
            else:
                break
        
        server1.shutdown()
        server2.shutdown()
        server3.shutdown()
        
        # check that every notification arrived:
        self._checkNotifications(server1.notifications, notifications_server1_expected)
        self._checkNotifications(server2.notifications, notifications_server2_expected)
        self._checkNotifications(server3.notifications, notifications_server3_expected)
        conn.closeConnection()

def _newSubscriptionServer(server_address = ('localhost', 0)):
    server = SubscriptionServer(server_address)
    #ip, port = server.server_address

    # Start a thread with the server
    server_thread = threading.Thread(target=server.serve_forever)
    # Exit the server thread when the main thread terminates
    server_thread.setDaemon(True)
    server_thread.start()
    #print "Server loop running in thread:", server_thread.getName()

    return server

class SubscriptionServer(HTTPServer):
   def __init__(self, server_address):
       HTTPServer.__init__(self, server_address, SubscriptionHandler)
       self.notifications = {}

class SubscriptionHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        if 'content-length' in self.headers:
            length = int(self.headers['content-length'])
            data = self.rfile.read(length)
            response_json = json.loads(data)
            # {"method":"notify","params":["1209386211287_SubscribeTest","content"],"id":482975}
            if 'method' in response_json and response_json['method'] == 'notify' \
                and 'params' in response_json and 'id' in response_json \
                and isinstance(response_json['params'], list) and len(response_json['params']) == 2:
                    topic = response_json['params'][0]
                    content = response_json['params'][1]
                    if hasattr(self.server, 'notifications'):
                        if topic not in self.server.notifications:
                            self.server.notifications[topic] = []
                        self.server.notifications[topic].append(content)
        else:
            pass
        
        response = '{}'
        self.send_response(200)
        self.send_header("Content-type", "text/html")
        self.send_header("Content-length", str(len(response)))
        self.end_headers()
        self.wfile.write(response)
    
    # to disable logging
    def log_message(self, *args):
        pass

if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(TestPubSub)
    if not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful():
        sys.exit(1)
