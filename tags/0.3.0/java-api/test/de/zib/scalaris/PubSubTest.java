/**
 *  Copyright 2007-2011 Zuse Institute Berlin
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package de.zib.scalaris;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Reader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.nio.SelectChannelConnector;
import org.eclipse.jetty.util.ajax.JSON;
import org.junit.Test;

import com.ericsson.otp.erlang.OtpErlangList;
import com.ericsson.otp.erlang.OtpErlangString;

/**
 * Unit test for the {@link PubSub} class.
 *
 * @author Nico Kruber, kruber@zib.de
 * @version 2.5
 * @since 2.5
 */
public class PubSubTest {
    private final static long testTime = System.currentTimeMillis();

    /**
     * wait that long for notifications to arrive
     */
    private static final int notifications_timeout = 60;

    private final static String[] testData = {
        "ahz2ieSh", "wooPhu8u", "quai9ooK", "Oquae4ee", "Airier1a", "Boh3ohv5", "ahD3Saog", "EM5ooc4i",
        "Epahrai8", "laVahta7", "phoo6Ahj", "Igh9eepa", "aCh4Lah6", "ooT0ath5", "uuzau4Ie", "Iup6mae6",
//        "xie7iSie", "ail8yeeP", "ooZ4eesi", "Ahn7ohph", "Ohy5moo6", "xooSh9Oo", "ieb6eeS7", "Thooqu9h",
//        "eideeC9u", "phois3Ie", "EimaiJ2p", "sha6ahR1", "Pheih3za", "bai4eeXe", "rai0aB7j", "xahXoox6",
//        "Xah4Okeg", "cieG8Yae", "Pe9Ohwoo", "Eehig6ph", "Xe7rooy6", "waY2iifu", "kemi8AhY", "Che7ain8",
//        "ohw6seiY", "aegh1oBa", "thoh9IeG", "Kee0xuwu", "Gohng8ee", "thoh9Chi", "aa4ahQuu", "Iesh5uge",
//        "Ahzeil8n", "ieyep5Oh", "xah3IXee", "Eefa5qui", "kai8Muuf", "seeCe0mu", "cooqua5Y", "Ci3ahF6z",
//        "ot0xaiNu", "aewael8K", "aev3feeM", "Fei7ua5t", "aeCa6oph", "ag2Aelei", "Shah1Pho", "ePhieb0N",
//        "Uqu7Phup", "ahBi8voh", "oon3aeQu", "Koopa0nu", "xi0quohT", "Oog4aiph", "Aip2ag5D", "tirai7Ae",
//        "gi0yoePh", "uay7yeeX", "aeb6ahC1", "OoJeic2a", "ieViom1y", "di0eeLai", "Taec2phe", "ID2cheiD",
//        "oi6ahR5M", "quaiGi8W", "ne1ohLuJ", "DeD0eeng", "yah8Ahng", "ohCee2ie", "ecu1aDai", "oJeijah4",
//        "Goo9Una1", "Aiph3Phi", "Ieph0ce5", "ooL6cae7", "nai0io1H", "Oop2ahn8", "ifaxae7O", "NeHai1ae",
//        "Ao8ooj6a", "hi9EiPhi", "aeTh9eiP", "ao8cheiH", "Yieg3sha", "mah7cu2D", "Uo5wiegi", "Oowei0ya",
//        "efeiDee7", "Oliese6y", "eiSh1hoh", "Joh6hoh9", "zib6Ooqu", "eejiJie4", "lahZ3aeg", "keiRai1d",
//        "Fei0aewe", "aeS8aboh", "hae3ohKe", "Een9ohQu", "AiYeeh7o", "Yaihah4s", "ood4Giez", "Oumai7te",
//        "hae2kahY", "afieGh4v", "Ush0boo0", "Ekootee5", "Ya8iz6Ie", "Poh6dich", "Eirae4Ah", "pai8Eeme",
//        "uNah7dae", "yo3hahCh", "teiTh7yo", "zoMa5Cuv", "ThiQu5ax", "eChi5caa", "ii9ujoiV", "ge7Iekui",
        "sai2aiTa", "ohKi9rie", "ei2ioChu", "aaNgah9y", "ooJai1Ie", "shoh0oH9", "Ool4Ahya", "poh0IeYa",
        "Uquoo0Il", "eiGh4Oop", "ooMa0ufe", "zee6Zooc", "ohhao4Ah", "Uweekek5", "aePoos9I", "eiJ9noor",
        "phoong1E", "ianieL2h", "An7ohs4T", "Eiwoeku3", "sheiS3ao", "nei5Thiw", "uL5iewai", "ohFoh9Ae"};

    static {
        // set not to automatically try reconnects (auto-retries prevent ConnectionException tests from working):
        ((DefaultConnectionPolicy) ConnectionFactory.getInstance().getConnectionPolicy()).setMaxRetries(0);
    }

    /**
     * Test method for
     * {@link PubSub#PubSub()}.
     * @throws ConnectionException
     */
    @Test
    public void testPubSub1() throws ConnectionException {
        PubSub conn = new PubSub();
        conn.closeConnection();
    }

    /**
     * Test method for
     * {@link PubSub#PubSub(Connection)}.
     * @throws ConnectionException
     */
    @Test
    public void testPubSub2() throws ConnectionException {
        PubSub conn = new PubSub(ConnectionFactory.getInstance().createConnection("test"));
        conn.closeConnection();
    }

    /**
     * Test method for {@link TransactionSingleOp#closeConnection()} trying to
     * close the connection twice.
     *
     * @throws UnknownException
     * @throws ConnectionException
     */
    @Test
    public void testDoubleClose() throws ConnectionException {
        TransactionSingleOp conn = new TransactionSingleOp();
        conn.closeConnection();
        conn.closeConnection();
    }

    /**
     * Test method for
     * {@link PubSub#publish(OtpErlangString, OtpErlangString)} with a closed
     * connection.
     *
     * @throws UnknownException
     * @throws TimeoutException
     * @throws ConnectionException
     * @throws NotFoundException
     */
    @Test(expected=ConnectionException.class)
    public void testPublishOtp_NotConnected() throws ConnectionException,
            TimeoutException, UnknownException, NotFoundException {
        String topic = "_PublishOtp_NotConnected";
        PubSub conn = new PubSub();
        conn.closeConnection();
        conn.publish(
                new OtpErlangString(testTime + topic),
                new OtpErlangString(testData[0]));
    }

    /**
     * Test method for
     * {@link PubSub#publish(OtpErlangString, OtpErlangString)}.
     * Publishes some topics and uses a distinct key for each value.
     *
     * @throws UnknownException
     * @throws TimeoutException
     * @throws ConnectionException
     * @throws NotFoundException
     */
    @Test
    public void testPublishOtp1() throws ConnectionException,
            TimeoutException, UnknownException, NotFoundException {
        String topic = "_PublishOtp1_";
        PubSub conn = new PubSub();

        try {
            for (int i = 0; i < testData.length; ++i) {
                conn.publish(
                        new OtpErlangString(testTime + topic + i),
                        new OtpErlangString(testData[i]));
            }
        } finally {
            conn.closeConnection();
        }
    }

    /**
     * Test method for
     * {@link PubSub#publish(OtpErlangString, OtpErlangString)}.
     * Publishes some topics and uses a single key for all the values.
     *
     * @throws UnknownException
     * @throws TimeoutException
     * @throws ConnectionException
     * @throws NotFoundException
     */
    @Test
    public void testPublishOtp2() throws ConnectionException,
            TimeoutException, UnknownException, NotFoundException {
        String topic = "_PublishOtp2";
        PubSub conn = new PubSub();

        try {
            for (int i = 0; i < testData.length; ++i) {
                conn.publish(
                        new OtpErlangString(testTime + topic),
                        new OtpErlangString(testData[i]) );
            }
        } finally {
            conn.closeConnection();
        }
    }

    /**
     * Test method for {@link PubSub#publish(String, String)} with a closed
     * connection.
     *
     * @throws UnknownException
     * @throws TimeoutException
     * @throws ConnectionException
     * @throws NotFoundException
     */
    @Test(expected=ConnectionException.class)
    public void testPublish_NotConnected() throws ConnectionException,
            TimeoutException, UnknownException, NotFoundException {
        String topic = "_Publish_NotConnected";
        PubSub conn = new PubSub();
        conn.closeConnection();
        conn.publish(testTime + topic, testData[0]);
    }

    /**
     * Test method for
     * {@link PubSub#publish(String, String)}.
     * Publishes some topics and uses a distinct key for each value.
     *
     * @throws UnknownException
     * @throws TimeoutException
     * @throws ConnectionException
     * @throws NotFoundException
     */
    @Test
    public void testPublish1() throws ConnectionException,
            TimeoutException, UnknownException, NotFoundException {
        String topic = "_Publish1_";
        PubSub conn = new PubSub();

        try {
            for (int i = 0; i < testData.length; ++i) {
                conn.publish(
                        testTime + topic + i,
                        testData[i] );
            }
        } finally {
            conn.closeConnection();
        }
    }

    /**
     * Test method for
     * {@link PubSub#publish(String, String)}.
     * Publishes some topics and uses a single key for all the values.
     *
     * @throws UnknownException
     * @throws TimeoutException
     * @throws ConnectionException
     * @throws NotFoundException
     */
    @Test
    public void testPublish2() throws ConnectionException,
            TimeoutException, UnknownException, NotFoundException {
        String topic = "_Publish2";
        PubSub conn = new PubSub();

        try {
            for (int i = 0; i < testData.length; ++i) {
                conn.publish(
                        testTime + topic,
                        testData[i] );
            }
        } finally {
            conn.closeConnection();
        }
    }

    // getSubscribers() test methods for not existing topics begin

    /**
     * Test method for
     * {@link PubSub#getSubscribers(OtpErlangString)} with a closed
     * connection.
     *
     * @throws UnknownException
     * @throws TimeoutException
     * @throws ConnectionException
     * @throws NotFoundException
     */
    @Test(expected=ConnectionException.class)
    public void testGetSubscribersOtp_NotConnected() throws ConnectionException,
            TimeoutException, UnknownException, NotFoundException {
        String topic = "_GetSubscribersOtp_NotConnected";
        PubSub conn = new PubSub();
        conn.closeConnection();
        conn.getSubscribers(new OtpErlangString(testTime + topic));
    }

    /**
     * Test method for
     * {@link PubSub#getSubscribers(OtpErlangString)}.
     * Tries to get a subscriber list from an empty topic.
     *
     * @throws UnknownException
     * @throws TimeoutException
     * @throws ConnectionException
     * @throws NotFoundException
     */
    @Test
    public void testGetSubscribersOtp_NotExistingTopic() throws ConnectionException,
            TimeoutException, UnknownException, NotFoundException {
        String topic = "_GetSubscribersOtp_NotExistingTopic";
        PubSub conn = new PubSub();

        try {
            OtpErlangList subscribers = (OtpErlangList) conn.getSubscribers(new OtpErlangString(testTime + topic)).value();
            assertTrue(subscribers.arity() == 0);
        } finally {
            conn.closeConnection();
        }
    }

    /**
     * Test method for
     * {@link PubSub#getSubscribers(String)} with a closed
     * connection.
     *
     * @throws UnknownException
     * @throws TimeoutException
     * @throws ConnectionException
     * @throws NotFoundException
     */
    @Test(expected=ConnectionException.class)
    public void testGetSubscribers_NotConnected() throws ConnectionException,
            TimeoutException, UnknownException, NotFoundException {
        String topic = "_GetSubscribers_NotConnected";
        PubSub conn = new PubSub();
        conn.closeConnection();
        conn.getSubscribers(testTime + topic);
    }

    /**
     * Test method for
     * {@link PubSub#getSubscribers(String)}.
     * Tries to get a subscriber list from an empty topic.
     *
     * @throws UnknownException
     * @throws TimeoutException
     * @throws ConnectionException
     * @throws NotFoundException
     */
    @Test
    public void testGetSubscribers_NotExistingTopic() throws ConnectionException,
            TimeoutException, UnknownException, NotFoundException {
        String topic = "_GetSubscribers_NotExistingTopic";
        PubSub conn = new PubSub();

        try {
            List<String> subscribers = conn.getSubscribers(testTime + topic).stringListValue();
            assertTrue(subscribers.isEmpty());
        } finally {
            conn.closeConnection();
        }
    }

    // getSubscribers() test methods for not existing topics end
    // subscribe() test methods begin

    /**
     * checks if the given subscriber exists in the given list
     *
     * @param list
     *            list of subscribers
     * @param subscriber
     *            subscriber to search for
     * @return true if the subscriber was found in the list
     */
    private boolean checkSubscribers(OtpErlangList list, String subscriber) {
        for (int i = 0; i < list.arity(); ++i) {
            if (((OtpErlangString) list.elementAt(i)).stringValue().equals(
                    subscriber)) {
                return true;
            }
        }
        return false;
    }

    /**
     * checks if the given subscriber exists in the given list
     *
     * @param list
     *            list of subscribers
     * @param subscriber
     *            subscriber to search for
     * @return true if the subscriber was found in the list
     */
    private boolean checkSubscribers(List<String> list, String subscriber) {
        return list.contains(subscriber);
    }

    /**
     * checks if there are more elements in {@code list} than in
     * {@code expectedElements} and returns one of those elements
     *
     * @param list
     * @param expectedElements
     * @return
     */
    private String getDiffElement(OtpErlangList list, String[] expectedElements) {
        Vector<String> expectedElements2 = new Vector<String>(Arrays.asList(expectedElements));
        for (int i = 0; i < list.arity(); ++i) {
            String element = ((OtpErlangString)list.elementAt(i)).stringValue();
            if (!expectedElements2.contains(element)) {
                return element;
            }
            expectedElements2.remove(element);
        }
        return null;
    }

    /**
     * checks if there are more elements in {@code list} than in
     * {@code expectedElements} and returns one of those elements
     *
     * @param list
     * @param expectedElements
     * @return
     */
    private String getDiffElement(List<String> list, String[] expectedElements) {
        List<String> expectedElements2 = new Vector<String>(Arrays.asList(expectedElements));
        list.removeAll(expectedElements2);

        if (list.size() > 0) {
            return list.get(0);
        } else {
            return null;
        }
    }

    /**
     * Test method for
     * {@link PubSub#subscribe(OtpErlangString, OtpErlangString)} with a
     * closed connection.
     *
     * @throws UnknownException
     * @throws TimeoutException
     * @throws ConnectionException
     * @throws NotFoundException
     * @throws AbortException
     */
    @Test(expected=ConnectionException.class)
    public void testSubscribeOtp_NotConnected() throws ConnectionException,
            TimeoutException, UnknownException, NotFoundException, AbortException {
        String topic = "_SubscribeOtp_NotConnected";
        PubSub conn = new PubSub();
        conn.closeConnection();
        conn.subscribe(
                new OtpErlangString(testTime + topic),
                new OtpErlangString(testData[0]) );
    }

    /**
     * Test method for
     * {@link PubSub#subscribe(OtpErlangString, OtpErlangString)} and
     * {@link PubSub#getSubscribers(OtpErlangString)}.
     * Subscribes some arbitrary URLs to arbitrary topics and uses a distinct topic for each URL.
     *
     * @throws UnknownException
     * @throws TimeoutException
     * @throws ConnectionException
     * @throws NotFoundException
     * @throws AbortException
     */
    @Test
    public void testSubscribeOtp1() throws ConnectionException,
            TimeoutException, UnknownException, NotFoundException, AbortException {
        String topic = "_SubscribeOtp1_";
        PubSub conn = new PubSub();

        try {
            for (int i = 0; i < testData.length; ++i) {
                conn.subscribe(
                        new OtpErlangString(testTime + topic + i),
                        new OtpErlangString(testData[i]) );
            }

            // check if the subscribers were successfully saved:
            for (int i = 0; i < testData.length; ++i) {
                String topic1 = topic + i;
                OtpErlangList subscribers = (OtpErlangList) conn
                        .getSubscribers(new OtpErlangString(testTime
                                + topic1)).value();
                assertTrue("Subscriber \"" + testData[i]
                        + "\" does not exist for topic \"" + topic1 + "\"", checkSubscribers(
                        subscribers, testData[i]));

                assertEquals("Subscribers of topic (" + topic1
                        + ") should only be [\"" + testData[i] + "\"], but is: "
                        + subscribers.toString(), 1, subscribers.arity());
            }
        } finally {
            conn.closeConnection();
        }
    }

    /**
     * Test method for
     * {@link PubSub#subscribe(OtpErlangString, OtpErlangString)} and
     * {@link PubSub#getSubscribers(OtpErlangString)}.
     * Subscribes some arbitrary URLs to arbitrary topics and uses a single topic for all URLs.
     *
     * @throws UnknownException
     * @throws TimeoutException
     * @throws ConnectionException
     * @throws NotFoundException
     * @throws AbortException
     */
    @Test
    public void testSubscribeOtp2() throws ConnectionException,
            TimeoutException, UnknownException, NotFoundException, AbortException {
        String topic = "_SubscribeOtp2";
        PubSub conn = new PubSub();

        try {
            for (int i = 0; i < testData.length; ++i) {
                conn.subscribe(
                        new OtpErlangString(testTime + topic),
                        new OtpErlangString(testData[i]) );
            }

            // check if the subscribers were successfully saved:
            OtpErlangList subscribers = (OtpErlangList) conn
                    .getSubscribers(new OtpErlangString(testTime + topic)).value();
            for (int i = 0; i < testData.length; ++i) {
                assertTrue("Subscriber \"" + testData[i]
                        + "\" does not exist for topic \"" + topic + "\"", checkSubscribers(
                        subscribers, testData[i]));
            }

            assertEquals("unexpected subscriber of topic \"" + topic + "\"", null, getDiffElement(subscribers, testData));
        } finally {
            conn.closeConnection();
        }
    }

    /**
     * Test method for {@link PubSub#subscribe(String, String)} with a closed
     * connection.
     *
     * @throws UnknownException
     * @throws TimeoutException
     * @throws ConnectionException
     * @throws NotFoundException
     * @throws AbortException
     */
    @Test(expected=ConnectionException.class)
    public void testSubscribe_NotConnected() throws ConnectionException,
            TimeoutException, UnknownException, NotFoundException, AbortException {
        String topic = "_Subscribe_NotConnected";
        PubSub conn = new PubSub();
        conn.closeConnection();
        conn.subscribe(testTime + topic, testData[0]);
    }

    /**
     * Test method for
     * {@link PubSub#subscribe(String, String)} and
     * {@link PubSub#getSubscribers(String)}.
     * Subscribes some arbitrary URLs to arbitrary topics and uses a distinct topic for each URL.
     *
     * @throws UnknownException
     * @throws TimeoutException
     * @throws ConnectionException
     * @throws NotFoundException
     * @throws AbortException
     */
    @Test
    public void testSubscribe1() throws ConnectionException,
            TimeoutException, UnknownException, NotFoundException, AbortException {
        String topic = "_Subscribe1_";
        PubSub conn = new PubSub();

        try {
            for (int i = 0; i < testData.length; ++i) {
                conn.subscribe(testTime + topic + i, testData[i]);
            }

            // check if the subscribers were successfully saved:
            for (int i = 0; i < testData.length; ++i) {
                String topic1 = topic + i;
                List<String> subscribers = conn.getSubscribers(testTime
                        + topic1).stringListValue();
                assertTrue("Subscriber \"" + testData[i]
                        + "\" does not exist for topic \"" + topic1 + "\"", checkSubscribers(
                        subscribers, testData[i]));

                assertEquals("Subscribers of topic (" + topic1
                        + ") should only be [" + testData[i] + "], but is: "
                        + subscribers.toString(), 1, subscribers.size());
            }
        } finally {
            conn.closeConnection();
        }
    }

    /**
     * Test method for
     * {@link PubSub#subscribe(String, String)} and
     * {@link PubSub#getSubscribers(String)}.
     * Subscribes some arbitrary URLs to arbitrary topics and uses a single topic for all URLs.
     *
     * @throws UnknownException
     * @throws TimeoutException
     * @throws ConnectionException
     * @throws NotFoundException
     * @throws AbortException
     */
    @Test
    public void testSubscribe2() throws ConnectionException,
            TimeoutException, UnknownException, NotFoundException, AbortException {
        String topic = "_Subscribe2";
        PubSub conn = new PubSub();

        try {
            for (int i = 0; i < testData.length; ++i) {
                conn.subscribe(
                        testTime + topic,
                        testData[i] );
            }

            // check if the subscribers were successfully saved:
            List<String> subscribers = conn
                    .getSubscribers(testTime + topic).stringListValue();
            for (int i = 0; i < testData.length; ++i) {
                assertTrue("Subscriber " + testData[i]
                        + " does not exist for topic " + topic, checkSubscribers(
                        subscribers, testData[i]));
            }

            assertEquals("unexpected subscriber of topic \"" + topic + "\"", null, getDiffElement(subscribers, testData));
        } finally {
            conn.closeConnection();
        }
    }

    // subscribe() test methods end
    // unsubscribe() test methods begin

    /**
     * Test method for
     * {@link PubSub#unsubscribe(OtpErlangString, OtpErlangString)} with a
     * closed connection.
     *
     * @throws UnknownException
     * @throws TimeoutException
     * @throws ConnectionException
     * @throws NotFoundException
     * @throws AbortException
     */
    @Test(expected=ConnectionException.class)
    public void testUnsubscribeOtp_NotConnected() throws ConnectionException,
            TimeoutException, UnknownException, NotFoundException, AbortException {
        String topic = "_UnsubscribeOtp_NotConnected";
        PubSub conn = new PubSub();
        conn.closeConnection();
        conn.unsubscribe(
                new OtpErlangString(testTime + topic),
                new OtpErlangString(testData[0]) );
    }

    /**
     * Test method for
     * {@link PubSub#unsubscribe(OtpErlangString, OtpErlangString)} and
     * {@link PubSub#getSubscribers(OtpErlangString)}.
     * Tries to unsubscribe an URL from a non-existing topic and tries to get
     * the subscriber list afterwards.
     *
     * @throws UnknownException
     * @throws TimeoutException
     * @throws ConnectionException
     * @throws NotFoundException
     * @throws AbortException
     */
    @Test(expected=NotFoundException.class)
    public void testUnsubscribeOtp_NotExistingTopic() throws ConnectionException,
            TimeoutException, UnknownException, NotFoundException, AbortException {
        String topic = "_UnsubscribeOtp_NotExistingTopic";
        PubSub conn = new PubSub();

        try {
            // unsubscribe test "url":
            conn.unsubscribe(
                    new OtpErlangString(testTime + topic),
                    new OtpErlangString(testData[0]) );

            // check whether the unsubscribed urls were unsubscribed:
            OtpErlangList subscribers = (OtpErlangList) conn
                    .getSubscribers(new OtpErlangString(testTime + topic)).value();
            assertFalse("Subscriber \"" + testData[0]
                    + "\" should have been unsubscribed from topic \"" + topic
                    + "\"", checkSubscribers(subscribers, testData[0]));

            assertEquals("Subscribers of topic (" + topic
                    + ") should only be [], but is: "
                    + subscribers.toString(), 0, subscribers.arity());
        } finally {
            conn.closeConnection();
        }
    }

    /**
     * Test method for
     * {@link PubSub#unsubscribe(OtpErlangString, OtpErlangString)} and
     * {@link PubSub#getSubscribers(OtpErlangString)}.
     * Tries to unsubscribe an unsubscribed URL from an existing topic and compares
     * the subscriber list afterwards.
     *
     * @throws UnknownException
     * @throws TimeoutException
     * @throws ConnectionException
     * @throws NotFoundException
     * @throws AbortException
     */
    @Test(expected=NotFoundException.class)
    public void testUnsubscribeOtp_NotExistingUrl()
            throws ConnectionException, TimeoutException, UnknownException,
            NotFoundException, AbortException {
        String topic = "_UnsubscribeOtp_NotExistingUrl";
        PubSub conn = new PubSub();

        try {
            // first subscribe test "urls"...
            conn.unsubscribe(new OtpErlangString(testTime + topic),
                    new OtpErlangString(testData[0]));
            conn.unsubscribe(new OtpErlangString(testTime + topic),
                    new OtpErlangString(testData[1]));

            // then unsubscribe another "url":
            conn.unsubscribe(new OtpErlangString(testTime + topic),
                    new OtpErlangString(testData[2]));

            OtpErlangList subscribers = (OtpErlangList) conn
                    .getSubscribers(new OtpErlangString(testTime + topic)).value();

            // check whether the subscribers were successfully saved:
            assertTrue("Subscriber \"" + testData[0]
                    + "\" does not exist for topic \"" + topic + "\"",
                    checkSubscribers(subscribers, testData[0]));

            assertTrue("Subscriber \"" + testData[1]
                    + "\" does not exist for topic \"" + topic + "\"",
                    checkSubscribers(subscribers, testData[1]));

            // check whether the unsubscribed urls were unsubscribed:
            assertFalse("Subscriber \"" + testData[2]
                    + "\" should have been unsubscribed from topic \"" + topic
                    + "\"", checkSubscribers(subscribers, testData[2]));

            assertEquals("Subscribers of topic (" + topic + ") should only be [\""
                    + testData[0] + "\", \"" + testData[1] + "\"], but is: "
                    + subscribers.toString(), 2, subscribers.arity());
        } finally {
            conn.closeConnection();
        }
    }

    /**
     * Test method for
     * {@link PubSub#subscribe(OtpErlangString, OtpErlangString)},
     * {@link PubSub#unsubscribe(OtpErlangString, OtpErlangString)} and
     * {@link PubSub#getSubscribers(OtpErlangString)}.
     * Subscribes some arbitrary URLs to arbitrary topics and uses a distinct topic for each URL.
     * Unsubscribes every second subscribed URL.
     *
     * @see #testSubscribeOtp1()
     *
     * @throws UnknownException
     * @throws TimeoutException
     * @throws ConnectionException
     * @throws NotFoundException
     * @throws AbortException
     */
    @Test
    public void testUnsubscribeOtp1() throws ConnectionException,
            TimeoutException, UnknownException, NotFoundException, AbortException {
        String topic = "_UnsubscribeOtp1_";
        PubSub conn = new PubSub();

        try {
            // first subscribe all test "urls"...
            for (int i = 0; i < testData.length; ++i) {
                conn.subscribe(
                        new OtpErlangString(testTime + topic + i),
                        new OtpErlangString(testData[i]) );
            }
            // ... then unsubscribe every second url:
            for (int i = 0; i < testData.length; i += 2) {
                conn.unsubscribe(
                        new OtpErlangString(testTime + topic + i),
                        new OtpErlangString(testData[i]) );
            }

            // check whether the subscribers were successfully saved:
            for (int i = 1; i < testData.length; i += 2) {
                String topic1 = topic + i;
                OtpErlangList subscribers = (OtpErlangList) conn
                        .getSubscribers(new OtpErlangString(testTime + topic1)).value();
                assertTrue("Subscriber \"" + testData[i]
                        + "\" does not exist for topic \"" + topic1 + "\"",
                        checkSubscribers(subscribers, testData[i]));

                assertEquals("Subscribers of topic (" + topic1
                        + ") should only be [\"" + testData[i] + "\"], but is: "
                        + subscribers.toString(), 1, subscribers.arity());
            }
            // check whether the unsubscribed urls were unsubscribed:
            for (int i = 0; i < testData.length; i += 2) {
                String topic1 = topic + i;
                OtpErlangList subscribers = (OtpErlangList) conn
                        .getSubscribers(new OtpErlangString(testTime + topic1)).value();
                assertFalse("Subscriber \"" + testData[i]
                        + "\" should have been unsubscribed from topic \"" + topic1
                        + "\"", checkSubscribers(subscribers, testData[i]));

                assertEquals("Subscribers of topic (" + topic1
                        + ") should only be [], but is: "
                        + subscribers.toString(), 0, subscribers.arity());
            }
        } finally {
            conn.closeConnection();
        }
    }

    /**
     * Test method for
     * {@link PubSub#subscribe(OtpErlangString, OtpErlangString)},
     * {@link PubSub#unsubscribe(OtpErlangString, OtpErlangString)} and
     * {@link PubSub#getSubscribers(OtpErlangString)}.
     * Subscribes some arbitrary URLs to arbitrary topics and uses a single topic for all URLs.
     * Unsubscribes every second subscribed URL.
     *
     * @see #testSubscribeOtp2()
     *
     * @throws UnknownException
     * @throws TimeoutException
     * @throws ConnectionException
     * @throws NotFoundException
     * @throws AbortException
     */
    @Test
    public void testUnsubscribeOtp2() throws ConnectionException,
            TimeoutException, UnknownException, NotFoundException, AbortException {
        String topic = "_UnubscribeOtp2";
        PubSub conn = new PubSub();

        try {
            // first subscribe all test "urls"...
            for (int i = 0; i < testData.length; ++i) {
                conn.subscribe(
                        new OtpErlangString(testTime + topic),
                        new OtpErlangString(testData[i]) );
            }
            // ... then unsubscribe every second url:
            for (int i = 0; i < testData.length; i += 2) {
                conn.unsubscribe(
                        new OtpErlangString(testTime + topic),
                        new OtpErlangString(testData[i]) );
            }

            // check if the subscribers were successfully saved:
            OtpErlangList subscribers = (OtpErlangList) conn
                    .getSubscribers(new OtpErlangString(testTime + topic)).value();
            String[] subscribers_expected = new String[testData.length / 2];
            for (int i = 1; i < testData.length; i += 2) {
                subscribers_expected[i / 2] = testData[i];
                assertTrue("Subscriber \"" + testData[i]
                        + "\" does not exist for topic \"" + topic + "\"",
                        checkSubscribers(subscribers, testData[i]));
            }
            // check whether the unsubscribed urls were unsubscribed:
            for (int i = 0; i < testData.length; i += 2) {
                assertFalse("Subscriber \"" + testData[i]
                        + "\" should have been unsubscribed from topic \"" + topic
                        + "\"", checkSubscribers(subscribers, testData[i]));
            }

            assertEquals("unexpected subscriber of topic \"" + topic + "\"", null, getDiffElement(subscribers, subscribers_expected));
        } finally {
            conn.closeConnection();
        }
    }

    /**
     * Test method for {@link PubSub#unsubscribe(String, String)} with a
     * closed connection.
     *
     * @throws UnknownException
     * @throws TimeoutException
     * @throws ConnectionException
     * @throws NotFoundException
     * @throws AbortException
     */
    @Test(expected=ConnectionException.class)
    public void testUnsubscribe_NotConnected() throws ConnectionException,
            TimeoutException, UnknownException, NotFoundException, AbortException {
        String topic = "_Unsubscribe_NotConnected";
        PubSub conn = new PubSub();
        conn.closeConnection();
        conn.unsubscribe(testTime + topic, testData[0]);
    }

    /**
     * Test method for
     * {@link PubSub#unsubscribe(String, String)} and
     * {@link PubSub#getSubscribers(String)}.
     * Tries to unsubscribe an URL from a non-existing topic and tries to get
     * the subscriber list afterwards.
     *
     * @see #testUnsubscribeOtp_NotExistingTopic()
     *
     * @throws UnknownException
     * @throws TimeoutException
     * @throws ConnectionException
     * @throws NotFoundException
     * @throws AbortException
     */
    @Test(expected=NotFoundException.class)
    public void testUnsubscribe_NotExistingTopic() throws ConnectionException,
            TimeoutException, UnknownException, NotFoundException, AbortException {
        String topic = "_Unsubscribe_NotExistingTopic";
        PubSub conn = new PubSub();

        try {
            // unsubscribe test "url":
            conn.unsubscribe(testTime + topic, testData[0]);

            // check whether the unsubscribed urls were unsubscribed:
            List<String> subscribers = conn.getSubscribers(testTime + topic).stringListValue();
            assertFalse("Subscriber \"" + testData[0]
                    + "\" should have been unsubscribed from topic \"" + topic
                    + "\"", checkSubscribers(subscribers, testData[0]));

            assertEquals("Subscribers of topic (" + topic
                    + ") should only be [], but is: "
                    + subscribers.toString(), 0, subscribers.size());
        } finally {
            conn.closeConnection();
        }
    }

    /**
     * Test method for
     * {@link PubSub#subscribe(String, String)},
     * {@link PubSub#unsubscribe(String, String)} and
     * {@link PubSub#getSubscribers(String)}.
     * Tries to unsubscribe an unsubscribed URL from an existing topic and compares
     * the subscriber list afterwards.
     *
     * @see #testUnsubscribeOtp_NotExistingUrl()
     *
     * @throws UnknownException
     * @throws TimeoutException
     * @throws ConnectionException
     * @throws NotFoundException
     * @throws AbortException
     */
    @Test(expected=NotFoundException.class)
    public void testUnsubscribe_NotExistingUrl()
            throws ConnectionException, TimeoutException, UnknownException,
            NotFoundException, AbortException {
        String topic = "_Unsubscribe_NotExistingUrl";
        PubSub conn = new PubSub();

        try {
            // first subscribe test "urls"...
            conn.subscribe(testTime + topic, testData[0]);
            conn.subscribe(testTime + topic, testData[1]);

            // then unsubscribe another "url":
            conn.unsubscribe(testTime + topic, testData[2]);


            // check whether the subscribers were successfully saved:
            List<String> subscribers = conn.getSubscribers(testTime + topic).stringListValue();
            assertTrue("Subscriber \"" + testData[0]
                    + "\" does not exist for topic \"" + topic + "\"",
                    checkSubscribers(subscribers, testData[0]));

            assertTrue("Subscriber \"" + testData[1]
                    + "\" does not exist for topic \"" + topic + "\"",
                    checkSubscribers(subscribers, testData[1]));

            // check whether the unsubscribed urls were unsubscribed:
            assertFalse("Subscriber \"" + testData[2]
                    + "\" should have been unsubscribed from topic \"" + topic
                    + "\"", checkSubscribers(subscribers, testData[2]));

            assertEquals("Subscribers of topic (" + topic + ") should only be [\""
                    + testData[0] + "\", \"" + testData[1] + "\"], but is: "
                    + subscribers.toString(), 2, subscribers.size());
        } finally {
            conn.closeConnection();
        }
    }

    /**
     * Test method for
     * {@link PubSub#subscribe(String, String)},
     * {@link PubSub#unsubscribe(String, String)} and
     * {@link PubSub#getSubscribers(String)}.
     * Subscribes some arbitrary URLs to arbitrary topics and uses a distinct topic for each URL.
     * Unsubscribes every second subscribed URL.
     *
     * @see #testSubscribe1()
     *
     * @throws UnknownException
     * @throws TimeoutException
     * @throws ConnectionException
     * @throws NotFoundException
     * @throws AbortException
     */
    @Test
    public void testUnsubscribe1() throws ConnectionException,
            TimeoutException, UnknownException, NotFoundException, AbortException {
        String topic = "_UnsubscribeString1_";
        PubSub conn = new PubSub();

        try {
            // first subscribe all test "urls"...
            for (int i = 0; i < testData.length; ++i) {
                conn.subscribe(
                        testTime + topic + i,
                        testData[i]);
            }
            // ... then unsubscribe every second url:
            for (int i = 0; i < testData.length; i += 2) {
                conn.unsubscribe(
                        testTime + topic + i,
                        testData[i]);
            }

            // check whether the subscribers were successfully saved:
            for (int i = 1; i < testData.length; i += 2) {
                String topic1 = topic + i;
                List<String> subscribers = conn.getSubscribers(testTime + topic1).stringListValue();
                assertTrue("Subscriber \"" + testData[i]
                          + "\" does not exist for topic \"" + topic1 + "\"", checkSubscribers(
                          subscribers, testData[i]));

                assertEquals("Subscribers of topic (" + topic1
                        + ") should only be [\"" + testData[i] + "\"], but is: "
                        + subscribers.toString(), 1, subscribers.size());
            }
            // check whether the unsubscribed urls were unsubscribed:
            for (int i = 0; i < testData.length; i += 2) {
                String topic1 = topic + i;
                List<String> subscribers = conn.getSubscribers(testTime
                        + topic1).stringListValue();
                assertFalse("Subscriber \"" + testData[i]
                        + "\" should have been unsubscribed from topic \"" + topic1 + "\"", checkSubscribers(
                        subscribers, testData[i]));

                assertEquals("Subscribers of topic (" + topic1
                        + ") should only be [], but is: "
                        + subscribers.toString(), 0, subscribers.size());
            }
        } finally {
            conn.closeConnection();
        }
    }

    /**
     * Test method for
     * {@link PubSub#subscribe(String, String)},
     * {@link PubSub#unsubscribe(String, String)} and
     * {@link PubSub#getSubscribers(String)}.
     * Subscribes some arbitrary URLs to arbitrary topics and uses a single topic for all URLs.
     * Unsubscribes every second subscribed URL.
     *
     * @see #testSubscribe2()
     *
     * @throws UnknownException
     * @throws TimeoutException
     * @throws ConnectionException
     * @throws NotFoundException
     * @throws AbortException
     */
    @Test
    public void testUnsubscribe2() throws ConnectionException,
            TimeoutException, UnknownException, NotFoundException, AbortException {
        String topic = "_SingleUnubscribeString2";
        PubSub conn = new PubSub();

        try {
            // first subscribe all test "urls"...
            for (int i = 0; i < testData.length; ++i) {
                conn.subscribe(
                        testTime + topic,
                        testData[i]);
            }
            // ... then unsubscribe every second url:
            for (int i = 0; i < testData.length; i += 2) {
                conn.unsubscribe(
                        testTime + topic,
                        testData[i]);
            }

            // check if the subscribers were successfully saved:
            List<String> subscribers = conn.getSubscribers(testTime + topic).stringListValue();
            String[] subscribers_expected = new String[testData.length / 2];
            for (int i = 1; i < testData.length; i += 2) {
                subscribers_expected[i / 2] = testData[i];
                assertTrue("Subscriber \"" + testData[i]
                        + "\" does not exist for topic \"" + topic + "\"",
                        checkSubscribers(subscribers, testData[i]));
            }
            // check whether the unsubscribed urls were unsubscribed:
            for (int i = 0; i < testData.length; i += 2) {
                assertFalse("Subscriber \"" + testData[i]
                        + "\" should have been unsubscribed from topic \"" + topic
                        + "\"", checkSubscribers(subscribers, testData[i]));
            }

            assertEquals("unexpected subscriber of topic \"" + topic + "\"", null, getDiffElement(subscribers, subscribers_expected));
        } finally {
            conn.closeConnection();
        }
    }

    // unsubscribe() test methods end

    /**
     * Creates a new subscription server and tries to start it at {@link #startPort}.
     */
    private static Server newSubscriptionServer()
            throws Exception {
        do {
            Server server = new Server();
            SelectChannelConnector connector = new SelectChannelConnector();
            connector.setHost("127.0.0.1");
            server.addConnector(connector);
            server.setHandler(new SubscriptionHandler());
            server.start();
            return server;
        } while (true);
    }

    private void checkNotifications(Map<String, Vector<String>> notifications, Map<String, Vector<String>> expected) {
        for (Entry<String, Vector<String>> expected_element : expected.entrySet()) {
            String topic = expected_element.getKey();
            Vector<String> notifications_topic = notifications.get(topic);
            for (String content : expected_element.getValue()) {
                assertTrue("subscription (" + topic + ", " + content
                        + ") not received by server)",
                        notifications_topic != null
                                && notifications_topic.contains(content));
                notifications_topic.remove(content);
            }
            if (notifications_topic != null && notifications_topic.size() > 0) {
                fail("Received element (" + topic + ", "
                        + notifications_topic.get(0)
                        + ") which is not part of the subscription.");
            }
            notifications.remove(topic);
        }

        // is there another (unexpected) topic we received content for?
        if (notifications.size() > 0) {
            for (Entry<String, Vector<String>> element : notifications.entrySet()) {
                if (element.getValue().size() > 0) {
                    fail("Received notification for topic (" + element.getKey() + ", "
                            + element.getValue().get(0)
                            + ") which is not part of the subscription.");
                }
            }
        }
    }

    /**
     * Test method for the publish/subscribe system.
     * Single server, subscription to one topic, multiple publishs.
     *
     * @throws Exception
     */
    @Test
    public void testSubscription1() throws Exception {
        String topic = testTime + "_Subscription1";
        PubSub conn = new PubSub();
        Server server1 = newSubscriptionServer();
        Map<String, Vector<String>> notifications_server1_expected = new HashMap<String, Vector<String>>();
        notifications_server1_expected.put(topic, new Vector<String>());

        try {
            conn.subscribe(topic, "http://" + server1.getConnectors()[0].getHost() + ":" + server1.getConnectors()[0].getLocalPort());

            for (int i = 0; i < testData.length; ++i) {
                conn.publish(topic, testData[i]);
                notifications_server1_expected.get(topic).add(testData[i]);
            }

            // wait max 'notifications_timeout' seconds for notifications:
            Map<String, Vector<String>> notifications_server1 =
                ((SubscriptionHandler) server1.getHandler()).notifications;
            for (int i = 0; i < notifications_timeout
                    && (notifications_server1.get(topic) == null ||
                        notifications_server1.get(topic).size() < notifications_server1_expected.get(topic).size()); ++i) {
                TimeUnit.SECONDS.sleep(1);
            }

            server1.stop();

            // check that every notification arrived:
            checkNotifications(notifications_server1, notifications_server1_expected);
        } finally {
            server1.stop();
            conn.closeConnection();
        }
    }

    /**
     * Test method for the publish/subscribe system.
     * Three servers, subscription to one topic, multiple publishs.
     *
     * @throws Exception
     */
    @Test
    public void testSubscription2() throws Exception {
        String topic = testTime + "_Subscription2";
        PubSub conn = new PubSub();
        Server server1 = newSubscriptionServer();
        Server server2 = newSubscriptionServer();
        Server server3 = newSubscriptionServer();
        Map<String, Vector<String>> notifications_server1_expected = new HashMap<String, Vector<String>>();
        notifications_server1_expected.put(topic, new Vector<String>());
        Map<String, Vector<String>> notifications_server2_expected = new HashMap<String, Vector<String>>();
        notifications_server2_expected.put(topic, new Vector<String>());
        Map<String, Vector<String>> notifications_server3_expected = new HashMap<String, Vector<String>>();
        notifications_server3_expected.put(topic, new Vector<String>());

        try {
            conn.subscribe(topic, "http://" + server1.getConnectors()[0].getHost() + ":" + server1.getConnectors()[0].getLocalPort());
            conn.subscribe(topic, "http://" + server1.getConnectors()[0].getHost() + ":" + server2.getConnectors()[0].getLocalPort());
            conn.subscribe(topic, "http://" + server1.getConnectors()[0].getHost() + ":" + server3.getConnectors()[0].getLocalPort());

            for (int i = 0; i < testData.length; ++i) {
                conn.publish(topic, testData[i]);
                notifications_server1_expected.get(topic).add(testData[i]);
                notifications_server2_expected.get(topic).add(testData[i]);
                notifications_server3_expected.get(topic).add(testData[i]);
            }

            // wait max 'notifications_timeout' seconds for notifications:
            Map<String, Vector<String>> notifications_server1 =
                ((SubscriptionHandler) server1.getHandler()).notifications;
            Map<String, Vector<String>> notifications_server2 =
                ((SubscriptionHandler) server2.getHandler()).notifications;
            Map<String, Vector<String>> notifications_server3 =
                ((SubscriptionHandler) server3.getHandler()).notifications;
            for (int i = 0; i < notifications_timeout
                    && (notifications_server1.get(topic) == null ||
                        notifications_server1.get(topic).size() < notifications_server1_expected.get(topic).size()||
                        notifications_server2.get(topic) == null ||
                        notifications_server2.get(topic).size() < notifications_server2_expected.get(topic).size() ||
                        notifications_server3.get(topic) == null ||
                        notifications_server3.get(topic).size() < notifications_server3_expected.get(topic).size()); ++i) {
                TimeUnit.SECONDS.sleep(1);
            }

            server1.stop();
            server2.stop();
            server3.stop();

            // check that every notification arrived:
            checkNotifications(notifications_server1, notifications_server1_expected);
            checkNotifications(notifications_server2, notifications_server2_expected);
            checkNotifications(notifications_server3, notifications_server3_expected);
        } finally {
            server1.stop();
            server2.stop();
            server3.stop();
            conn.closeConnection();
        }
    }

    /**
     * Test method for the publish/subscribe system.
     * Three servers, subscription to different topics, multiple publishs, each
     * server receives a different number of elements.
     *
     * @throws Exception
     */
    @Test
    public void testSubscription3() throws Exception {
        String topic1 = testTime + "_Subscription3_1";
        String topic2 = testTime + "_Subscription3_2";
        String topic3 = testTime + "_Subscription3_3";
        PubSub conn = new PubSub();
        Server server1 = newSubscriptionServer();
        Server server2 = newSubscriptionServer();
        Server server3 = newSubscriptionServer();
        Map<String, Vector<String>> notifications_server1_expected = new HashMap<String, Vector<String>>();
        notifications_server1_expected.put(topic1, new Vector<String>());
        Map<String, Vector<String>> notifications_server2_expected = new HashMap<String, Vector<String>>();
        notifications_server2_expected.put(topic2, new Vector<String>());
        Map<String, Vector<String>> notifications_server3_expected = new HashMap<String, Vector<String>>();
        notifications_server3_expected.put(topic3, new Vector<String>());

        try {
            conn.subscribe(topic1, "http://" + server1.getConnectors()[0].getHost() + ":" + server1.getConnectors()[0].getLocalPort());
            conn.subscribe(topic2, "http://" + server1.getConnectors()[0].getHost() + ":" + server2.getConnectors()[0].getLocalPort());
            conn.subscribe(topic3, "http://" + server1.getConnectors()[0].getHost() + ":" + server3.getConnectors()[0].getLocalPort());

            for (int i = 0; i < testData.length; ++i) {
                if (i % 2 == 0) {
                    conn.publish(topic1, testData[i]);
                    notifications_server1_expected.get(topic1).add(testData[i]);
                }
                if (i % 3 == 0) {
                    conn.publish(topic2, testData[i]);
                    notifications_server2_expected.get(topic2).add(testData[i]);
                }
                if (i % 5 == 0) {
                    conn.publish(topic3, testData[i]);
                    notifications_server3_expected.get(topic3).add(testData[i]);
                }
            }

            // wait max 'notifications_timeout' seconds for notifications:
            Map<String, Vector<String>> notifications_server1 =
                ((SubscriptionHandler) server1.getHandler()).notifications;
            Map<String, Vector<String>> notifications_server2 =
                ((SubscriptionHandler) server2.getHandler()).notifications;
            Map<String, Vector<String>> notifications_server3 =
                ((SubscriptionHandler) server3.getHandler()).notifications;
            for (int i = 0; i < notifications_timeout
                    && (notifications_server1.get(topic1) == null ||
                        notifications_server1.get(topic1).size() < notifications_server1_expected.get(topic1).size() ||
                        notifications_server2.get(topic2) == null ||
                        notifications_server2.get(topic2).size() < notifications_server2_expected.get(topic2).size() ||
                        notifications_server3.get(topic3) == null ||
                        notifications_server3.get(topic3).size() < notifications_server3_expected.get(topic3).size()); ++i) {
                TimeUnit.SECONDS.sleep(1);
            }

            server1.stop();
            server2.stop();
            server3.stop();

            // check that every notification arrived:
            checkNotifications(notifications_server1, notifications_server1_expected);
            checkNotifications(notifications_server2, notifications_server2_expected);
            checkNotifications(notifications_server3, notifications_server3_expected);
        } finally {
            server1.stop();
            server2.stop();
            server3.stop();
            conn.closeConnection();
        }
    }

    /**
     * Test method for the publish/subscribe system.
     *
     * Like {@link #testSubscription3()} but some subscribed urls will be unsubscribed.
     *
     * @throws Exception
     */
    @Test
    public void testSubscription4() throws Exception {
        String topic1 = testTime + "_Subscription4_1";
        String topic2 = testTime + "_Subscription4_2";
        String topic3 = testTime + "_Subscription4_3";
        PubSub conn = new PubSub();
        Server server1 = newSubscriptionServer();
        Server server2 = newSubscriptionServer();
        Server server3 = newSubscriptionServer();
        Map<String, Vector<String>> notifications_server1_expected = new HashMap<String, Vector<String>>();
        notifications_server1_expected.put(topic1, new Vector<String>());
        Map<String, Vector<String>> notifications_server2_expected = new HashMap<String, Vector<String>>();
        notifications_server2_expected.put(topic2, new Vector<String>());
        Map<String, Vector<String>> notifications_server3_expected = new HashMap<String, Vector<String>>();
        notifications_server3_expected.put(topic3, new Vector<String>());

        try {
            conn.subscribe(topic1, "http://" + server1.getConnectors()[0].getHost() + ":" + server1.getConnectors()[0].getLocalPort());
            conn.subscribe(topic2, "http://" + server1.getConnectors()[0].getHost() + ":" + server2.getConnectors()[0].getLocalPort());
            conn.subscribe(topic3, "http://" + server1.getConnectors()[0].getHost() + ":" + server3.getConnectors()[0].getLocalPort());
            conn.unsubscribe(topic2, "http://" + server1.getConnectors()[0].getHost() + ":" + server2.getConnectors()[0].getLocalPort());

            for (int i = 0; i < testData.length; ++i) {
                if (i % 2 == 0) {
                    conn.publish(topic1, testData[i]);
                    notifications_server1_expected.get(topic1).add(testData[i]);
                }
                if (i % 3 == 0) {
                    conn.publish(topic2, testData[i]);
                    // note: topic2 is unsubscribed
//                    notifications_server2_expected.get(topic2).add(testData[i]);
                }
                if (i % 5 == 0) {
                    conn.publish(topic3, testData[i]);
                    notifications_server3_expected.get(topic3).add(testData[i]);
                }
            }

            // wait max 'notifications_timeout' seconds for notifications:
            Map<String, Vector<String>> notifications_server1 =
                ((SubscriptionHandler) server1.getHandler()).notifications;
            Map<String, Vector<String>> notifications_server2 =
                ((SubscriptionHandler) server2.getHandler()).notifications;
            Map<String, Vector<String>> notifications_server3 =
                ((SubscriptionHandler) server3.getHandler()).notifications;
            for (int i = 0; i < notifications_timeout
                    && (notifications_server1.get(topic1) == null ||
                        notifications_server1.get(topic1).size() < notifications_server1_expected.get(topic1).size() ||
//                        notifications_server3.get(topic2) == null ||
//                        notifications_server3.get(topic2).size() < notifications_server2_expected.get(topic2).size() ||
                        notifications_server3.get(topic3) == null ||
                        notifications_server3.get(topic3).size() < notifications_server3_expected.get(topic3).size()); ++i) {
                TimeUnit.SECONDS.sleep(1);
            }

            server1.stop();
            server2.stop();
            server3.stop();

            // check that every notification arrived:
            checkNotifications(notifications_server1, notifications_server1_expected);
            checkNotifications(notifications_server2, notifications_server2_expected);
            checkNotifications(notifications_server3, notifications_server3_expected);
        } finally {
            server1.stop();
            server2.stop();
            server3.stop();
            conn.closeConnection();
        }
    }

    private static class SubscriptionHandler extends AbstractHandler {
        public Map<String, Vector<String>> notifications = new HashMap<String, Vector<String>>();

        public SubscriptionHandler() {
        }

        private String[] getParametersFromJSON(Reader reader)
                throws IOException {
            String[] result;
            /**
             * {"method":"notify","params":["1209386211287_SubscribeTest","content"],"id":482975}
             */

            Object json = JSON.parse(reader);
            if (json instanceof Map<?, ?>) {
                @SuppressWarnings("unchecked")
                Map<String, Object> json_object = (Map<String, Object>) json;
                result = new String[json_object.size()];

                if (json_object.get("method").equals("notify")) {
                    Object[] params = (Object[]) json_object.get("params");
                    for (int i = 0; i < params.length; i++) {
                        result[i] = (String) params[i];
                    }
                }
                return result;
            } else {
                return null;
            }
        }

        public void handle(String target, Request baseRequest,
                HttpServletRequest request, HttpServletResponse response)
                throws IOException, ServletException {
            response.setContentType("text/html");
            response.setStatus(HttpServletResponse.SC_OK);
            PrintWriter out = response.getWriter();

            String[] params = getParametersFromJSON(request.getReader());
            if (params.length >= 2) {
                String topic = params[0];
                String content = params[1];

                synchronized (this) {
                    Vector<String> l = notifications.get(topic);
                    if (l == null) {
                        notifications.put(topic, l = new Vector<String>());
                    }
                    l.add(content);
                }

//              System.out.print(content + " ");
//                notifications.put(topic, content);
            }

            out.println("{}");

            ((Request) request).setHandled(true);
        }
    }
}
