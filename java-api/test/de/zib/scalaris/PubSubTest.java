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
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Reader;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.catalina.Context;
import org.apache.catalina.Lifecycle;
import org.apache.catalina.LifecycleEvent;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.LifecycleListener;
import org.apache.catalina.LifecycleState;
import org.apache.catalina.startup.Tomcat;
import org.eclipse.jetty.util.ajax.JSON;
import org.junit.Test;

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
        final PubSub conn = new PubSub();
        conn.closeConnection();
    }

    /**
     * Test method for
     * {@link PubSub#PubSub(Connection)}.
     * @throws ConnectionException
     */
    @Test
    public void testPubSub2() throws ConnectionException {
        final PubSub conn = new PubSub(ConnectionFactory.getInstance().createConnection("test"));
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
        final TransactionSingleOp conn = new TransactionSingleOp();
        conn.closeConnection();
        conn.closeConnection();
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
        final String topic = "_Publish_NotConnected";
        final PubSub conn = new PubSub();
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
        final String topic = "_Publish1_";
        final PubSub conn = new PubSub();

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
        final String topic = "_Publish2";
        final PubSub conn = new PubSub();

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
        final String topic = "_GetSubscribers_NotConnected";
        final PubSub conn = new PubSub();
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
        final String topic = "_GetSubscribers_NotExistingTopic";
        final PubSub conn = new PubSub();

        try {
            final List<String> subscribers = conn.getSubscribers(testTime + topic).stringListValue();
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
    private boolean checkSubscribers(final List<String> list, final String subscriber) {
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
    private String getDiffElement(final List<String> list, final String[] expectedElements) {
        final List<String> expectedElements2 = new Vector<String>(Arrays.asList(expectedElements));
        list.removeAll(expectedElements2);

        if (list.size() > 0) {
            return list.get(0);
        } else {
            return null;
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
        final String topic = "_Subscribe_NotConnected";
        final PubSub conn = new PubSub();
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
        final String topic = "_Subscribe1_";
        final PubSub conn = new PubSub();

        try {
            for (int i = 0; i < testData.length; ++i) {
                conn.subscribe(testTime + topic + i, testData[i]);
            }

            // check if the subscribers were successfully saved:
            for (int i = 0; i < testData.length; ++i) {
                final String topic1 = topic + i;
                final List<String> subscribers = conn.getSubscribers(testTime
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
        final String topic = "_Subscribe2";
        final PubSub conn = new PubSub();

        try {
            for (int i = 0; i < testData.length; ++i) {
                conn.subscribe(
                        testTime + topic,
                        testData[i] );
            }

            // check if the subscribers were successfully saved:
            final List<String> subscribers = conn
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
        final String topic = "_Unsubscribe_NotConnected";
        final PubSub conn = new PubSub();
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
     * @throws UnknownException
     * @throws TimeoutException
     * @throws ConnectionException
     * @throws NotFoundException
     * @throws AbortException
     */
    @Test(expected=NotFoundException.class)
    public void testUnsubscribe_NotExistingTopic() throws ConnectionException,
            TimeoutException, UnknownException, NotFoundException, AbortException {
        final String topic = "_Unsubscribe_NotExistingTopic";
        final PubSub conn = new PubSub();

        try {
            // unsubscribe test "url":
            conn.unsubscribe(testTime + topic, testData[0]);

            // check whether the unsubscribed urls were unsubscribed:
            final List<String> subscribers = conn.getSubscribers(testTime + topic).stringListValue();
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
        final String topic = "_Unsubscribe_NotExistingUrl";
        final PubSub conn = new PubSub();

        try {
            // first subscribe test "urls"...
            conn.subscribe(testTime + topic, testData[0]);
            conn.subscribe(testTime + topic, testData[1]);

            // then unsubscribe another "url":
            conn.unsubscribe(testTime + topic, testData[2]);


            // check whether the subscribers were successfully saved:
            final List<String> subscribers = conn.getSubscribers(testTime + topic).stringListValue();
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
        final String topic = "_UnsubscribeString1_";
        final PubSub conn = new PubSub();

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
                final String topic1 = topic + i;
                final List<String> subscribers = conn.getSubscribers(testTime + topic1).stringListValue();
                assertTrue("Subscriber \"" + testData[i]
                          + "\" does not exist for topic \"" + topic1 + "\"", checkSubscribers(
                          subscribers, testData[i]));

                assertEquals("Subscribers of topic (" + topic1
                        + ") should only be [\"" + testData[i] + "\"], but is: "
                        + subscribers.toString(), 1, subscribers.size());
            }
            // check whether the unsubscribed urls were unsubscribed:
            for (int i = 0; i < testData.length; i += 2) {
                final String topic1 = topic + i;
                final List<String> subscribers = conn.getSubscribers(testTime
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
        final String topic = "_SingleUnubscribeString2";
        final PubSub conn = new PubSub();

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
            final List<String> subscribers = conn.getSubscribers(testTime + topic).stringListValue();
            final String[] subscribers_expected = new String[testData.length / 2];
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
    private static class Server {
        Tomcat tomcat;
        SubscriptionServlet[] servlet;

        Server(final Tomcat tomcat, final SubscriptionServlet[] servlet) {
            this.tomcat = tomcat;
            this.servlet = servlet;
        }

        String getHost() {
            return ((Inet4Address) tomcat.getConnector().getProperty("address")).getHostAddress();
        }

        int getLocalPort() {
            return tomcat.getConnector().getLocalPort();
        }

        void stop() throws LifecycleException {
            if ((tomcat.getServer() != null)
                    && (tomcat.getServer().getState() != LifecycleState.DESTROYED)) {
                if (tomcat.getServer().getState() != LifecycleState.STOPPED) {
                    tomcat.stop();
                }

                tomcat.destroy();
            }
        }
    }

    private static class TomcatWaitForStartListener implements LifecycleListener{
        boolean started = false;

        public void lifecycleEvent(final LifecycleEvent lifecycleEvent) {
            if (lifecycleEvent.getType().equals(Lifecycle.START_EVENT)) {
                started = true;
            }
        }

    }

    /**
     * Creates a new subscription server and tries to start it at {@link #startPort}.
     */
    private static Server newSubscriptionServers(final int number)
            throws Exception {
        final String tmpDir = System.getProperty("java.io.tmpdir");
        final Tomcat tomcat = new Tomcat();
        tomcat.setBaseDir(tmpDir);
        // Listen only on localhost
        tomcat.getConnector().setAttribute("address",
                InetAddress.getByName("localhost").getHostAddress());
        // Use random free port
        tomcat.getConnector().setPort(0);
        // prevent the usual startup information being logged
        tomcat.setSilent(true);
        final File docBase = new File(tmpDir);
        final Context context = tomcat.addContext("", docBase.getAbsolutePath());
        final SubscriptionServlet[] servlets = new SubscriptionServlet[number];
        for (int i = 0; i < number; ++i) {
            servlets[i] = new SubscriptionServlet("/" + i);
            Tomcat.addServlet(context, "subscr" + i, servlets[i]);
            context.addServletMapping(servlets[i].path + "/*", "subscr" + i);
        }
//        ((RealmBase) tomcat.getServer().getRealm()).setRealmPath("/realm" + tomcat.getConnector().getPort());
        do {
            final TomcatWaitForStartListener listener = new TomcatWaitForStartListener();
            tomcat.getServer().addLifecycleListener(listener);
            try {
                tomcat.start();
                for (int i = 0; i < 100; ++i) { // wait 10s in total
                    if (listener.started) {
                        return new Server(tomcat, servlets);
                    }
                    Thread.sleep(100);
                }
                assertTrue("Timeout starting server", false);
                return null; // just in case
            } catch (final LifecycleException e) {
                // e.printStackTrace();
            }
        } while (true);
    }

    private void checkNotifications(final Map<String, Vector<String>> notifications, final Map<String, Vector<String>> expected) {
        final List<String> notReceived = new ArrayList<String>();
        final List<String> unrelatedItems = new ArrayList<String>();
        final List<String> unrelatedTopics = new ArrayList<String>();;
        for (final Entry<String, Vector<String>> expected_element : expected.entrySet()) {
            final String topic = expected_element.getKey();
            final Vector<String> notifications_topic = notifications.get(topic);
            for (final String content : expected_element.getValue()) {
                if ((notifications_topic == null) || !notifications_topic.contains(content)) {
                    notReceived.add(topic + ": " + content);
                }
                notifications_topic.remove(content);
            }
            if ((notifications_topic != null) && !notifications_topic.isEmpty()) {
                unrelatedItems.add("(" + topic + ": " + notifications_topic.toString() + ")");
            }
            notifications.remove(topic);
        }

        // is there another (unexpected) topic we received content for?
        if (notifications.size() > 0) {
            for (final Entry<String, Vector<String>> element : notifications.entrySet()) {
                if (element.getValue().size() > 0) {
                    unrelatedTopics.add("(" + element.getKey() + ": " + element.getValue().toString() + ")");
                }
            }
        }

        final String failMsg = "not received: " + notReceived.toString() + "\n"
                + "unrelated items: " + unrelatedItems.toString() + "\n"
                + "unrelated topics: " + unrelatedTopics.toString();
        assertTrue(failMsg, notReceived.isEmpty() && unrelatedItems.isEmpty()
                && unrelatedTopics.isEmpty());
    }

    /**
     * Test method for the publish/subscribe system.
     * Single server, subscription to one topic, multiple publishs.
     *
     * @throws Exception
     */
    @Test
    public void testSubscription1() throws Exception {
        final String topic = testTime + "_Subscription1";
        final PubSub conn = new PubSub();
        final Server server = newSubscriptionServers(1);
        final Map<String, Vector<String>> notifications_server1_expected = new HashMap<String, Vector<String>>();
        notifications_server1_expected.put(topic, new Vector<String>());

        try {
            conn.subscribe(topic, "http://" + server.getHost() + ":" + server.getLocalPort() + server.servlet[0].path);

            for (int i = 0; i < testData.length; ++i) {
                conn.publish(topic, testData[i]);
                notifications_server1_expected.get(topic).add(testData[i]);
            }

            // wait max 'notifications_timeout' seconds for notifications:
            final Map<String, Vector<String>> notifications_server1 =
                server.servlet[0].notifications;
            for (int i = 0; (i < notifications_timeout)
                    && ((notifications_server1.get(topic) == null) ||
                        (notifications_server1.get(topic).size() < notifications_server1_expected.get(topic).size())); ++i) {
                TimeUnit.SECONDS.sleep(1);
            }

            server.stop();

            // check that every notification arrived:
            checkNotifications(notifications_server1, notifications_server1_expected);
        } finally {
            server.stop();
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
        final String topic = testTime + "_Subscription2";
        final PubSub conn = new PubSub();
        final Server server = newSubscriptionServers(3);
        final Map<String, Vector<String>> notifications_server1_expected = new HashMap<String, Vector<String>>();
        notifications_server1_expected.put(topic, new Vector<String>());
        final Map<String, Vector<String>> notifications_server2_expected = new HashMap<String, Vector<String>>();
        notifications_server2_expected.put(topic, new Vector<String>());
        final Map<String, Vector<String>> notifications_server3_expected = new HashMap<String, Vector<String>>();
        notifications_server3_expected.put(topic, new Vector<String>());

        try {
            conn.subscribe(topic, "http://" + server.getHost() + ":" + server.getLocalPort() + server.servlet[0].path);
            conn.subscribe(topic, "http://" + server.getHost() + ":" + server.getLocalPort() + server.servlet[1].path);
            conn.subscribe(topic, "http://" + server.getHost() + ":" + server.getLocalPort() + server.servlet[2].path);

            for (int i = 0; i < testData.length; ++i) {
                conn.publish(topic, testData[i]);
                notifications_server1_expected.get(topic).add(testData[i]);
                notifications_server2_expected.get(topic).add(testData[i]);
                notifications_server3_expected.get(topic).add(testData[i]);
            }

            // wait max 'notifications_timeout' seconds for notifications:
            final Map<String, Vector<String>> notifications_server1 =
                server.servlet[0].notifications;
            final Map<String, Vector<String>> notifications_server2 =
                server.servlet[1].notifications;
            final Map<String, Vector<String>> notifications_server3 =
                server.servlet[2].notifications;
            for (int i = 0; (i < notifications_timeout)
                    && ((notifications_server1.get(topic) == null) ||
                        (notifications_server1.get(topic).size() < notifications_server1_expected.get(topic).size())||
                        (notifications_server2.get(topic) == null) ||
                        (notifications_server2.get(topic).size() < notifications_server2_expected.get(topic).size()) ||
                        (notifications_server3.get(topic) == null) ||
                        (notifications_server3.get(topic).size() < notifications_server3_expected.get(topic).size())); ++i) {
                TimeUnit.SECONDS.sleep(1);
            }

            server.stop();

            // check that every notification arrived:
            checkNotifications(notifications_server1, notifications_server1_expected);
            checkNotifications(notifications_server2, notifications_server2_expected);
            checkNotifications(notifications_server3, notifications_server3_expected);
        } finally {
            server.stop();
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
        final String topic1 = testTime + "_Subscription3_1";
        final String topic2 = testTime + "_Subscription3_2";
        final String topic3 = testTime + "_Subscription3_3";
        final PubSub conn = new PubSub();
        final Server server = newSubscriptionServers(3);
        final Map<String, Vector<String>> notifications_server1_expected = new HashMap<String, Vector<String>>();
        notifications_server1_expected.put(topic1, new Vector<String>());
        final Map<String, Vector<String>> notifications_server2_expected = new HashMap<String, Vector<String>>();
        notifications_server2_expected.put(topic2, new Vector<String>());
        final Map<String, Vector<String>> notifications_server3_expected = new HashMap<String, Vector<String>>();
        notifications_server3_expected.put(topic3, new Vector<String>());

        try {
            conn.subscribe(topic1, "http://" + server.getHost() + ":" + server.getLocalPort() + server.servlet[0].path);
            conn.subscribe(topic2, "http://" + server.getHost() + ":" + server.getLocalPort() + server.servlet[1].path);
            conn.subscribe(topic3, "http://" + server.getHost() + ":" + server.getLocalPort() + server.servlet[2].path);

            for (int i = 0; i < testData.length; ++i) {
                if ((i % 2) == 0) {
                    conn.publish(topic1, testData[i]);
                    notifications_server1_expected.get(topic1).add(testData[i]);
                }
                if ((i % 3) == 0) {
                    conn.publish(topic2, testData[i]);
                    notifications_server2_expected.get(topic2).add(testData[i]);
                }
                if ((i % 5) == 0) {
                    conn.publish(topic3, testData[i]);
                    notifications_server3_expected.get(topic3).add(testData[i]);
                }
            }

            // wait max 'notifications_timeout' seconds for notifications:
            final Map<String, Vector<String>> notifications_server1 =
                server.servlet[0].notifications;
            final Map<String, Vector<String>> notifications_server2 =
                server.servlet[1].notifications;
            final Map<String, Vector<String>> notifications_server3 =
                server.servlet[2].notifications;
            for (int i = 0; (i < notifications_timeout)
                    && ((notifications_server1.get(topic1) == null) ||
                        (notifications_server1.get(topic1).size() < notifications_server1_expected.get(topic1).size()) ||
                        (notifications_server2.get(topic2) == null) ||
                        (notifications_server2.get(topic2).size() < notifications_server2_expected.get(topic2).size()) ||
                        (notifications_server3.get(topic3) == null) ||
                        (notifications_server3.get(topic3).size() < notifications_server3_expected.get(topic3).size())); ++i) {
                TimeUnit.SECONDS.sleep(1);
            }

            server.stop();

            // check that every notification arrived:
            checkNotifications(notifications_server1, notifications_server1_expected);
            checkNotifications(notifications_server2, notifications_server2_expected);
            checkNotifications(notifications_server3, notifications_server3_expected);
        } finally {
            server.stop();
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
        final String topic1 = testTime + "_Subscription4_1";
        final String topic2 = testTime + "_Subscription4_2";
        final String topic3 = testTime + "_Subscription4_3";
        final PubSub conn = new PubSub();
        final Server server = newSubscriptionServers(3);
        final Map<String, Vector<String>> notifications_server1_expected = new HashMap<String, Vector<String>>();
        notifications_server1_expected.put(topic1, new Vector<String>());
        final Map<String, Vector<String>> notifications_server2_expected = new HashMap<String, Vector<String>>();
        notifications_server2_expected.put(topic2, new Vector<String>());
        final Map<String, Vector<String>> notifications_server3_expected = new HashMap<String, Vector<String>>();
        notifications_server3_expected.put(topic3, new Vector<String>());

        try {
            conn.subscribe(topic1, "http://" + server.getHost() + ":" + server.getLocalPort() + server.servlet[0].path);
            conn.subscribe(topic2, "http://" + server.getHost() + ":" + server.getLocalPort() + server.servlet[1].path);
            conn.subscribe(topic3, "http://" + server.getHost() + ":" + server.getLocalPort() + server.servlet[2].path);
            conn.unsubscribe(topic2, "http://" + server.getHost() + ":" + server.getLocalPort() + server.servlet[1].path);

            for (int i = 0; i < testData.length; ++i) {
                if ((i % 2) == 0) {
                    conn.publish(topic1, testData[i]);
                    notifications_server1_expected.get(topic1).add(testData[i]);
                }
                if ((i % 3) == 0) {
                    conn.publish(topic2, testData[i]);
                    // note: topic2 is unsubscribed
//                    notifications_server2_expected.get(topic2).add(testData[i]);
                }
                if ((i % 5) == 0) {
                    conn.publish(topic3, testData[i]);
                    notifications_server3_expected.get(topic3).add(testData[i]);
                }
            }

            // wait max 'notifications_timeout' seconds for notifications:
            final Map<String, Vector<String>> notifications_server1 =
                server.servlet[0].notifications;
            final Map<String, Vector<String>> notifications_server2 =
                server.servlet[1].notifications;
            final Map<String, Vector<String>> notifications_server3 =
                server.servlet[2].notifications;
            for (int i = 0; (i < notifications_timeout)
                    && ((notifications_server1.get(topic1) == null) ||
                        (notifications_server1.get(topic1).size() < notifications_server1_expected.get(topic1).size()) ||
                        (
//                        notifications_server3.get(topic2) == null ||
//                        notifications_server3.get(topic2).size() < notifications_server2_expected.get(topic2).size() ||
                         notifications_server3.get(topic3) == null) ||
                         (notifications_server3.get(topic3).size() < notifications_server3_expected.get(topic3).size())); ++i) {
                TimeUnit.SECONDS.sleep(1);
            }

            server.stop();

            // check that every notification arrived:
            checkNotifications(notifications_server1, notifications_server1_expected);
            checkNotifications(notifications_server2, notifications_server2_expected);
            checkNotifications(notifications_server3, notifications_server3_expected);
        } finally {
            server.stop();
            conn.closeConnection();
        }
    }

    private static class SubscriptionServlet extends HttpServlet {
        private static final long serialVersionUID = 1L;
        final String path;

        public Map<String, Vector<String>> notifications = new HashMap<String, Vector<String>>();

        public SubscriptionServlet(final String path) {
            this.path = path;
        }

        private String[] getParametersFromJSON(final Reader reader)
                throws IOException {
            String[] result;
            /**
             * {"method":"notify","params":["1209386211287_SubscribeTest","content"],"id":482975}
             */

            final Object json = JSON.parse(reader);
            if (json instanceof Map<?, ?>) {
                @SuppressWarnings("unchecked")
                final
                Map<String, Object> json_object = (Map<String, Object>) json;
                result = new String[json_object.size()];

                if (json_object.get("method").equals("notify")) {
                    final Object[] params = (Object[]) json_object.get("params");
                    for (int i = 0; i < params.length; i++) {
                        result[i] = (String) params[i];
                    }
                }
                return result;
            } else {
                return null;
            }
        }

        @Override
        protected void doGet(final HttpServletRequest request,
                final HttpServletResponse response) throws ServletException, IOException {
            System.out.println(request.toString());
        }

        /*
         * (non-Javadoc)
         *
         * @see javax.servlet.http.HttpServlet#doPost(HttpServletRequest request,
         *      HttpServletResponse response)
         */
        @Override
        protected void doPost(final HttpServletRequest request,
                final HttpServletResponse response) throws ServletException, IOException {
            response.setContentType("text/html");
            response.setStatus(HttpServletResponse.SC_OK);
            final PrintWriter out = response.getWriter();

            final String[] params = getParametersFromJSON(request.getReader());
            if (params.length >= 2) {
                final String topic = params[0];
                final String content = params[1];

                synchronized (this) {
                    Vector<String> l = notifications.get(topic);
                    if (l == null) {
                        notifications.put(topic, l = new Vector<String>());
                    }
                    l.add(content);
                }

//              System.out.print(path + "/" + content + " ");
//                notifications.put(topic, content);
            }

            out.println("{}");
        }
    }
}
