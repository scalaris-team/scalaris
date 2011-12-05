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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import de.zib.scalaris.TransactionSingleOp.RequestList;
import de.zib.scalaris.TransactionSingleOp.ResultList;

/**
 * Unit test for the {@link TransactionSingleOp} class.
 *
 * @author Nico Kruber, kruber@zib.de
 * @version 3.2
 * @since 2.0
 */
public class TransactionSingleOpTest {
    private final static long testTime = System.currentTimeMillis();

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
     * {@link TransactionSingleOp#TransactionSingleOp()}.
     * @throws ConnectionException
     */
    @Test
    public void testTransactionSingleOp1() throws ConnectionException {
        final TransactionSingleOp conn = new TransactionSingleOp();
        conn.closeConnection();
    }

    /**
     * Test method for
     * {@link TransactionSingleOp#TransactionSingleOp(Connection)}.
     * @throws ConnectionException
     */
    @Test
    public void testTransactionSingleOp2() throws ConnectionException {
        final TransactionSingleOp conn = new TransactionSingleOp(ConnectionFactory.getInstance().createConnection("test"));
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
     * Test method for {@link TransactionSingleOp#read(String)}.
     *
     * @throws NotFoundException
     * @throws UnknownException
     * @throws TimeoutException
     * @throws ConnectionException
     */
    @Test(expected=NotFoundException.class)
    public void testRead_NotFound() throws ConnectionException,
            TimeoutException, UnknownException, NotFoundException {
        final String key = "_Read_NotFound";
        final TransactionSingleOp conn = new TransactionSingleOp();
        try {
            conn.read(testTime + key);
        } finally {
            conn.closeConnection();
        }
    }

    /**
     * Test method for {@link TransactionSingleOp#read(String)} with a closed connection.
     *
     * @throws NotFoundException
     * @throws UnknownException
     * @throws TimeoutException
     * @throws ConnectionException
     */
    @Test(expected=ConnectionException.class)
    public void testRead_NotConnected() throws ConnectionException,
            TimeoutException, UnknownException, NotFoundException {
        final String key = "_Read_NotConnected";
        final TransactionSingleOp conn = new TransactionSingleOp();
        conn.closeConnection();
        conn.read(testTime + key);
    }

    /**
     * Test method for {@link TransactionSingleOp#write(String, String)} with a
     * closed connection.
     *
     * @throws UnknownException
     * @throws TimeoutException
     * @throws ConnectionException
     * @throws NotFoundException
     * @throws AbortException
     */
    @Test(expected=ConnectionException.class)
    public void testWriteString_NotConnected() throws ConnectionException,
            TimeoutException, UnknownException, NotFoundException, AbortException {
        final String key = "_WriteString_NotConnected";
        final TransactionSingleOp conn = new TransactionSingleOp();
        conn.closeConnection();
        conn.write(testTime + key, testData[0]);
    }

    /**
     * Test method for {@link TransactionSingleOp#write(String, String)} and
     * {@link TransactionSingleOp#read(String)}. Writes strings and uses a
     * distinct key for each value. Tries to read the data afterwards.
     *
     * @throws UnknownException
     * @throws TimeoutException
     * @throws ConnectionException
     * @throws NotFoundException
     * @throws AbortException
     */
    @Test
    public void testWriteString1() throws ConnectionException,
            TimeoutException, UnknownException, NotFoundException, AbortException {
        final String key = "_WriteString1_";
        final TransactionSingleOp conn = new TransactionSingleOp();

        try {
            for (int i = 0; i < testData.length; ++i) {
                conn.write(testTime + key + i, testData[i]);
            }

            // now try to read the data:
            for (int i = 0; i < testData.length; ++i) {
                final String actual = conn.read(testTime + key + i).stringValue();
                assertEquals(testData[i], actual);
            }
        } finally {
            conn.closeConnection();
        }
    }

    /**
     * Test method for {@link TransactionSingleOp#write(String, String)} and
     * {@link TransactionSingleOp#read(String)}. Writes strings and uses a
     * single key for all the values. Tries to read the data afterwards.
     *
     * @throws UnknownException
     * @throws TimeoutException
     * @throws ConnectionException
     * @throws NotFoundException
     * @throws AbortException
     */
    @Test
    public void testWriteString2() throws ConnectionException,
            TimeoutException, UnknownException, NotFoundException, AbortException {
        final String key = "_WriteString2";
        final TransactionSingleOp conn = new TransactionSingleOp();

        try {
            for (int i = 0; i < testData.length; ++i) {
                conn.write(testTime + key, testData[i]);
            }

            // now try to read the data:
            final String actual = conn.read(testTime + key).stringValue();
            assertEquals(testData[testData.length - 1], actual);
        } finally {
            conn.closeConnection();
        }
    }

    /**
     * Test method for {@link TransactionSingleOp#write(String, List)} with a
     * closed connection.
     *
     * @throws UnknownException
     * @throws TimeoutException
     * @throws ConnectionException
     * @throws NotFoundException
     * @throws AbortException
     *
     * @since 3.2
     */
    @Test(expected=ConnectionException.class)
    public void testWriteList_NotConnected() throws ConnectionException,
            TimeoutException, UnknownException, NotFoundException, AbortException {
        final String key = "_WriteList_NotConnected";
        final TransactionSingleOp conn = new TransactionSingleOp();
        conn.closeConnection();
        final ArrayList<String> list = new ArrayList<String>();
        list.add(testData[0]);
        list.add(testData[1]);
        conn.write(testTime + key, list);
    }

    /**
     * Test method for {@link TransactionSingleOp#write(String, List)} and
     * {@link TransactionSingleOp#read(String)}. Writes lists and uses a
     * distinct key for each value. Tries to read the data afterwards.
     *
     * @throws UnknownException
     * @throws TimeoutException
     * @throws ConnectionException
     * @throws NotFoundException
     * @throws AbortException
     *
     * @since 3.2
     */
    @Test
    public void testWriteList1() throws ConnectionException,
            TimeoutException, UnknownException, NotFoundException, AbortException {
        final String key = "_WriteList1_";
        final TransactionSingleOp conn = new TransactionSingleOp();

        try {
            for (int i = 0; i < (testData.length - 1); i += 2) {
                final ArrayList<String> list = new ArrayList<String>();
                list.add(testData[i]);
                list.add(testData[i + 1]);
                conn.write(testTime + key + i, list);
            }

            // now try to read the data:

            for (int i = 0; i < (testData.length - 1); i += 2) {
                final ArrayList<String> expected = new ArrayList<String>();
                expected.add(testData[i]);
                expected.add(testData[i + 1]);
                final List<String> actual = conn.read(testTime + key + i).stringListValue();

                assertEquals(expected, actual);
            }
        } finally {
            conn.closeConnection();
        }
    }

    /**
     * Test method for {@link TransactionSingleOp#write(String, List)} and
     * {@link TransactionSingleOp#read(String)}. Writes lists and uses a single
     * key for all the values. Tries to read the data afterwards.
     *
     * @throws UnknownException
     * @throws TimeoutException
     * @throws ConnectionException
     * @throws NotFoundException
     * @throws AbortException
     *
     * @since 3.2
     */
    @Test
    public void testWriteList2() throws ConnectionException,
            TimeoutException, UnknownException, NotFoundException, AbortException {
        final String key = "_WriteList2";
        final TransactionSingleOp conn = new TransactionSingleOp();

        try {
            final List<String> list = new ArrayList<String>();
            for (int i = 0; i < (testData.length - 1); i += 2) {
                list.clear();
                list.add(testData[i]);
                list.add(testData[i + 1]);
                conn.write(testTime + key, list);
            }

            // now try to read the data:

            final List<String> actual = conn.read(testTime + key).stringListValue();
            final List<String> expected = list;

            assertEquals(expected, actual);
        } finally {
            conn.closeConnection();
        }
    }

    /**
     * Test method for
     * {@link TransactionSingleOp#testAndSet(String, String, List)}
     * with a closed connection.
     *
     * @throws UnknownException
     * @throws TimeoutException
     * @throws ConnectionException
     * @throws NotFoundException
     * @throws AbortException
     * @throws KeyChangedException
     *
     * @since 3.2
     */
    @Test(expected=ConnectionException.class)
    public void testTestAndSetList_NotConnected() throws ConnectionException,
            TimeoutException, UnknownException, NotFoundException, AbortException, KeyChangedException {
        final String key = "_TestAndSetList_NotConnected";
        final TransactionSingleOp conn = new TransactionSingleOp();
        conn.closeConnection();
        final ArrayList<String> list = new ArrayList<String>();
        list.add(testData[0]);
        list.add(testData[1]);
        conn.testAndSet(testTime + key, "fail", list);
    }

    /**
     * Test method for
     * {@link TransactionSingleOp#testAndSet(String, String, List)}.
     * Tries test_and_set with a non-existing key.
     *
     * @throws UnknownException
     * @throws TimeoutException
     * @throws ConnectionException
     * @throws NotFoundException
     * @throws AbortException
     * @throws KeyChangedException
     *
     * @since 3.2
     */
    @Test(expected=NotFoundException.class)
    public void testTestAndSetList_NotFound() throws ConnectionException,
            TimeoutException, UnknownException, NotFoundException, AbortException, KeyChangedException {
        final String key = "_TestAndSetList_NotFound";
        final TransactionSingleOp conn = new TransactionSingleOp();

        try {
            final ArrayList<String> list = new ArrayList<String>();
            list.add(testData[0]);
            list.add(testData[1]);
            conn.testAndSet(testTime + key, "fail", list);
        } finally {
            conn.closeConnection();
        }
    }

    /**
     * Test method for
     * {@link TransactionSingleOp#testAndSet(String, List, List)},
     * {@link TransactionSingleOp#read(String)}
     * and {@link TransactionSingleOp#write(String, List)}.
     * Writes a list and tries to overwrite it using test_and_set
     * knowing the correct old value. Tries to read the data afterwards.
     *
     * @throws UnknownException
     * @throws TimeoutException
     * @throws ConnectionException
     * @throws NotFoundException
     * @throws AbortException
     * @throws KeyChangedException
     *
     * @since 3.9
     */
    @Test
    public void testTestAndSetList1a() throws ConnectionException,
            TimeoutException, UnknownException, NotFoundException, AbortException, KeyChangedException {
        final String key = "_TestAndSetList1a";
        final TransactionSingleOp conn = new TransactionSingleOp();

        try {
            // first write all values:
            for (int i = 0; i < (testData.length - 1); i += 2) {
                final ArrayList<String> list = new ArrayList<String>();
                list.add(testData[i]);
                list.add(testData[i + 1]);
                conn.write(testTime + key + i, list);
            }

            // now try to overwrite them using test_and_set:
            for (int i = 0; i < (testData.length - 1); i += 2) {
                final ArrayList<String> old_list = new ArrayList<String>();
                old_list.add(testData[i]);
                old_list.add(testData[i + 1]);
                final ArrayList<String> new_list = new ArrayList<String>();
                new_list.add(testData[i + 1]);
                new_list.add(testData[i]);
                conn.testAndSet(testTime + key + i, old_list, new_list);
            }

            // now try to read the data:
            for (int i = 0; i < (testData.length - 1); i += 2) {
                final List<String> actual = conn.read(testTime + key + i).stringListValue();
                final ArrayList<String> expected = new ArrayList<String>();
                expected.add(testData[i + 1]);
                expected.add(testData[i]);
                assertEquals(expected, actual);
            }
        } finally {
            conn.closeConnection();
        }
    }

    /**
     * Test method for
     * {@link TransactionSingleOp#testAndSet(String, Object, List)},
     * {@link TransactionSingleOp#read(String)}
     * and {@link TransactionSingleOp#write(String, List)}.
     * Writes a list and tries to overwrite it using test_and_set
     * knowing the wrong old value. Tries to read the data afterwards.
     *
     * @throws UnknownException
     * @throws TimeoutException
     * @throws ConnectionException
     * @throws NotFoundException
     * @throws AbortException
     *
     * @since 3.9
     */
    @Test
    public void testTestAndSetList1b() throws ConnectionException,
            TimeoutException, UnknownException, NotFoundException, AbortException {
        final String key = "_TestAndSetList1b";
        final TransactionSingleOp conn = new TransactionSingleOp();

        try {
            // first write all values:
            for (int i = 0; i < (testData.length - 1); i += 2) {
                final ArrayList<String> list = new ArrayList<String>();
                list.add(testData[i]);
                list.add(testData[i + 1]);
                conn.write(testTime + key + i, list);
            }

            // now try to overwrite them using test_and_set:
            for (int i = 0; i < (testData.length - 1); i += 2) {
                final int new_value = 1;
                try {
                    conn.testAndSet(testTime + key + i, "fail", new_value);
                    // a key changed exception must be thrown
                    assertTrue(false);
                } catch (final KeyChangedException e) {
                    final ArrayList<String> expected = new ArrayList<String>();
                    expected.add(testData[i]);
                    expected.add(testData[i + 1]);
                    assertEquals(expected, e.getOldValue().stringListValue());
                }
            }

            // now try to read the data:
            for (int i = 0; i < (testData.length - 1); i += 2) {
                final List<String> actual = conn.read(testTime + key + i).stringListValue();
                final ArrayList<String> expected = new ArrayList<String>();
                expected.add(testData[i]);
                expected.add(testData[i + 1]);
                assertEquals(expected, actual);
            }
        } finally {
            conn.closeConnection();
        }
    }

    /**
     * Test method for
     * {@link TransactionSingleOp#testAndSet(String, List, List)},
     * {@link TransactionSingleOp#read(String)} and
     * {@link TransactionSingleOp#write(String, List)}. Writes a list and tries
     * to overwrite it using test_and_set knowing the correct old value and
     * using a single key for all the values. Tries to read the data afterwards.
     *
     * @throws UnknownException
     * @throws TimeoutException
     * @throws ConnectionException
     * @throws NotFoundException
     * @throws AbortException
     * @throws KeyChangedException
     *
     * @since 3.9
     */
    @Test
    public void testTestAndSetList2a() throws ConnectionException,
            TimeoutException, UnknownException, NotFoundException, AbortException, KeyChangedException {
        final String key = "_TestAndSetList2a";
        final TransactionSingleOp conn = new TransactionSingleOp();

        try {
            // first write all values:
            final ArrayList<String> list = new ArrayList<String>();
            list.add(testData[0]);
            list.add(testData[1]);
            conn.write(testTime + key, list);

            // now try to overwrite them using test_and_set:
            for (int i = 1; i < (testData.length - 1); ++i) {
                final ArrayList<String> old_list = new ArrayList<String>();
                old_list.add(testData[i - 1]);
                old_list.add(testData[i]);
                final ArrayList<String> new_list = new ArrayList<String>();
                new_list.add(testData[i]);
                new_list.add(testData[i + 1]);
                conn.testAndSet(testTime + key, old_list, new_list);
            }

            // now try to read the data:
            final List<String> actual = conn.read(testTime + key).stringListValue();
            final ArrayList<String> expected = new ArrayList<String>();
            expected.add(testData[testData.length - 2]);
            expected.add(testData[testData.length - 1]);
            assertEquals(expected, actual);
        } finally {
            conn.closeConnection();
        }
    }

    /**
     * Test method for
     * {@link TransactionSingleOp#testAndSet(String, Object, List)},
     * {@link TransactionSingleOp#read(String)} and
     * {@link TransactionSingleOp#write(String, List)}.
     * Writes a list and tries to overwrite it using test_and_set knowing the
     * wrong old value and using a single key for all the values. Tries to read
     * the data afterwards.
     *
     * @throws UnknownException
     * @throws TimeoutException
     * @throws ConnectionException
     * @throws NotFoundException
     * @throws AbortException
     *
     * @since 3.9
     */
    @Test
    public void testTestAndSetList2b() throws ConnectionException,
            TimeoutException, UnknownException, NotFoundException, AbortException {
        final String key = "_TestAndSetList2b";
        final TransactionSingleOp conn = new TransactionSingleOp();

        try {
            // first write all values:
            final ArrayList<String> list = new ArrayList<String>();
            list.add(testData[0]);
            list.add(testData[1]);
            conn.write(testTime + key, list);

            // now try to overwrite them using test_and_set:
            for (int i = 0; i < (testData.length - 1); ++i) {
                final int new_value = 1;
                try {
                    conn.testAndSet(testTime + key, "fail", new_value);
                    // a key changed exception must be thrown
                    assertTrue(false);
                } catch (final KeyChangedException e) {
                    final ArrayList<String> expected = new ArrayList<String>();
                    expected.add(testData[0]);
                    expected.add(testData[1]);
                    assertEquals(expected, e.getOldValue().stringListValue());
                }
            }

            // now try to read the data:
            final List<String> actual = conn.read(testTime + key).stringListValue();
            final ArrayList<String> expected = list;
            assertEquals(expected, actual);
        } finally {
            conn.closeConnection();
        }
    }

    /**
     * Test method for
     * {@link TransactionSingleOp#testAndSet(String, String, String)}
     * with a closed connection.
     *
     * @throws UnknownException
     * @throws TimeoutException
     * @throws ConnectionException
     * @throws NotFoundException
     * @throws AbortException
     * @throws KeyChangedException
     *
     * @since 2.7
     */
    @Test(expected=ConnectionException.class)
    public void testTestAndSetString_NotConnected() throws ConnectionException,
            TimeoutException, UnknownException, NotFoundException, AbortException, KeyChangedException {
        final String key = "_TestAndSetString_NotConnected";
        final TransactionSingleOp conn = new TransactionSingleOp();
        conn.closeConnection();
        conn.testAndSet(testTime + key, testData[0], testData[1]);
    }

    /**
     * Test method for
     * {@link TransactionSingleOp#testAndSet(String, String, String)}.
     * Tries test_and_set with a non-existing key.
     *
     * @throws UnknownException
     * @throws TimeoutException
     * @throws ConnectionException
     * @throws NotFoundException
     * @throws AbortException
     * @throws KeyChangedException
     *
     * @since 2.7
     */
    @Test(expected=NotFoundException.class)
    public void testTestAndSetString_NotFound() throws ConnectionException,
            TimeoutException, UnknownException, NotFoundException, AbortException, KeyChangedException {
        final String key = "_TestAndSetString_NotFound";
        final TransactionSingleOp conn = new TransactionSingleOp();

        try {
            conn.testAndSet(testTime + key, testData[0], testData[1]);
        } finally {
            conn.closeConnection();
        }
    }

    /**
     * Test method for
     * {@link TransactionSingleOp#testAndSet(String, String, String)},
     * {@link TransactionSingleOp#read(String)}
     * and {@link TransactionSingleOp#write(String, Object)}.
     * Writes a string and tries to overwrite it using test_and_set
     * knowing the correct old value. Tries to read the string afterwards.
     *
     * @throws UnknownException
     * @throws TimeoutException
     * @throws ConnectionException
     * @throws NotFoundException
     * @throws AbortException
     * @throws KeyChangedException
     *
     * @since 2.7
     */
    @Test
    public void testTestAndSetString1() throws ConnectionException,
            TimeoutException, UnknownException, NotFoundException, AbortException, KeyChangedException {
        final String key = "_TestAndSetString1";
        final TransactionSingleOp conn = new TransactionSingleOp();

        try {
            // first write all values:
            for (int i = 0; i < (testData.length - 1); i += 2) {
                conn.write(testTime + key + i, testData[i]);
            }

            // now try to overwrite them using test_and_set:
            for (int i = 0; i < (testData.length - 1); i += 2) {
                conn.testAndSet(testTime + key + i, testData[i], testData[i + 1]);
            }

            // now try to read the data:
            for (int i = 0; i < (testData.length - 1); i += 2) {
                assertEquals(testData[i + 1], conn.read(testTime + key + i).stringValue());
            }
        } finally {
            conn.closeConnection();
        }
    }

    /**
     * Test method for
     * {@link TransactionSingleOp#testAndSet(String, String, String)},
     * {@link TransactionSingleOp#read(String)}
     * and {@link TransactionSingleOp#write(String, Object)}.
     * Writes a string and tries to overwrite it using test_and_set
     * knowing the wrong old value. Tries to read the string afterwards.
     *
     * @throws UnknownException
     * @throws TimeoutException
     * @throws ConnectionException
     * @throws NotFoundException
     * @throws AbortException
     *
     * @since 2.7
     */
    @Test
    public void testTestAndSetString2() throws ConnectionException,
            TimeoutException, UnknownException, NotFoundException, AbortException {
        final String key = "_TestAndSetString2";
        final TransactionSingleOp conn = new TransactionSingleOp();

        try {
            // first write all values:
            for (int i = 0; i < (testData.length - 1); i += 2) {
                conn.write(testTime + key + i, testData[i]);
            }

            // now try to overwrite them using test_and_set:
            for (int i = 0; i < (testData.length - 1); i += 2) {
                try {
                    conn.testAndSet(testTime + key + i, testData[i + 1], "fail");
                    // a key changed exception must be thrown
                    assertTrue(false);
                } catch (final KeyChangedException e) {
                    assertEquals(testData[i], e.getOldValue().stringValue());
                }
            }

            // now try to read the data:
            for (int i = 0; i < (testData.length - 1); i += 2) {
                assertEquals(testData[i], conn.read(testTime + key + i).stringValue());
            }
        } finally {
            conn.closeConnection();
        }
    }

    /**
     * Test method for {@link TransactionSingleOp#req_list(RequestList)} with an
     * empty request list.
     *
     * @throws ConnectionException
     * @throws UnknownException
     * @throws AbortException
     * @throws TimeoutException
     */
    @Test
    public void testReqList_Empty() throws ConnectionException, UnknownException, TimeoutException, AbortException {
        final TransactionSingleOp t = new TransactionSingleOp();
        try {
            t.req_list(new RequestList());
        } finally {
            t.closeConnection();
        }
    }

    /**
     * Test method for {@link TransactionSingleOp#req_list(RequestList)} with a
     * mixed request list.
     *
     * @throws ConnectionException
     * @throws UnknownException
     * @throws AbortException
     * @throws TimeoutException
     * @throws NotFoundException
     */
    @Test
    public void testReqList1() throws ConnectionException, UnknownException, TimeoutException, AbortException, NotFoundException {
        final String key = "_ReqList1_";
        final TransactionSingleOp conn = new TransactionSingleOp();

        try {
            final RequestList readRequests = new RequestList();
            final RequestList firstWriteRequests = new RequestList();
            final RequestList writeRequests = new RequestList();
            for (int i = 0; i < testData.length; ++i) {
                if ((i % 2) == 0) {
                    firstWriteRequests.addWrite(testTime + key + i, testData[i]);
                }
                writeRequests.addWrite(testTime + key + i, testData[i]);
                readRequests.addRead(testTime + key + i);
            }

            ResultList results = conn.req_list(firstWriteRequests);
            // evaluate the first write results:
            for (int i = 0; i < firstWriteRequests.size(); ++i) {
                results.processWriteAt(i);
            }

            final RequestList requests = new RequestList(readRequests).addAll(writeRequests);
            results = conn.req_list(requests);
            assertEquals(requests.size(), results.size());

            // now evaluate the read results:
            for (int i = 0; i < readRequests.size(); ++i) {
                if ((i % 2) == 0) {
                    final String actual = results.processReadAt(i).stringValue();
                    assertEquals(testData[i], actual);
                } else {
                    try {
                        results.processReadAt(i);
                        // a not found exception must be thrown
                        assertTrue(false);
                    } catch (final NotFoundException e) {
                    }
                }
            }

            // now evaluate the write results:
            for (int i = 0; i < writeRequests.size(); ++i) {
                final int pos = readRequests.size() + i;
                results.processWriteAt(pos);
            }

            // once again test reads - now all reads should be successful
            results = conn.req_list(readRequests);
            assertEquals(readRequests.size(), results.size());

            // now evaluate the read results:
            for (int i = 0; i < readRequests.size(); ++i) {
                final String actual = results.processReadAt(i).stringValue();
                assertEquals(testData[i], actual);
            }
        } finally {
            conn.closeConnection();
        }
    }
}
