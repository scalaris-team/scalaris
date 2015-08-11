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

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TreeSet;

import org.junit.Test;

import de.zib.scalaris.Transaction.RequestList;
import de.zib.scalaris.Transaction.ResultList;
import de.zib.scalaris.operations.ReadOp;
import de.zib.scalaris.operations.WriteOp;


/**
 * Unit test for the {@link Transaction} class.
 *
 * @author Nico Kruber, kruber@zib.de
 * @version 3.17
 * @since 2.0
 */
public class TransactionTest {
    private static long testTime = System.currentTimeMillis();

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
        // determine good/bad nodes:
        final ConnectionFactory cf = ConnectionFactory.getInstance();
        cf.testAllNodes();
        // set not to automatically try reconnects (auto-retries prevent ConnectionException tests from working):
        ((DefaultConnectionPolicy) cf.getConnectionPolicy()).setMaxRetries(0);
    }

    /**
     * Test method for {@link Transaction#Transaction()}.
     * @throws ConnectionException
     */
    @Test
    public void testTransaction1() throws ConnectionException {
        final Transaction t = new Transaction();
        t.closeConnection();
    }

    /**
     * Test method for {@link Transaction#Transaction(Connection)}.
     * @throws ConnectionException
     */
    @Test
    public void testTransaction2() throws ConnectionException {
        final Transaction t = new Transaction(ConnectionFactory.getInstance().createConnection("test"));
        t.closeConnection();
    }

    /**
     * Test method for {@link Transaction#closeConnection()} trying to
     * close the connection twice.
     *
     * @throws UnknownException
     * @throws ConnectionException
     */
    @Test
    public void testDoubleClose() throws ConnectionException {
        final Transaction t = new Transaction(ConnectionFactory.getInstance().createConnection("test"));
        t.closeConnection();
        t.closeConnection();
    }

    /**
     * Test method for {@link Transaction#commit()} with a closed connection.
     *
     * @throws UnknownException
     * @throws ConnectionException
     * @throws NotFoundException
     * @throws AbortException
     */
    @Test(expected=ConnectionException.class)
    public void testCommit_NotConnected() throws ConnectionException,
            UnknownException, NotFoundException, AbortException {
        final Transaction t = new Transaction();
        try {
            t.closeConnection();
            t.commit();
        } finally {
            t.closeConnection();
        }
    }

    /**
     * Test method for {@link Transaction#commit()} which commits an empty
     * transaction.
     *
     * @throws ConnectionException
     * @throws UnknownException
     * @throws AbortException
     */
    @Test
    public void testCommit_Empty() throws ConnectionException,
            UnknownException, AbortException {
        final Transaction t = new Transaction();
        try {
            t.commit();
        } finally {
            t.closeConnection();
        }
    }

    /**
     * Test method for {@link Transaction#abort()} with a closed connection.
     *
     * @throws UnknownException
     * @throws ConnectionException
     * @throws NotFoundException
     */
    @Test
    public void testAbort_NotConnected() throws ConnectionException,
            UnknownException, NotFoundException {
        final Transaction t = new Transaction();
        t.closeConnection();
        t.abort();
    }

    /**
     * Test method for {@link Transaction#abort()} which aborts an empty
     * transaction.
     *
     * @throws UnknownException
     * @throws ConnectionException
     */
    @Test
    public void testAbort_Empty() throws ConnectionException, UnknownException {
        final Transaction t = new Transaction();
        try {
            t.abort();
        } finally {
            t.closeConnection();
        }
    }

    /**
     * Test method for {@link Transaction#read(String)}.
     *
     * @throws UnknownException
     * @throws ConnectionException
     * @throws NotFoundException
     */
    @Test(expected = NotFoundException.class)
    public void testRead_NotFound() throws ConnectionException,
            UnknownException, NotFoundException {
        final String key = "_Read_NotFound";
        final Transaction t = new Transaction();
        try {
            t.read(testTime + key);
        } finally {
            t.closeConnection();
        }
    }

    /**
     * Test method for {@link Transaction#read(String)} with a closed
     * connection.
     *
     * @throws UnknownException
     * @throws ConnectionException
     * @throws NotFoundException
     */
    @Test(expected = ConnectionException.class)
    public void testRead_NotConnected() throws ConnectionException,
            UnknownException, NotFoundException {
        final String key = "_Read_NotConnected";
        final Transaction t = new Transaction();
        try {
            t.closeConnection();
            t.read(testTime + key);
        } finally {
            t.closeConnection();
        }
    }

    /**
     * Test method for {@link Transaction#write(String, Object)} with a closed
     * connection.
     *
     * @throws UnknownException
     * @throws ConnectionException
     * @throws NotFoundException
     */
    @Test(expected = ConnectionException.class)
    public void testWriteString_NotConnected() throws ConnectionException,
            UnknownException, NotFoundException {
        final String key = "_WriteString_NotConnected";
        final Transaction t = new Transaction();
        try {
            t.closeConnection();
            t.write(testTime + key, testData[0]);
        } finally {
            t.closeConnection();
        }
    }

    /**
     * Test method for {@link Transaction#read(String)} and
     * {@link Transaction#write(String, Object)} which should show that
     * writing a value for a key for which a previous read returned a
     * NotFoundException is possible.
     *
     * @throws UnknownException
     * @throws ConnectionException
     * @throws NotFoundException
     */
    @Test
    public void testWriteString_NotFound() throws ConnectionException,
            UnknownException, NotFoundException {
        final String key = "_WriteString_notFound";
        final Transaction t = new Transaction();
        try {
            try {
                t.read(testTime + key);
                assertTrue(false);
            } catch (final NotFoundException e) {
            }
            t.write(testTime + key, testData[0]);

            assertEquals(testData[0], t.read(testTime + key).stringValue());
        } finally {
            t.closeConnection();
        }
    }

    /**
     * Test method for {@link Transaction#write(String, Object)} and
     * {@link Transaction#read(String)}. Writes strings and uses a distinct key
     * for each value. Tries to read the data afterwards.
     *
     * @throws ConnectionException
     * @throws NotFoundException
     * @throws UnknownException
     * @throws AbortException
     *
     * @since 3.9
     */
    @Test
    public void testWriteString1() throws ConnectionException,
            UnknownException, NotFoundException, AbortException {
        final String key = "_testWriteString1_";
        Transaction t = new Transaction();
        try {
            for (int i = 0; i < testData.length; ++i) {
                t.write(testTime + key + i, testData[i]);
            }

            // now try to read the data:
            for (int i = 0; i < testData.length; ++i) {
                final String actual = t.read(testTime + key + i).stringValue();
                assertEquals(testData[i], actual);
            }

            // try to read the data with another transaction (keys should not exist):
            do {
                final Transaction t2 = new Transaction();
                for (int i = 0; i < testData.length; ++i) {
                    try {
                        t2.read(testTime + key + i).stringValue();
                        // a key changed exception must be thrown
                        assertTrue(false);
                    } catch (final NotFoundException e) {
                        t2.abort();
                    }
                }
            } while (false);

            // commit the transaction and try to read the data with a new one:
            t.commit();
            t = new Transaction();
            for (int i = 0; i < testData.length; ++i) {
                final String actual = t.read(testTime + key + i).stringValue();
                assertEquals(testData[i], actual);
            }
        } finally {
            t.closeConnection();
        }
    }

    /**
     * Test method for {@link Transaction#write(String, Object)} and
     * {@link Transaction#read(String)}. Writes strings and uses a single key
     * for all the values. Tries to read the data afterwards.
     *
     * @throws ConnectionException
     * @throws NotFoundException
     * @throws UnknownException
     * @throws AbortException
     *
     * @since 3.9
     */
    @Test
    public void testWriteString2() throws ConnectionException,
            UnknownException, NotFoundException, AbortException {
        final String key = "_testWriteString2_";
        Transaction t = new Transaction();
        try {
            for (final String element : testData) {
                t.write(testTime + key, element);
            }

            // now try to read the data:
            final String actual1 = t.read(testTime + key).stringValue();
            assertEquals(testData[testData.length - 1], actual1);

            // try to read the data with another transaction (keys should not exist):
            do {
                final Transaction t2 = new Transaction();
                try {
                    t2.read(testTime + key).stringValue();
                    // a key changed exception must be thrown
                    assertTrue(false);
                } catch (final NotFoundException e) {
                    t2.abort();
                }
            } while (false);

            // commit the transaction and try to read the data with a new one:
            t.commit();
            t = new Transaction();
            final String actual2 = t.read(testTime + key).stringValue();
            assertEquals(testData[testData.length - 1], actual2);
        } finally {
            t.closeConnection();
        }
    }

    /**
     * Test method for {@link Transaction#write(String, List)} and
     * {@link Transaction#read(String)}. Writes lists and uses a single key for
     * all the values. Tries to read the data afterwards.
     *
     * @throws NotFoundException
     * @throws UnknownException
     * @throws ConnectionException
     * @throws AbortException
     *
     * @since 3.9
     */
    @Test
    public void testWriteList1() throws ConnectionException, UnknownException,
            NotFoundException, AbortException {
        final String key = "_testWriteList1_";
        Transaction t = new Transaction();
        try {
            for (int i = 0; i < (testData.length - 1); i += 2) {
                final ArrayList<String> list = new ArrayList<String>();
                list.add(testData[i]);
                list.add(testData[i + 1]);
                t.write(testTime + key + i, list);
            }

            // now try to read the data:
            for (int i = 0; i < (testData.length - 1); i += 2) {
                final List<String> actual = t.read(testTime + key + i).stringListValue();
                final ArrayList<String> expected = new ArrayList<String>();
                expected.add(testData[i]);
                expected.add(testData[i + 1]);
                assertEquals(expected, actual);
            }

            // try to read the data with another transaction (keys should not exist):
            do {
                final Transaction t2 = new Transaction();
                for (int i = 0; i < (testData.length - 1); i += 2) {
                    try {
                        t2.read(testTime + key + i).stringListValue();
                        // a key changed exception must be thrown
                        assertTrue(false);
                    } catch (final NotFoundException e) {
                        t2.abort();
                    }
                }
            } while (false);

            // commit the transaction and try to read the data with a new one:
            t.commit();
            t = new Transaction();
            for (int i = 0; i < (testData.length - 1); i += 2) {
                final ArrayList<String> expected = new ArrayList<String>();
                expected.add(testData[i]);
                expected.add(testData[i + 1]);
                final List<String> actual = t.read(testTime + key + i).stringListValue();

                assertEquals(expected, actual);
            }
        } finally {
            t.closeConnection();
        }
    }

    /**
     * Test method for {@link Transaction#write(String, List)} and
     * {@link Transaction#read(String)}. Writes lists and uses a single
     * key for all the values. Tries to read the data afterwards.
     *
     * @throws NotFoundException
     * @throws UnknownException
     * @throws ConnectionException
     * @throws AbortException
     *
     * @since 3.9
     */
    @Test
    public void testWriteList2() throws ConnectionException, UnknownException,
            NotFoundException, AbortException {
        final String key = "_testWriteList1_";
        Transaction t = new Transaction();
        try {
            final ArrayList<String> list = new ArrayList<String>();
            for (int i = 0; i < (testData.length - 1); i += 2) {
                list.clear();
                list.add(testData[i]);
                list.add(testData[i + 1]);
                t.write(testTime + key, list);
            }

            // now try to read the data:
            final List<String> actual1 = t.read(testTime + key).stringListValue();
            assertEquals(list, actual1);

            // try to read the data with another transaction (keys should not exist):
            do {
                final Transaction t2 = new Transaction();
                try {
                    t2.read(testTime + key).stringValue();
                    // a key changed exception must be thrown
                    assertTrue(false);
                } catch (final NotFoundException e) {
                    t2.abort();
                }
            } while (false);

            // commit the transaction and try to read the data with a new one:
            t.commit();
            t = new Transaction();
            final List<String> actual2 = t.read(testTime + key).stringListValue();
            assertEquals(list, actual2);
        } finally {
            t.closeConnection();
        }
    }

    /**
     * Test method for
     * {@link Transaction#testAndSet(String, Object, List)}
     * with a closed connection.
     *
     * @throws UnknownException
     * @throws ConnectionException
     * @throws NotFoundException
     * @throws AbortException
     * @throws KeyChangedException
     *
     * @since 3.9
     */
    @Test(expected=ConnectionException.class)
    public void testTestAndSetList_NotConnected() throws ConnectionException,
            UnknownException, NotFoundException, AbortException,
            KeyChangedException {
        final String key = "_TestAndSetList_NotConnected";
        final Transaction t = new Transaction();
        t.closeConnection();
        final ArrayList<String> list = new ArrayList<String>();
        list.add(testData[0]);
        list.add(testData[1]);
        t.testAndSet(testTime + key, "fail", list);
    }

    /**
     * Test method for
     * {@link Transaction#testAndSet(String, Object, List)}
     * Tries test_and_set with a non-existing key.
     *
     * @throws UnknownException
     * @throws ConnectionException
     * @throws NotFoundException
     * @throws AbortException
     * @throws KeyChangedException
     *
     * @since 3.9
     */
    @Test(expected=NotFoundException.class)
    public void testTestAndSetList_NotFound() throws ConnectionException,
            UnknownException, NotFoundException, AbortException,
            KeyChangedException {
        final String key = "_TestAndSetList_NotFound";
        final Transaction t = new Transaction();

        try {
            final ArrayList<String> list = new ArrayList<String>();
            list.add(testData[0]);
            list.add(testData[1]);
            t.testAndSet(testTime + key, "fail", list);
        } finally {
            t.closeConnection();
        }
    }

    /**
     * Test method for
     * {@link Transaction#testAndSet(String, List, List)},
     * {@link Transaction#read(String)}
     * and {@link Transaction#write(String, List)}.
     * Writes an erlang list and tries to overwrite it using test_and_set
     * knowing the correct old value. Tries to read the data afterwards.
     *
     * @throws UnknownException
     * @throws ConnectionException
     * @throws NotFoundException
     * @throws AbortException
     * @throws KeyChangedException
     *
     * @since 3.9
     */
    @Test
    public void testTestAndSetList1a() throws ConnectionException,
            UnknownException, NotFoundException, AbortException,
            KeyChangedException {
        final String key = "_TestAndSetList1a";
        Transaction t = new Transaction();

        try {
            // first write all values:
            for (int i = 0; i < (testData.length - 1); i += 2) {
                final ArrayList<String> list = new ArrayList<String>();
                list.add(testData[i]);
                list.add(testData[i + 1]);
                t.write(testTime + key + i, list);
                t.commit();
            }

            // now try to overwrite them using test_and_set:
            for (int i = 0; i < (testData.length - 1); i += 2) {
                final ArrayList<String> old_list = new ArrayList<String>();
                old_list.add(testData[i]);
                old_list.add(testData[i + 1]);
                final ArrayList<String> new_list = new ArrayList<String>();
                new_list.add(testData[i + 1]);
                new_list.add(testData[i]);
                t.testAndSet(testTime + key + i, old_list, new_list);
            }

            // now try to read the data:
            for (int i = 0; i < (testData.length - 1); i += 2) {
                final List<String> actual = t.read(testTime + key + i).stringListValue();
                final ArrayList<String> expected = new ArrayList<String>();
                expected.add(testData[i + 1]);
                expected.add(testData[i]);
                assertEquals(expected, actual);
            }

            // try to read the data with another transaction (verify old value is read):
            do {
                final Transaction t2 = new Transaction();
                for (int i = 0; i < (testData.length - 1); i += 2) {
                    final List<String> actual = t2.read(testTime + key + i).stringListValue();
                    final ArrayList<String> expected = new ArrayList<String>();
                    expected.add(testData[i]);
                    expected.add(testData[i + 1]);
                    assertEquals(expected, actual);
                    t2.commit();
                }
            } while (false);

            // commit the transaction and try to read the data with a new one:
            t.commit();
            t = new Transaction();
            for (int i = 0; i < (testData.length - 1); i += 2) {
                final List<String> actual = t.read(testTime + key + i).stringListValue();
                final ArrayList<String> expected = new ArrayList<String>();
                expected.add(testData[i + 1]);
                expected.add(testData[i]);
                assertEquals(expected, actual);
                t.abort(); // do not accumulate state in tlog
            }
        } finally {
            t.closeConnection();
        }
    }

    /**
     * Test method for
     * {@link Transaction#testAndSet(String, Object, Object)},
     * {@link Transaction#read(String)}
     * and {@link Transaction#write(String, List)}.
     * Writes an erlang list and tries to overwrite it using test_and_set
     * knowing the wrong old value. Tries to read the data afterwards.
     *
     * @throws UnknownException
     * @throws ConnectionException
     * @throws NotFoundException
     * @throws AbortException
     *
     * @since 3.9
     */
    @Test
    public void testTestAndSetList1b() throws ConnectionException,
            UnknownException, NotFoundException, AbortException {
        final String key = "_TestAndSetList1b";
        Transaction t = new Transaction();

        try {
            // first write all values:
            for (int i = 0; i < (testData.length - 1); i += 2) {
                final ArrayList<String> list = new ArrayList<String>();
                list.add(testData[i]);
                list.add(testData[i + 1]);
                t.write(testTime + key + i, list);
                t.commit();
            }

            // now try to overwrite them using test_and_set:
            for (int i = 0; i < (testData.length - 1); i += 2) {
                final int new_value = 1;
                try {
                    t.testAndSet(testTime + key + i, "fail", new_value);
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
                t.read(testTime + key + i).stringListValue();
            }

            // try to read the data with another transaction (verify old value is read):
            do {
                final Transaction t2 = new Transaction();
                for (int i = 0; i < (testData.length - 1); i += 2) {
                    final List<String> actual = t2.read(testTime + key + i).stringListValue();
                    final ArrayList<String> expected = new ArrayList<String>();
                    expected.add(testData[i]);
                    expected.add(testData[i + 1]);
                    assertEquals(expected, actual);
                    t2.commit();
                }
            } while (false);

            // commit the transaction and try to read the data with a new one:
            try {
                t.commit();
                // an AbortException must be thrown
                assertTrue(false);
            } catch (final AbortException e) {
                final TreeSet<String> expFailKeys = new TreeSet<String>();
                for (int i = 0; i < (testData.length - 1); i += 2) {
                    expFailKeys.add(testTime + key + i);
                }
                assertEquals(expFailKeys, new TreeSet<String>(e.getFailedKeys()));
                t.abort();
            }
            t = new Transaction();

            // now try to read the data:
            for (int i = 0; i < (testData.length - 1); i += 2) {
                final List<String> actual = t.read(testTime + key + i).stringListValue();
                final ArrayList<String> expected = new ArrayList<String>();
                expected.add(testData[i]);
                expected.add(testData[i + 1]);
                assertEquals(expected, actual);
                t.abort(); // do not accumulate state in tlog
            }
        } finally {
            t.closeConnection();
        }
    }

    /**
     * Test method for
     * {@link Transaction#testAndSet(String, List, List)},
     * {@link Transaction#read(String)} and
     * {@link Transaction#write(String, List)}. Writes a list and tries
     * to overwrite it using test_and_set knowing the correct old value and
     * using a single key for all the values. Tries to read the data afterwards.
     *
     * @throws UnknownException
     * @throws ConnectionException
     * @throws NotFoundException
     * @throws AbortException
     * @throws KeyChangedException
     *
     * @since 3.9
     */
    @Test
    public void testTestAndSetList2a() throws ConnectionException,
            UnknownException, NotFoundException, AbortException,
            KeyChangedException {
        final String key = "_TestAndSetList2a";
        Transaction t = new Transaction();

        try {
            // first write all values:
            final ArrayList<String> list = new ArrayList<String>();
            list.add(testData[0]);
            list.add(testData[1]);
            t.write(testTime + key, list);
            t.commit();

            // now try to overwrite them using test_and_set:
            for (int i = 1; i < (testData.length - 1); ++i) {
                final ArrayList<String> old_list = new ArrayList<String>();
                old_list.add(testData[i - 1]);
                old_list.add(testData[i]);
                final ArrayList<String> new_list = new ArrayList<String>();
                new_list.add(testData[i]);
                new_list.add(testData[i + 1]);
                t.testAndSet(testTime + key, old_list, new_list);
            }

            final ArrayList<String> expected = new ArrayList<String>();
            expected.add(testData[testData.length - 2]);
            expected.add(testData[testData.length - 1]);

            // now try to read the data:
            final List<String> actual1 = t.read(testTime + key).stringListValue();
            assertEquals(expected, actual1);

            // try to read the data with another transaction (verify old value is read):
            do {
                final Transaction t2 = new Transaction();
                final List<String> actual = t2.read(testTime + key).stringListValue();
                assertEquals(list, actual);
                t2.commit();
            } while (false);

            // commit the transaction and try to read the data with a new one:
            t.commit();
            t = new Transaction();
            final List<String> actual2 = t.read(testTime + key).stringListValue();
            assertEquals(expected, actual2);
        } finally {
            t.closeConnection();
        }
    }

    /**
     * Test method for
     * {@link Transaction#testAndSet(String, Object, Object)},
     * {@link Transaction#read(String)} and
     * {@link Transaction#write(String, List)}.
     * Writes a list and tries to overwrite it using test_and_set knowing the
     * wrong old value and using a single key for all the values. Tries to read
     * the data afterwards.
     *
     * @throws UnknownException
     * @throws ConnectionException
     * @throws NotFoundException
     * @throws AbortException
     *
     * @since 3.9
     */
    @Test
    public void testTestAndSetList2b() throws ConnectionException,
            UnknownException, NotFoundException, AbortException {
        final String key = "_TestAndSetList2b";
        Transaction t = new Transaction();

        try {
            // first write all values:
            final ArrayList<String> list = new ArrayList<String>();
            list.add(testData[0]);
            list.add(testData[1]);
            t.write(testTime + key, list);
            t.commit();

            // now try to overwrite them using test_and_set:
            for (int i = 0; i < (testData.length - 1); ++i) {
                final int new_value = 1;
                try {
                    t.testAndSet(testTime + key, "fail", new_value);
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
            t.read(testTime + key).stringListValue();

            // try to read the data with another transaction (verify old value is read):
            do {
                final Transaction t2 = new Transaction();
                final List<String> actual = t2.read(testTime + key).stringListValue();
                assertEquals(list, actual);
                t2.commit();
            } while (false);

            // commit the transaction and try to read the data with a new one:
            try {
                t.commit();
                // an AbortException must be thrown
                assertTrue(false);
            } catch (final AbortException e) {
                final TreeSet<String> expFailKeys = new TreeSet<String>();
                expFailKeys.add(testTime + key);
                assertEquals(expFailKeys, new TreeSet<String>(e.getFailedKeys()));
                t.abort();
            }
            t = new Transaction();
            final List<String> actual2 = t.read(testTime + key).stringListValue();
            assertEquals(list, actual2);
        } finally {
            t.closeConnection();
        }
    }

    /**
     * Test method for
     * {@link Transaction#testAndSet(String, String, String)}
     * with a closed connection.
     *
     * @throws UnknownException
     * @throws ConnectionException
     * @throws NotFoundException
     * @throws AbortException
     * @throws KeyChangedException
     *
     * @since 3.9
     */
    @Test(expected=ConnectionException.class)
    public void testTestAndSetString_NotConnected() throws ConnectionException,
            UnknownException, NotFoundException, AbortException,
            KeyChangedException {
        final String key = "_TestAndSetString_NotConnected";
        final Transaction t = new Transaction();
        t.closeConnection();
        t.testAndSet(testTime + key, testData[0], testData[1]);
    }

    /**
     * Test method for
     * {@link Transaction#testAndSet(String, String, String)}.
     * Tries test_and_set with a non-existing key.
     *
     * @throws UnknownException
     * @throws ConnectionException
     * @throws NotFoundException
     * @throws AbortException
     * @throws KeyChangedException
     *
     * @since 3.9
     */
    @Test(expected=NotFoundException.class)
    public void testTestAndSetString_NotFound() throws ConnectionException,
            UnknownException, NotFoundException, AbortException,
            KeyChangedException {
        final String key = "_TestAndSetString_NotFound";
        final Transaction t = new Transaction();

        try {
            t.testAndSet(testTime + key, testData[0], testData[1]);
        } finally {
            t.closeConnection();
        }
    }

    /**
     * Test method for
     * {@link Transaction#testAndSet(String, String, String)},
     * {@link Transaction#read(String)}
     * and {@link Transaction#write(String, Object)}.
     * Writes a string and tries to overwrite it using test_and_set
     * knowing the correct old value. Tries to read the string afterwards.
     *
     * @throws UnknownException
     * @throws ConnectionException
     * @throws NotFoundException
     * @throws AbortException
     * @throws KeyChangedException
     *
     * @since 3.9
     */
    @Test
    public void testTestAndSetString1a() throws ConnectionException,
            UnknownException, NotFoundException, AbortException,
            KeyChangedException {
        final String key = "_TestAndSetString1a";
        Transaction t = new Transaction();

        try {
            // first write all values:
            for (int i = 0; i < (testData.length - 1); i += 2) {
                t.write(testTime + key + i, testData[i]);
                t.commit();
            }

            // now try to overwrite them using test_and_set:
            for (int i = 0; i < (testData.length - 1); i += 2) {
                t.testAndSet(testTime + key + i, testData[i], testData[i + 1]);
            }

            // now try to read the data:
            for (int i = 0; i < (testData.length - 1); i += 2) {
                final String actual = t.read(testTime + key + i).stringValue();
                final String expected = testData[i + 1];
                assertEquals(expected, actual);
            }

            // try to read the data with another transaction (verify old value is read):
            do {
                final Transaction t2 = new Transaction();
                for (int i = 0; i < (testData.length - 1); i += 2) {
                    final String actual = t2.read(testTime + key + i).stringValue();
                    final String expected = testData[i];
                    assertEquals(expected, actual);
                    t2.commit();
                }
            } while (false);

            // commit the transaction and try to read the data with a new one:
            t.commit();
            t = new Transaction();
            for (int i = 0; i < (testData.length - 1); i += 2) {
                final String actual = t.read(testTime + key + i).stringValue();
                final String expected = testData[i + 1];
                assertEquals(expected, actual);
                t.abort(); // do not accumulate state in tlog
            }
        } finally {
            t.closeConnection();
        }
    }

    /**
     * Test method for
     * {@link Transaction#testAndSet(String, String, String)},
     * {@link Transaction#read(String)}
     * and {@link Transaction#write(String, Object)}.
     * Writes a string and tries to overwrite it using test_and_set
     * knowing the wrong old value. Tries to read the string afterwards.
     *
     * @throws UnknownException
     * @throws ConnectionException
     * @throws NotFoundException
     * @throws AbortException
     *
     * @since 3.9
     */
    @Test
    public void testTestAndSetString1b() throws ConnectionException,
            UnknownException, NotFoundException, AbortException {
        final String key = "_TestAndSetString1b";
        Transaction t = new Transaction();

        try {
            // first write all values:
            for (int i = 0; i < (testData.length - 1); i += 2) {
                t.write(testTime + key + i, testData[i]);
                t.commit();
            }

            // now try to overwrite them using test_and_set:
            for (int i = 0; i < (testData.length - 1); i += 2) {
                try {
                    t.testAndSet(testTime + key + i, testData[i + 1], "fail");
                    // a key changed exception must be thrown
                    assertTrue(false);
                } catch (final KeyChangedException e) {
                    assertEquals(testData[i], e.getOldValue().stringValue());
                }
            }

            // now try to read the data:
            for (int i = 0; i < (testData.length - 1); i += 2) {
                t.read(testTime + key + i).stringValue();
            }

            // try to read the data with another transaction (verify old value is read):
            do {
                final Transaction t2 = new Transaction();
                for (int i = 0; i < (testData.length - 1); i += 2) {
                    final String actual = t2.read(testTime + key + i).stringValue();
                    final String expected = testData[i];
                    assertEquals(expected, actual);
                    t2.commit();
                }
            } while (false);

            // commit the transaction and try to read the data with a new one:
            try {
                t.commit();
                // an AbortException must be thrown
                assertTrue(false);
            } catch (final AbortException e) {
                final TreeSet<String> expFailKeys = new TreeSet<String>();
                for (int i = 0; i < (testData.length - 1); i += 2) {
                    expFailKeys.add(testTime + key + i);
                }
                assertEquals(expFailKeys, new TreeSet<String>(e.getFailedKeys()));
                t.abort();
            }
            t = new Transaction();
            for (int i = 0; i < (testData.length - 1); i += 2) {
                final String actual = t.read(testTime + key + i).stringValue();
                final String expected = testData[i];
                assertEquals(expected, actual);
                t.abort(); // do not accumulate state in tlog
            }
        } finally {
            t.closeConnection();
        }
    }

    /**
     * Test method for
     * {@link Transaction#testAndSet(String, String, String)},
     * {@link Transaction#read(String)} and
     * {@link Transaction#write(String, String)}. Writes a string and tries
     * to overwrite it using test_and_set knowing the correct old value and
     * using a single key for all the values. Tries to read the data afterwards.
     *
     * @throws UnknownException
     * @throws ConnectionException
     * @throws NotFoundException
     * @throws AbortException
     * @throws KeyChangedException
     *
     * @since 3.9
     */
    @Test
    public void testTestAndSetString2a() throws ConnectionException,
            UnknownException, NotFoundException, AbortException,
            KeyChangedException {
        final String key = "_TestAndSetString2a";
        Transaction t = new Transaction();

        try {
            // first write all values:
            t.write(testTime + key, testData[0]);
            t.commit();

            // now try to overwrite them using test_and_set:
            for (int i = 1; i < (testData.length - 1); ++i) {
                final String old_value = testData[i - 1];
                final String new_value = testData[i];
                t.testAndSet(testTime + key, old_value, new_value);
            }

            final String expected = testData[testData.length - 2];

            // now try to read the data:
            final String actual1 = t.read(testTime + key).stringValue();
            assertEquals(expected, actual1);

            // try to read the data with another transaction (verify old value is read):
            do {
                final Transaction t2 = new Transaction();
                final String actual = t2.read(testTime + key).stringValue();
                assertEquals(testData[0], actual);
                t2.commit();
            } while (false);

            // commit the transaction and try to read the data with a new one:
            t.commit();
            t = new Transaction();
            final String actual2 = t.read(testTime + key).stringValue();
            assertEquals(expected, actual2);
        } finally {
            t.closeConnection();
        }
    }

    /**
     * Test method for
     * {@link Transaction#testAndSet(String, Object, Object)},
     * {@link Transaction#read(String)} and
     * {@link Transaction#write(String, String)}.
     * Writes a string and tries to overwrite it using test_and_set knowing the
     * wrong old value and using a single key for all the values. Tries to read
     * the data afterwards.
     *
     * @throws UnknownException
     * @throws ConnectionException
     * @throws NotFoundException
     * @throws AbortException
     *
     * @since 3.9
     */
    @Test
    public void testTestAndSetString2b() throws ConnectionException,
            UnknownException, NotFoundException, AbortException {
        final String key = "_TestAndSetString2b";
        Transaction t = new Transaction();

        try {
            // first write all values:
            final String expected = testData[0];
            t.write(testTime + key, expected);
            t.commit();

            // now try to overwrite them using test_and_set:
            for (int i = 0; i < (testData.length - 1); ++i) {
                final int new_value = 1;
                try {
                    t.testAndSet(testTime + key, "fail", new_value);
                    // a key changed exception must be thrown
                    assertTrue(false);
                } catch (final KeyChangedException e) {
                    assertEquals(expected, e.getOldValue().stringValue());
                }
            }

            // now try to read the data:
            t.read(testTime + key).stringValue();

            // try to read the data with another transaction (verify old value is read):
            do {
                final Transaction t2 = new Transaction();
                final String actual = t2.read(testTime + key).stringValue();
                assertEquals(expected, actual);
                t2.commit();
            } while (false);

            // commit the transaction and try to read the data with a new one:
            try {
                t.commit();
                // an AbortException must be thrown
                assertTrue(false);
            } catch (final AbortException e) {
                final TreeSet<String> expFailKeys = new TreeSet<String>();
                expFailKeys.add(testTime + key);
                assertEquals(expFailKeys, new TreeSet<String>(e.getFailedKeys()));
                t.abort();
            }
            t = new Transaction();
            final String actual2 = t.read(testTime + key).stringValue();
            assertEquals(expected, actual2);
        } finally {
            t.closeConnection();
        }
    }

    /**
     * Test method for {@link Transaction#req_list(RequestList)} with an empty
     * request list.
     *
     * @throws ConnectionException
     * @throws UnknownException
     * @throws AbortException
     */
    @Test
    public void testReqList_Empty() throws ConnectionException,
            UnknownException, AbortException {
        final Transaction t = new Transaction();
        try {
            t.req_list(new RequestList());
        } finally {
            t.closeConnection();
        }
    }

    /**
     * Test method for {@link Transaction#req_list(RequestList)} with a mixed
     * request list.
     *
     * @throws ConnectionException
     * @throws UnknownException
     * @throws AbortException
     * @throws NotFoundException
     */
    @Test
    public void testReqList1() throws ConnectionException, UnknownException,
            AbortException, NotFoundException {
        final String key = "_ReqList1_";
        final Transaction conn = new Transaction();

        try {
            final RequestList readRequests = new RequestList();
            final RequestList firstWriteRequests = new RequestList();
            final RequestList writeRequests = new RequestList();
            for (int i = 0; i < testData.length; ++i) {
                if ((i % 2) == 0) {
                    firstWriteRequests.addOp(new WriteOp(testTime + key + i, testData[i]));
                }
                writeRequests.addOp(new WriteOp(testTime + key + i, testData[i]));
                readRequests.addOp(new ReadOp(testTime + key + i));
            }

            ResultList results = conn.req_list(firstWriteRequests);
            // evaluate the first write results:
            for (int i = 0; i < firstWriteRequests.size(); ++i) {
                results.processWriteAt(i);
            }

            final RequestList requests = new RequestList(readRequests);
            requests.addAll(writeRequests).addCommit();
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

    /**
     * Various tests.
     *
     * @throws ConnectionException
     * @throws NotFoundException
     * @throws UnknownException
     * @throws AbortException
     *
     * @since 3.17
     */
    @Test
    public void testVarious() throws ConnectionException, UnknownException,
            NotFoundException, AbortException {
        writeSingleTest("_0:Šarplaninac:page_", testData[0]);
    }

    protected void writeSingleTest(final String key, final String data)
            throws ConnectionException, UnknownException, ClassCastException,
            NotFoundException, AbortException {
        Transaction t = new Transaction();
        try {
            t.write(testTime + key, data);
            // now try to read the data:
            assertEquals(data, t.read(testTime + key).stringValue());
            // try to read the data with another transaction (keys should not exist):
            do {
                final Transaction t2 = new Transaction();
                try {
                    t2.read(testTime + key).stringValue();
                    // a not found exception must be thrown
                    assertTrue(false);
                } catch (final NotFoundException e) {
                    t2.abort();
                }
            } while (false);

            // commit the transaction and try to read the data with a new one:
            t.commit();
            t = new Transaction();
            assertEquals(data, t.read(testTime + key).stringValue());
        } finally {
            t.closeConnection();
        }
    }

    /**
     * Tests the performance of the Transaction class with a very long list of
     * random strings (enable manually).
     *
     * @throws ConnectionException
     * @throws NotFoundException
     * @throws ClassCastException
     * @throws AbortException
     * @throws UnknownException
     */
//    @Test
    public void testPerformance1() throws ConnectionException,
            NotFoundException, ClassCastException, AbortException,
            UnknownException {
        final String key = "_testPerformance1_";
        final Transaction t = new Transaction();
        try {
            System.out.println(("[" + new Date()).toString() + "] 1a");
            final ArrayList<String> list = new ArrayList<String>(100000);
            for (int i = 0; i < 100000; ++i) {
//                list.add(testData[i % testData.length]);
                try {
                    list.add(Benchmark.getRandom(20, String.class));
                } catch (final Exception e) {
                    e.printStackTrace();
                }
            }
            System.out.println(("[" + new Date()).toString() + "] 1b");
            final Transaction.RequestList reqs = new Transaction.RequestList();
            reqs.addOp(new WriteOp(testTime + key, list)).addCommit();
            System.out.println(("[" + new Date()).toString() + "] 2a");
            t.req_list(reqs);
            System.out.println(("[" + new Date()).toString() + "] 2b");

            // commit the transaction and try to read the data with a new one:
            final ErlangValue readVal = t.read(testTime + key);
            System.out.println(("[" + new Date()).toString() + "] 3");
            final List<String> actual = readVal.stringListValue();
            System.out.println(("[" + new Date()).toString() + "] 4");
            assertEquals(list, actual);
//            reqs = new Transaction.RequestList();
//            reqs.addCommit();
            t.req_list(reqs);
            System.out.println(("[" + new Date()).toString() + "] 5");
        } finally {
            t.closeConnection();
        }
    }
}
