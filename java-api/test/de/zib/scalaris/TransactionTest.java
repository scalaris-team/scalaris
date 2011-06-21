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
import java.util.List;

import org.junit.Test;

import com.ericsson.otp.erlang.OtpErlangList;
import com.ericsson.otp.erlang.OtpErlangObject;
import com.ericsson.otp.erlang.OtpErlangString;

import de.zib.scalaris.Transaction.RequestList;

/**
 * Unit test for the {@link Transaction} class.
 *
 * @author Nico Kruber, kruber@zib.de
 * @version 2.0
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
        // set not to automatically try reconnects (auto-retries prevent ConnectionException tests from working):
        ((DefaultConnectionPolicy) ConnectionFactory.getInstance().getConnectionPolicy()).setMaxRetries(0);
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
     * Test method for {@link TransactionSingleOp#closeConnection()} trying to
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
     * @throws TimeoutException
     * @throws ConnectionException
     * @throws NotFoundException
     * @throws AbortException
     */
    @Test(expected=ConnectionException.class)
    public void testCommit_NotConnected() throws ConnectionException,
            TimeoutException, UnknownException, NotFoundException,
            AbortException {
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
     * @throws TimeoutException
     */
    @Test
    public void testCommit_Empty() throws ConnectionException, UnknownException, TimeoutException, AbortException {
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
     * @throws TimeoutException
     * @throws ConnectionException
     * @throws NotFoundException
     */
    @Test
    public void testAbort_NotConnected() throws ConnectionException,
            TimeoutException, UnknownException, NotFoundException {
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
     * @throws TimeoutException
     */
    @Test(expected = NotFoundException.class)
    public void testRead_NotFound() throws ConnectionException,
            UnknownException,
            TimeoutException, NotFoundException {
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
     * @throws TimeoutException
     * @throws ConnectionException
     * @throws NotFoundException
     */
    @Test(expected = ConnectionException.class)
    public void testRead_NotConnected() throws ConnectionException,
            TimeoutException, UnknownException, NotFoundException {
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
     * Test method for {@link Transaction#read(OtpErlangString)}.
     *
     * @throws UnknownException
     * @throws ConnectionException
     * @throws NotFoundException
     * @throws TimeoutException
     */
    @Test(expected = NotFoundException.class)
    public void testReadOtp_NotFound() throws ConnectionException,
            UnknownException,
            TimeoutException, NotFoundException {
        final String key = "_ReadOtp_NotFound";
        final Transaction t = new Transaction();
        try {
            t.read(new OtpErlangString(testTime + key));
        } finally {
            t.closeConnection();
        }
    }

    /**
     * Test method for {@link Transaction#read(String)} with a closed
     * connection.
     *
     * @throws UnknownException
     * @throws TimeoutException
     * @throws ConnectionException
     * @throws NotFoundException
     */
    @Test(expected = ConnectionException.class)
    public void testReadOtp_NotConnected() throws ConnectionException,
            TimeoutException, UnknownException, NotFoundException {
        final String key = "_ReadOtp_NotConnected";
        final Transaction t = new Transaction();
        try {
            t.closeConnection();
            t.read(new OtpErlangString(testTime + key));
        } finally {
            t.closeConnection();
        }
    }

    /**
     * Test method for {@link Transaction#write(String, Object)} with a closed
     * connection.
     *
     * @throws UnknownException
     * @throws TimeoutException
     * @throws ConnectionException
     * @throws NotFoundException
     */
    @Test(expected = ConnectionException.class)
    public void testWriteString_NotConnected() throws ConnectionException,
            TimeoutException, UnknownException, NotFoundException {
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
     * @throws TimeoutException
     * @throws NotFoundException
     */
    @Test
    public void testWriteString_NotFound() throws ConnectionException, UnknownException, TimeoutException, NotFoundException {
        final String key = "_WriteString_notFound";
        final Transaction t = new Transaction();
        try {
            boolean notFound = false;
            try {
                t.read(testTime + key);
            } catch (final NotFoundException e) {
                notFound = true;
            }
            assertTrue(notFound);
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
     * @throws TimeoutException
     * @throws AbortException
     */
    @Test
    public void testWriteString() throws ConnectionException, TimeoutException, UnknownException, NotFoundException, AbortException {
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
     * Test method for {@link Transaction#write(String, List)} and
     * {@link Transaction#read(String)}. Writes lists and uses a single key for
     * all the values. Tries to read the data afterwards.
     *
     * @throws NotFoundException
     * @throws UnknownException
     * @throws TimeoutException
     * @throws ConnectionException
     * @throws AbortException
     */
    @Test
    public void testWriteList() throws ConnectionException,
            TimeoutException, UnknownException, NotFoundException, AbortException {
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
     * Test method for
     * {@link Transaction#write(OtpErlangString, OtpErlangObject)} with a
     * closed connection.
     *
     * @throws UnknownException
     * @throws TimeoutException
     * @throws ConnectionException
     * @throws NotFoundException
     */
    @Test(expected = ConnectionException.class)
    public void testWriteOtpString_NotConnected() throws ConnectionException,
            TimeoutException, UnknownException, NotFoundException {
        final String key = "_WriteOtp_NotConnected";
        final Transaction t = new Transaction();
        try {
            t.closeConnection();
            t.write(
                    new OtpErlangString(testTime + key),
                    new OtpErlangString(testData[0]));
        } finally {
            t.closeConnection();
        }
    }

    /**
     * Test method for {@link Transaction#read(OtpErlangString)} and
     * {@link Transaction#write(OtpErlangString, OtpErlangObject)} which
     * should show that writing a value for a key for which a previous read
     * returned a NotFoundException is possible.
     *
     * @throws UnknownException
     * @throws ConnectionException
     * @throws TimeoutException
     * @throws NotFoundException
     */
    @Test
    public void testWriteOtpString_NotFound() throws ConnectionException, UnknownException, TimeoutException, NotFoundException {
        final String key = "_WriteOtp_notFound";
        final Transaction t = new Transaction();
        try {
            boolean notFound = false;
            try {
                t.read(new OtpErlangString(testTime + key));
            } catch (final NotFoundException e) {
                notFound = true;
            }
            assertTrue(notFound);
            t.write(
                    new OtpErlangString(testTime + key),
                    new OtpErlangString(testData[0]));

            assertEquals(testData[0], t.read(testTime + key).stringValue());
        } finally {
            t.closeConnection();
        }
    }

    /**
     * Test method for
     * {@link Transaction#write(OtpErlangString, OtpErlangObject)} and
     * {@link Transaction#read(OtpErlangString)}.
     *
     * @throws NotFoundException
     * @throws UnknownException
     * @throws TimeoutException
     * @throws ConnectionException
     * @throws AbortException
     */
    @Test
    public void testWriteOtpList() throws ConnectionException,
            TimeoutException, UnknownException, NotFoundException, AbortException {
        final String key = "_testWriteOtpList1_";
        Transaction t = new Transaction();
        try {
            for (int i = 0; i < (testData.length - 1); i += 2) {
                final OtpErlangObject[] data = new OtpErlangObject[] {
                        new OtpErlangString(testData[i]),
                        new OtpErlangString(testData[i + 1]) };
                t.write(new OtpErlangString(testTime + key + i),
                        new OtpErlangList(data));
            }

            // now try to read the data:
            for (int i = 0; i < (testData.length - 1); i += 2) {
                final OtpErlangObject[] data = new OtpErlangObject[] {
                        new OtpErlangString(testData[i]),
                        new OtpErlangString(testData[i + 1]) };
                final OtpErlangObject actual = t.read(
                        new OtpErlangString(testTime + key + i)).value();
                final OtpErlangList expected = new OtpErlangList(data);

                assertEquals(expected, actual);
            }

            // commit the transaction and try to read the data with a new one:
            t.commit();
            t = new Transaction();
            for (int i = 0; i < (testData.length - 1); i += 2) {
                final OtpErlangObject[] data = new OtpErlangObject[] {
                        new OtpErlangString(testData[i]),
                        new OtpErlangString(testData[i + 1]) };
                final OtpErlangObject actual = t.read(
                        new OtpErlangString(testTime + key + i)).value();
                final OtpErlangList expected = new OtpErlangList(data);

                assertEquals(expected, actual);
            }
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
     * @throws TimeoutException
     */
    @Test
    public void testReqList_Empty() throws ConnectionException, UnknownException, TimeoutException, AbortException {
        final Transaction t = new Transaction();
        try {
            t.req_list(new RequestList());
        } finally {
            t.closeConnection();
        }
    }
}
