/**
 *  Copyright 2007-2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
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

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.ericsson.otp.erlang.OtpErlangObject;
import com.ericsson.otp.erlang.OtpErlangString;
import com.ericsson.otp.erlang.OtpErlangTuple;

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
//		"xie7iSie", "ail8yeeP", "ooZ4eesi", "Ahn7ohph", "Ohy5moo6", "xooSh9Oo", "ieb6eeS7", "Thooqu9h", 
//		"eideeC9u", "phois3Ie", "EimaiJ2p", "sha6ahR1", "Pheih3za", "bai4eeXe", "rai0aB7j", "xahXoox6", 
//		"Xah4Okeg", "cieG8Yae", "Pe9Ohwoo", "Eehig6ph", "Xe7rooy6", "waY2iifu", "kemi8AhY", "Che7ain8", 
//		"ohw6seiY", "aegh1oBa", "thoh9IeG", "Kee0xuwu", "Gohng8ee", "thoh9Chi", "aa4ahQuu", "Iesh5uge", 
//		"Ahzeil8n", "ieyep5Oh", "xah3IXee", "Eefa5qui", "kai8Muuf", "seeCe0mu", "cooqua5Y", "Ci3ahF6z", 
//		"ot0xaiNu", "aewael8K", "aev3feeM", "Fei7ua5t", "aeCa6oph", "ag2Aelei", "Shah1Pho", "ePhieb0N", 
//		"Uqu7Phup", "ahBi8voh", "oon3aeQu", "Koopa0nu", "xi0quohT", "Oog4aiph", "Aip2ag5D", "tirai7Ae", 
//		"gi0yoePh", "uay7yeeX", "aeb6ahC1", "OoJeic2a", "ieViom1y", "di0eeLai", "Taec2phe", "ID2cheiD", 
//		"oi6ahR5M", "quaiGi8W", "ne1ohLuJ", "DeD0eeng", "yah8Ahng", "ohCee2ie", "ecu1aDai", "oJeijah4", 
//		"Goo9Una1", "Aiph3Phi", "Ieph0ce5", "ooL6cae7", "nai0io1H", "Oop2ahn8", "ifaxae7O", "NeHai1ae", 
//		"Ao8ooj6a", "hi9EiPhi", "aeTh9eiP", "ao8cheiH", "Yieg3sha", "mah7cu2D", "Uo5wiegi", "Oowei0ya", 
//		"efeiDee7", "Oliese6y", "eiSh1hoh", "Joh6hoh9", "zib6Ooqu", "eejiJie4", "lahZ3aeg", "keiRai1d", 
//		"Fei0aewe", "aeS8aboh", "hae3ohKe", "Een9ohQu", "AiYeeh7o", "Yaihah4s", "ood4Giez", "Oumai7te", 
//		"hae2kahY", "afieGh4v", "Ush0boo0", "Ekootee5", "Ya8iz6Ie", "Poh6dich", "Eirae4Ah", "pai8Eeme", 
//		"uNah7dae", "yo3hahCh", "teiTh7yo", "zoMa5Cuv", "ThiQu5ax", "eChi5caa", "ii9ujoiV", "ge7Iekui", 
		"sai2aiTa", "ohKi9rie", "ei2ioChu", "aaNgah9y", "ooJai1Ie", "shoh0oH9", "Ool4Ahya", "poh0IeYa", 
		"Uquoo0Il", "eiGh4Oop", "ooMa0ufe", "zee6Zooc", "ohhao4Ah", "Uweekek5", "aePoos9I", "eiJ9noor", 
		"phoong1E", "ianieL2h", "An7ohs4T", "Eiwoeku3", "sheiS3ao", "nei5Thiw", "uL5iewai", "ohFoh9Ae"};
	
	/**
	 * @throws Exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	/**
	 * @throws Exception
	 */
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}
	
	/**
	 * @throws Exception
	 */
	@Before
	public void setUp() throws Exception {
	}

	/**
	 * @throws Exception
	 */
	@After
	public void tearDown() throws Exception {
	}

	/**
	 * Test method for {@link Transaction#Transaction()}.
	 * @throws ConnectionException 
	 */
	@Test
	public void testTransaction1() throws ConnectionException {
		Transaction t = new Transaction();
		t.closeConnection();
	}
	
	/**
	 * Test method for {@link Transaction#Transaction(com.ericsson.otp.erlang.OtpConnection)}.
	 * @throws ConnectionException 
	 */
	@Test
	public void testTransaction2() throws ConnectionException {
		Transaction t = new Transaction(ConnectionFactory.getInstance().createConnection("test"));
		t.closeConnection();
	}

	/**
	 * Test method for {@link Transaction#start()} with a closed connection.
	 * 
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 * @throws NotFoundException
	 * @throws TransactionNotFinishedException
	 */
	@Test(expected=ConnectionException.class)
	public void testStart_NotConnected() throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException,
			TransactionNotFinishedException {
		Transaction t = new Transaction();
		t.closeConnection();
		t.start();
	}
	
	/**
	 * Test method for {@link Transaction#start()} which starts a transaction once.
	 * 
	 * @throws TransactionNotFinishedException 
	 * @throws UnknownException 
	 * @throws ConnectionException 
	 */
	@Test
	public void testStart1() throws ConnectionException, UnknownException, TransactionNotFinishedException {
		Transaction t = new Transaction();
		try {
			t.start();
		} finally {
			t.closeConnection();
		}
	}

	/**
	 * Test method for {@link Transaction#start()} which tries to start a second
	 * transaction on a given transaction object.
	 * 
	 * @throws TransactionNotFinishedException
	 * @throws UnknownException
	 * @throws ConnectionException
	 */
	@Test(expected=TransactionNotFinishedException.class)
	public void testStart2() throws ConnectionException, UnknownException, TransactionNotFinishedException {
		Transaction t = new Transaction();try {
			t.start();
			t.start();
		} finally {
			t.closeConnection();
		}
	}

	/**
	 * Test method for {@link Transaction#commit()} which evaluates the case
	 * where the transaction was not started.
	 * 
	 * @throws ConnectionException
	 * @throws UnknownException 
	 */
	@Test(expected=TransactionNotStartedException.class)
	public void testCommit_NotStarted() throws ConnectionException, UnknownException {
		Transaction t = new Transaction();
		try {
			t.commit();
		} finally {
			t.closeConnection();
		}
	}

	/**
	 * Test method for {@link Transaction#start()} with a closed connection.
	 * 
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 * @throws NotFoundException
	 * @throws TransactionNotFinishedException
	 */
	@Test(expected=ConnectionException.class)
	public void testCommit_NotConnected() throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException,
			TransactionNotFinishedException {
		Transaction t = new Transaction();
		try {
			t.start();
			t.closeConnection();
			t.commit();
		} finally {
			t.closeConnection();
		}
	}
	
	/**
	 * Test method for {@link Transaction#commit()} which commits an empty
	 * transaction and tries to start a new one afterwards.
	 * 
	 * @throws ConnectionException
	 * @throws TransactionNotFinishedException
	 * @throws UnknownException
	 */
	@Test
	public void testCommit_Empty() throws ConnectionException, UnknownException, TransactionNotFinishedException {
		Transaction t = new Transaction();
		try {
			t.start();
			t.commit();
			t.start();
		} finally {
			t.closeConnection();
		}
	}
	
	/**
	 * Test method for {@link Transaction#abort()} which evaluates the case
	 * where the transaction was not started.
	 * 
	 * @throws ConnectionException
	 */
	@Test
	public void testAbort_NotStarted() throws ConnectionException {
		Transaction t = new Transaction();
		try {
			t.abort();
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
	 * @throws TransactionNotFinishedException
	 */
	@Test
	public void testAbort_NotConnected() throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException,
			TransactionNotFinishedException {
		Transaction t = new Transaction();
		t.closeConnection();
		t.abort();
	}

	/**
	 * Test method for {@link Transaction#abort()} which aborts an empty
	 * transaction and tries to start a new one afterwards.
	 * 
	 * @throws TransactionNotFinishedException 
	 * @throws UnknownException 
	 * @throws ConnectionException 
	 */
	@Test
	public void testAbort_Empty() throws ConnectionException, UnknownException, TransactionNotFinishedException {
		Transaction t = new Transaction();
		try {
			t.start();
			t.abort();
			t.start();
		} finally {
			t.closeConnection();
		}
	}
	
	/**
	 * Test method for {@link Transaction#reset()} which evaluates the case
	 * where the transaction was not started.
	 * 
	 * @throws ConnectionException
	 */
	@Test
	public void testReset_NotStarted() throws ConnectionException {
		Transaction t = new Transaction();
		try {
			t.reset();
		} finally {
			t.closeConnection();
		}
	}

	/**
	 * Test method for {@link Transaction#reset()} with a closed connection.
	 * 
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 * @throws NotFoundException
	 * @throws TransactionNotFinishedException
	 */
	@Test
	public void testReset_NotConnected() throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException,
			TransactionNotFinishedException {
		Transaction t = new Transaction();
		t.closeConnection();
		t.reset();
	}

	/**
	 * Test method for {@link Transaction#reset()} which resets an empty
	 * transaction and tries to start a new one afterwards.
	 * 
	 * @throws TransactionNotFinishedException 
	 * @throws UnknownException 
	 * @throws ConnectionException 
	 */
	@Test
	public void testReset_Empty() throws ConnectionException, UnknownException, TransactionNotFinishedException {
		Transaction t = new Transaction();
		try {
			t.start();
			t.reset();
			t.start();
		} finally {
			t.closeConnection();
		}
	}

	/**
	 * Test method for {@link Transaction#read(String)}.
	 * 
	 * @throws TransactionNotFinishedException 
	 * @throws UnknownException 
	 * @throws ConnectionException 
	 * @throws NotFoundException 
	 * @throws TimeoutException 
	 */
	@Test(expected = NotFoundException.class)
	public void testRead_NotFound() throws ConnectionException,
			UnknownException, TransactionNotFinishedException,
			TimeoutException, NotFoundException {
		String key = "_Read_NotFound";
		Transaction t = new Transaction();
		try {
			t.start();
			t.read(testTime + key);
		} finally {
			t.closeConnection();
		}
	}
	
	/**
	 * Test method for {@link Transaction#read(String)} which evaluates the case
	 * where the transaction was not started.
	 * 
	 * @throws ConnectionException
	 * @throws NotFoundException 
	 * @throws UnknownException 
	 * @throws TimeoutException 
	 */
	@Test(expected = TransactionNotStartedException.class)
	public void testRead_NotStarted() throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException {
		String key = "_Read_NotStarted";
		Transaction t = new Transaction();
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
	 * @throws TransactionNotFinishedException 
	 */
	@Test(expected = ConnectionException.class)
	public void testRead_NotConnected() throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException,
			TransactionNotFinishedException {
		String key = "_Read_NotConnected";
		Transaction t = new Transaction();
		try {
			t.start();
			t.closeConnection();
			t.read(testTime + key);
		} finally {
			t.closeConnection();
		}
	}
	
	/**
	 * Test method for {@link Transaction#readObject(OtpErlangString)}.
	 * 
	 * @throws TransactionNotFinishedException 
	 * @throws UnknownException 
	 * @throws ConnectionException 
	 * @throws NotFoundException 
	 * @throws TimeoutException 
	 */
	@Test(expected = NotFoundException.class)
	public void testReadObject_NotFound() throws ConnectionException,
			UnknownException, TransactionNotFinishedException,
			TimeoutException, NotFoundException {
		String key = "_ReadObject_NotFound";
		Transaction t = new Transaction();
		try {
			t.start();
			t.readObject(new OtpErlangString(testTime + key));
		} finally {
			t.closeConnection();
		}
	}
	
	/**
	 * Test method for {@link Transaction#readObject(OtpErlangString)} which evaluates the case
	 * where the transaction was not started.
	 * 
	 * @throws UnknownException 
	 * @throws ConnectionException 
	 * @throws NotFoundException 
	 * @throws TimeoutException 
	 */
	@Test(expected = TransactionNotStartedException.class)
	public void testReadObject_NotStarted() throws ConnectionException,
			UnknownException, TimeoutException, NotFoundException {
		String key = "_ReadObject_NotStarted";
		Transaction t = new Transaction();
		try {
			t.readObject(new OtpErlangString(testTime + key));
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
	 * @throws TransactionNotFinishedException 
	 */
	@Test(expected = ConnectionException.class)
	public void testReadObject_NotConnected() throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException,
			TransactionNotFinishedException {
		String key = "_ReadObject_NotConnected";
		Transaction t = new Transaction();
		try {
			t.start();
			t.closeConnection();
			t.readObject(new OtpErlangString(testTime + key));
		} finally {
			t.closeConnection();
		}
	}

	/**
	 * Test method for {@link Transaction#write(String, String)} which evaluates
	 * the case where the transaction was not started.
	 * 
	 * @throws UnknownException
	 * @throws ConnectionException
	 * @throws NotFoundException
	 * @throws TimeoutException
	 */
	@Test(expected = TransactionNotStartedException.class)
	public void testWrite_NotStarted() throws ConnectionException,
			UnknownException, TimeoutException, NotFoundException {
		String key = "_Write_notStarted";
		Transaction t = new Transaction();
		try {
			t.write(testTime + key, testData[0]);
		} finally {
			t.closeConnection();
		}
	}

	/**
	 * Test method for {@link Transaction#write(String, String)} with a closed
	 * connection.
	 * 
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 * @throws NotFoundException
	 * @throws TransactionNotFinishedException 
	 */
	@Test(expected = ConnectionException.class)
	public void testWrite_NotConnected() throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException,
			TransactionNotFinishedException {
		String key = "_Write_NotConnected";
		Transaction t = new Transaction();
		try {
			t.start();
			t.closeConnection();
			t.write(testTime + key, testData[0]);
		} finally {
			t.closeConnection();
		}
	}

	/**
	 * Test method for {@link Transaction#write(String, String)} and
	 * {@link Transaction#read(String)}.
	 * 
	 * @throws ConnectionException
	 * @throws NotFoundException
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws TransactionNotFinishedException
	 */
	@Test
	public void testWrite() throws ConnectionException, TimeoutException, UnknownException, NotFoundException, TransactionNotFinishedException {
		Transaction t = new Transaction();
		try {
			t.start();
			for (int i = 0; i < testData.length; ++i) {
				t.write(testTime + "_testWriteString1_" + i, testData[i]);
			}
			
			// now try to read the data:
			
			for (int i = 0; i < testData.length; ++i) {
				String actual = t.read(testTime + "_testWriteString1_" + i);
				assertEquals(new OtpErlangString(testData[i]), actual);
			}
			
			// commit the transaction and try to read the data with a new one:
			
			t.commit();
			t = new Transaction();
			t.start();
			for (int i = 0; i < testData.length; ++i) {
				String actual = t.read(testTime + "_testWriteString1_" + i);
				assertEquals(new OtpErlangString(testData[i]), actual);
			}
		} finally {
			t.closeConnection();
		}
	}
	
	/**
	 * Test method for
	 * {@link Transaction#writeObject(OtpErlangString, OtpErlangObject)} which
	 * evaluates the case where the transaction was not started.
	 * 
	 * @throws UnknownException
	 * @throws ConnectionException
	 * @throws NotFoundException
	 * @throws TimeoutException
	 */
	@Test(expected = TransactionNotStartedException.class)
	public void testWriteObject_NotStarted() throws ConnectionException,
			UnknownException, TimeoutException, NotFoundException {
		String key = "_WriteObject_notStarted";
		Transaction t = new Transaction();
		try {
			t.writeObject(
					new OtpErlangString(testTime + key),
					new OtpErlangString(testData[0]));
		} finally {
			t.closeConnection();
		}
	}

	/**
	 * Test method for
	 * {@link Transaction#writeObject(OtpErlangString, OtpErlangObject)} with a
	 * closed connection.
	 * 
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 * @throws NotFoundException
	 * @throws TransactionNotFinishedException 
	 */
	@Test(expected = ConnectionException.class)
	public void testWriteObject_NotConnected() throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException,
			TransactionNotFinishedException {
		String key = "_WriteObject_NotConnected";
		Transaction t = new Transaction();
		try {
			t.start();
			t.closeConnection();
			t.writeObject(
					new OtpErlangString(testTime + key),
					new OtpErlangString(testData[0]));
		} finally {
			t.closeConnection();
		}
	}

	/**
	 * Test method for
	 * {@link Transaction#writeObject(OtpErlangString, OtpErlangObject)} and
	 * {@link Transaction#readObject(OtpErlangString)}.
	 * 
	 * @throws NotFoundException
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 * @throws TransactionNotFinishedException 
	 * 
	 * TODO: fix test for the original data set of 160 items (is way too slow or not working at all)
	 */
	@Test
	public void testWriteObject() throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException, TransactionNotFinishedException {
		Transaction t = new Transaction();
		try {
			t.start();
			for (int i = 0; i < testData.length - 1; ++i) {
				OtpErlangObject[] data = new OtpErlangObject[] {
						new OtpErlangString(testData[i]),
						new OtpErlangString(testData[i + 1]) };
				t.writeObject(new OtpErlangString(testTime + "_testWriteObject1_" + i),
						new OtpErlangTuple(data));
			}

			// now try to read the data:

			for (int i = 0; i < testData.length - 1; i += 2) {
				OtpErlangObject[] data = new OtpErlangObject[] {
						new OtpErlangString(testData[i]),
						new OtpErlangString(testData[i + 1]) };
				OtpErlangObject actual = t.readObject(
						new OtpErlangString(testTime + "_testWriteObject1_" + i));
				OtpErlangTuple expected = new OtpErlangTuple(data);

				assertEquals(expected, actual);
			}
			
			// commit the transaction and try to read the data with a new one:
			
			t.commit();
			t = new Transaction();
			t.start();
			for (int i = 0; i < testData.length - 1; i += 2) {
				OtpErlangObject[] data = new OtpErlangObject[] {
						new OtpErlangString(testData[i]),
						new OtpErlangString(testData[i + 1]) };
				OtpErlangObject actual = t.readObject(
						new OtpErlangString(testTime + "_testWriteObject1_" + i));
				OtpErlangTuple expected = new OtpErlangTuple(data);

				assertEquals(expected, actual);
			}
		} finally {
			t.closeConnection();
		}
	}

	/**
	 * Test method for {@link Transaction#revertLastOp()} which tries to revert
	 * an operation without starting a transaction.
	 * 
	 * @throws TransactionNotFinishedException
	 * @throws UnknownException
	 * @throws ConnectionException
	 * @throws TimeoutException
	 * @throws NotFoundException
	 */
	@Test
	public void testRevertLastOp_NotStarted() throws ConnectionException,
			UnknownException, TransactionNotFinishedException,
			TimeoutException, NotFoundException {
		Transaction t = new Transaction();
		try {
			t.revertLastOp();
		} finally {
			t.closeConnection();
		}
	}

	/**
	 * Test method for {@link Transaction#revertLastOp()} with a closed
	 * connection.
	 * 
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 * @throws NotFoundException
	 * @throws TransactionNotFinishedException 
	 */
	@Test
	public void testRevertLastOp_NotConnected() throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException,
			TransactionNotFinishedException {
		Transaction t = new Transaction();
		try {
			t.start();
			t.closeConnection();
			t.revertLastOp();
		} finally {
			t.closeConnection();
		}
	}
	
	/**
	 * Test method for {@link Transaction#revertLastOp()},
	 * {@link Transaction#write(String, String)} and
	 * {@link Transaction#read(String)} which overwrites to a single key
	 * twice after an initial write and tries to revert the last write. A
	 * final read verifies that the old value was restored.
	 * 
	 * @throws TransactionNotFinishedException
	 * @throws UnknownException
	 * @throws ConnectionException
	 * @throws TimeoutException
	 * @throws NotFoundException
	 */
	@Test
	public void testRevertLastOp1() throws ConnectionException, UnknownException, TransactionNotFinishedException, TimeoutException, NotFoundException {
		Transaction t = new Transaction();
		try {
			t.start();
			t.write(testTime + "_RevertLastOp1", testData[0]);
			t.write(testTime + "_RevertLastOp1", testData[1]);
			t.write(testTime + "_RevertLastOp1", testData[2]);
			t.revertLastOp();
			
			assertEquals(testData[1], t.read(testTime + "_RevertLastOp1"));
		} finally {
			t.closeConnection();
		}
	}

	/**
	 * Test method for {@link Transaction#revertLastOp()},
	 * {@link Transaction#write(String, String)} and
	 * {@link Transaction#read(String)} which overwrites to a single key
	 * twice after an initial write and tries to revert the last two write
	 * operations. A final read verifies that only the last write was reverted.
	 * 
	 * @throws TransactionNotFinishedException
	 * @throws UnknownException
	 * @throws ConnectionException
	 * @throws TimeoutException
	 * @throws NotFoundException
	 */
	@Test
	public void testRevertLastOp2() throws ConnectionException, UnknownException, TransactionNotFinishedException, TimeoutException, NotFoundException {
		Transaction t = new Transaction();
		try {
			t.start();
			t.write(testTime + "_RevertLastOp2", testData[0]);
			t.write(testTime + "_RevertLastOp2", testData[1]);
			t.write(testTime + "_RevertLastOp2", testData[2]);
			t.revertLastOp();
			t.revertLastOp();
			
			assertEquals(testData[1], t.read(testTime + "_RevertLastOp2"));
		} finally {
			t.closeConnection();
		}
	}

	/**
	 * Test method for {@link Transaction#revertLastOp()},
	 * {@link Transaction#write(String, String)} and
	 * {@link Transaction#read(String)} which writes a value and tries to
	 * revert this write operations. A final read verifies that the write
	 * operation was reverted.
	 * 
	 * @throws TransactionNotFinishedException
	 * @throws UnknownException
	 * @throws ConnectionException
	 * @throws TimeoutException
	 * @throws NotFoundException
	 */
	@Test(expected=NotFoundException.class)
	public void testRevertLastOp3() throws ConnectionException, UnknownException, TransactionNotFinishedException, TimeoutException, NotFoundException {
		Transaction t = new Transaction();
		try {
			t.start();
			t.write(testTime + "_RevertLastOp3", testData[0]);
			t.revertLastOp();
			
			assertEquals(testData[1], t.read(testTime + "_RevertLastOp3"));
		} finally {
			t.closeConnection();
		}
	}

	/**
	 * Test method for {@link Transaction#revertLastOp()},
	 * {@link Transaction#write(String, String)} and
	 * {@link Transaction#read(String)} which should show that
	 * writing a value for a key for which a previous read returned a
	 * NotFoundException is not possible without reverting the last operation.
	 * 
	 * @throws TransactionNotFinishedException
	 * @throws UnknownException
	 * @throws ConnectionException
	 * @throws TimeoutException
	 * @throws NotFoundException
	 */
	@Test(expected=UnknownException.class)
	public void testRevertLastOp4() throws ConnectionException, UnknownException, TransactionNotFinishedException, TimeoutException, NotFoundException {
		Transaction t = new Transaction();
		try {
			t.start();
			try {
				t.read(testTime + "_RevertLastOp4_notFound");
			} catch (NotFoundException e) {
			}
			t.write(testTime + "_RevertLastOp4_notFound", testData[0]);
			
			assertEquals(testData[0], t.read(testTime + "_RevertLastOp4_notFound"));
		} finally {
			t.closeConnection();
		}
	}
	
	/**
	 * Test method for {@link Transaction#revertLastOp()},
	 * {@link Transaction#write(String, String)} and
	 * {@link Transaction#read(String)}.
	 * 
	 * @throws TransactionNotFinishedException 
	 * @throws UnknownException 
	 * @throws ConnectionException 
	 * @throws TimeoutException 
	 * @throws NotFoundException 
	 */
	@Test
	public void testRevertLastOp5() throws ConnectionException, UnknownException, TransactionNotFinishedException, TimeoutException, NotFoundException {
		Transaction t = new Transaction();
		try {
			t.start();
			try {
				t.read(testTime + "_RevertLastOp5_notFound");
			} catch (NotFoundException e) {
			}
			t.revertLastOp();
			t.write(testTime + "_RevertLastOp5_notFound", testData[0]);
			
			assertEquals(testData[0], t.read(testTime + "_RevertLastOp5_notFound"));
		} finally {
			t.closeConnection();
		}
	}
}
