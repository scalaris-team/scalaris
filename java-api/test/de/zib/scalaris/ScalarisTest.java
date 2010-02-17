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

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Reader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.TimeUnit;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mortbay.jetty.Request;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.AbstractHandler;
import org.mortbay.util.ajax.JSON;

import com.ericsson.otp.erlang.OtpErlangList;
import com.ericsson.otp.erlang.OtpErlangObject;
import com.ericsson.otp.erlang.OtpErlangString;
import com.ericsson.otp.erlang.OtpErlangTuple;

/**
 * Unit test for the {@link Scalaris} class.
 * 
 * @author Nico Kruber, kruber@zib.de
 * @version 2.0
 * @since 2.0
 */
public class ScalarisTest {
	/**
	 * First port to use for jetty servers (subsequent servers will use
	 * subsequent port numbers).
	 */
	private final static int startPort = 8081;
	private final static long testTime = System.currentTimeMillis();
	
	private final static String[] testData = {
		"ahz2ieSh", "wooPhu8u", "quai9ooK", "Oquae4ee", "Airier1a", "Boh3ohv5", "ahD3Saog", "EM5ooc4i", 
		"Epahrai8", "laVahta7", "phoo6Ahj", "Igh9eepa", "aCh4Lah6", "ooT0ath5", "uuzau4Ie", "Iup6mae6", 
		"xie7iSie", "ail8yeeP", "ooZ4eesi", "Ahn7ohph", "Ohy5moo6", "xooSh9Oo", "ieb6eeS7", "Thooqu9h", 
		"eideeC9u", "phois3Ie", "EimaiJ2p", "sha6ahR1", "Pheih3za", "bai4eeXe", "rai0aB7j", "xahXoox6", 
		"Xah4Okeg", "cieG8Yae", "Pe9Ohwoo", "Eehig6ph", "Xe7rooy6", "waY2iifu", "kemi8AhY", "Che7ain8", 
		"ohw6seiY", "aegh1oBa", "thoh9IeG", "Kee0xuwu", "Gohng8ee", "thoh9Chi", "aa4ahQuu", "Iesh5uge", 
		"Ahzeil8n", "ieyep5Oh", "xah3IXee", "Eefa5qui", "kai8Muuf", "seeCe0mu", "cooqua5Y", "Ci3ahF6z", 
		"ot0xaiNu", "aewael8K", "aev3feeM", "Fei7ua5t", "aeCa6oph", "ag2Aelei", "Shah1Pho", "ePhieb0N", 
		"Uqu7Phup", "ahBi8voh", "oon3aeQu", "Koopa0nu", "xi0quohT", "Oog4aiph", "Aip2ag5D", "tirai7Ae", 
		"gi0yoePh", "uay7yeeX", "aeb6ahC1", "OoJeic2a", "ieViom1y", "di0eeLai", "Taec2phe", "ID2cheiD", 
		"oi6ahR5M", "quaiGi8W", "ne1ohLuJ", "DeD0eeng", "yah8Ahng", "ohCee2ie", "ecu1aDai", "oJeijah4", 
		"Goo9Una1", "Aiph3Phi", "Ieph0ce5", "ooL6cae7", "nai0io1H", "Oop2ahn8", "ifaxae7O", "NeHai1ae", 
		"Ao8ooj6a", "hi9EiPhi", "aeTh9eiP", "ao8cheiH", "Yieg3sha", "mah7cu2D", "Uo5wiegi", "Oowei0ya", 
		"efeiDee7", "Oliese6y", "eiSh1hoh", "Joh6hoh9", "zib6Ooqu", "eejiJie4", "lahZ3aeg", "keiRai1d", 
		"Fei0aewe", "aeS8aboh", "hae3ohKe", "Een9ohQu", "AiYeeh7o", "Yaihah4s", "ood4Giez", "Oumai7te", 
		"hae2kahY", "afieGh4v", "Ush0boo0", "Ekootee5", "Ya8iz6Ie", "Poh6dich", "Eirae4Ah", "pai8Eeme", 
		"uNah7dae", "yo3hahCh", "teiTh7yo", "zoMa5Cuv", "ThiQu5ax", "eChi5caa", "ii9ujoiV", "ge7Iekui", 
		"sai2aiTa", "ohKi9rie", "ei2ioChu", "aaNgah9y", "ooJai1Ie", "shoh0oH9", "Ool4Ahya", "poh0IeYa", 
		"Uquoo0Il", "eiGh4Oop", "ooMa0ufe", "zee6Zooc", "ohhao4Ah", "Uweekek5", "aePoos9I", "eiJ9noor", 
		"phoong1E", "ianieL2h", "An7ohs4T", "Eiwoeku3", "sheiS3ao", "nei5Thiw", "uL5iewai", "ohFoh9Ae"};

	private HashMap<String, Vector<String>> notifications_server1;
	private HashMap<String, Vector<String>> notifications_server2;
	private HashMap<String, Vector<String>> notifications_server3;
	
	/**
	 * wait that long for notifications to arrive
	 */
	private static final int notifications_timeout = 60;
	
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
	 * Test method for
	 * {@link Scalaris#Scalaris()}.
	 * @throws ConnectionException 
	 */
	@Test
	public void testScalaris1() throws ConnectionException {
		Scalaris conn = new Scalaris();
		conn.closeConnection();
	}
	
	/**
	 * Test method for
	 * {@link Scalaris#Scalaris(Connection)}.
	 * @throws ConnectionException 
	 */
	@Test
	public void testScalaris2() throws ConnectionException {
		Scalaris conn = new Scalaris(ConnectionFactory.getInstance().createConnection("test"));
		conn.closeConnection();
	}

	/**
	 * Test method for {@link Scalaris#read(String)}.
	 * 
	 * @throws NotFoundException
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 */
	@Test(expected=NotFoundException.class)
	public void testRead_NotFound() throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException {
		String key = "_Read_NotFound";
		Scalaris conn = new Scalaris();
		try {
			conn.read(testTime + key);
		} finally {
			conn.closeConnection();
		}
	}

	/**
	 * Test method for {@link Scalaris#readObject(OtpErlangString)}.
	 * 
	 * @throws NotFoundException
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 */
	@Test(expected=NotFoundException.class)
	public void testReadObject_NotFound() throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException {
		String key = "_ReadObject_NotFound";
		Scalaris conn = new Scalaris();
		try {
			conn.readObject(new OtpErlangString(testTime + key));
		} finally {
			conn.closeConnection();
		}
	}

	/**
	 * Test method for {@link Scalaris#read(String)} with a closed connection.
	 * 
	 * @throws NotFoundException
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 */
	@Test(expected=ConnectionException.class)
	public void testRead_NotConnected() throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException {
		String key = "_Read_NotConnected";
		Scalaris conn = new Scalaris();
		conn.closeConnection();
		conn.readObject(new OtpErlangString(testTime + key));
	}

	/**
	 * Test method for {@link Scalaris#readObject(OtpErlangString)} with a
	 * closed connection.
	 * 
	 * @throws NotFoundException
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 */
	@Test(expected=ConnectionException.class)
	public void testReadObject_NotConnected() throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException {
		String key = "_ReadObject_NotConnected";
		Scalaris conn = new Scalaris();
		conn.closeConnection();
		conn.readObject(new OtpErlangString(testTime + key));
	}

	/**
	 * Test method for
	 * {@link Scalaris#writeObject(OtpErlangString, OtpErlangObject)} with a
	 * closed connection.
	 * 
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 * @throws NotFoundException
	 */
	@Test(expected=ConnectionException.class)
	public void testWriteObject_NotConnected() throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException {
		String key = "_WriteObject_NotConnected";
		Scalaris conn = new Scalaris();
		conn.closeConnection();
		OtpErlangObject[] data = new OtpErlangObject[] {
				new OtpErlangString(testData[0]),
				new OtpErlangString(testData[1]) };
		conn.writeObject(
				new OtpErlangString(testTime + key),
				new OtpErlangTuple(data) );
	}
	
	/**
	 * Test method for
	 * {@link Scalaris#writeObject(OtpErlangString, OtpErlangObject)}
	 * and {@link Scalaris#readObject(OtpErlangString)}.
	 * Writes erlang tuples and uses a distinct key for each value. Tries to read the data afterwards.
	 * 
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 * @throws NotFoundException 
	 */
	@Test
	public void testWriteObject1() throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException {
		String key = "_WriteObject1_";
		Scalaris conn = new Scalaris();

		try {
			for (int i = 0; i < testData.length - 1; i += 2) {
				OtpErlangObject[] data = new OtpErlangObject[] {
						new OtpErlangString(testData[i]),
						new OtpErlangString(testData[i + 1]) };
				conn.writeObject(
						new OtpErlangString(testTime + key + i),
						new OtpErlangTuple(data) );
			}
			
			// now try to read the data:
			
			for (int i = 0; i < testData.length - 1; i += 2) {
				OtpErlangObject[] data = new OtpErlangObject[] {
						new OtpErlangString(testData[i]),
						new OtpErlangString(testData[i + 1]) };
				OtpErlangObject actual = conn.readObject(
						new OtpErlangString(testTime + key + i));
				OtpErlangTuple expected = new OtpErlangTuple(data);
				
				assertEquals(expected, actual);
			}
		} finally {
			conn.closeConnection();
		}
	}
	
	/**
	 * Test method for
	 * {@link Scalaris#writeObject(OtpErlangString, OtpErlangObject)}
	 * and {@link Scalaris#readObject(OtpErlangString)}.
	 * Writes erlang tuples and uses a single key for all the values. Tries to read the data afterwards.
	 * 
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 * @throws NotFoundException 
	 */
	@Test
	public void testWriteObject2() throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException {
		String key = "_WriteObject2";
		Scalaris conn = new Scalaris();

		try {
			OtpErlangObject[] data = new OtpErlangObject[0];
			for (int i = 0; i < testData.length - 1; i += 2) {
				data = new OtpErlangObject[] {
						new OtpErlangString(testData[i]),
						new OtpErlangString(testData[i + 1]) };
				conn.writeObject(
						new OtpErlangString(testTime + key),
						new OtpErlangTuple(data));
			}
			
			// now try to read the data:
			
			OtpErlangObject actual = conn.readObject(
					new OtpErlangString(testTime + key));
			OtpErlangTuple expected = new OtpErlangTuple(data);
			
			assertEquals(expected, actual);
		} finally {
			conn.closeConnection();
		}
	}

	/**
	 * Test method for {@link Scalaris#write(String, String)} with a
	 * closed connection.
	 * 
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 * @throws NotFoundException
	 */
	@Test(expected=ConnectionException.class)
	public void testWrite_NotConnected() throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException {
		String key = "_Write_NotConnected";
		Scalaris conn = new Scalaris();
		conn.closeConnection();
		conn.write(testTime + key, testData[0]);
	}
	
	/**
	 * Test method for
	 * {@link Scalaris#write(String, String)}
	 * and {@link Scalaris#read(String)}.
	 * Writes strings and uses a distinct key for each value. Tries to read the data afterwards.
	 * 
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 * @throws NotFoundException 
	 */
	@Test
	public void testWrite1() throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException {
		String key = "_Write1_";
		Scalaris conn = new Scalaris();
		
		try {
			for (int i = 0; i < testData.length; ++i) {
				conn.write(testTime + key + i, testData[i]);
			}
			
			// now try to read the data:
			for (int i = 0; i < testData.length; ++i) {
				String actual = conn.read(testTime + key + i);
				assertEquals(testData[i], actual);
			}
		} finally {
			conn.closeConnection();
		}
	}
	
	/**
	 * Test method for
	 * {@link Scalaris#write(String, String)}
	 * and {@link Scalaris#read(String)}.
	 * Writes strings and uses a single key for all the values. Tries to read the data afterwards.
	 * 
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 * @throws NotFoundException 
	 */
	@Test
	public void testWrite2() throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException {
		String key = "_Write2";
		Scalaris conn = new Scalaris();

		try {
			for (int i = 0; i < testData.length; ++i) {
				conn.write(testTime + key, testData[i]);
			}
			
			// now try to read the data:
			String actual = conn.read(testTime + key);
			assertEquals(testData[testData.length - 1], actual);
		} finally {
			conn.closeConnection();
		}
	}
	
	/**
	 * Test method for {@link Scalaris#delete(String)} and
	 * {@link Scalaris#write(String, String)}.
	 * Tries to delete some not existing keys.
	 * 
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 * @throws NodeNotFoundException
	 */
	@Test
	public void testDelete_notExistingKey() throws ConnectionException,
			TimeoutException, UnknownException, NodeNotFoundException {
		String key = "_Delete_NotExistingKey";
		Scalaris conn = new Scalaris();

		try {
			for (int i = 0; i < testData.length; ++i) {
				long deleted = conn.delete(testTime + key + i);
				DeleteResult result = conn.getLastDeleteResult();
				assertEquals(0, deleted);
				assertEquals(0, result.ok);
				assertEquals(0, result.locks_set);
				assertEquals(4, result.undef);
			}
		} finally {
			conn.closeConnection();
		}
	}
	
	/**
	 * Test method for {@link Scalaris#delete(String)} and
	 * {@link Scalaris#write(String, String)}.
	 * Inserts some values, tries to delete them afterwards and tries the
	 * delete again.
	 * 
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 * @throws NodeNotFoundException
	 */
	@Test
	public void testDelete1() throws ConnectionException,
			TimeoutException, UnknownException, NodeNotFoundException {
		String key = "_Delete1";
		Scalaris conn = new Scalaris();

		try {
			for (int i = 0; i < testData.length; ++i) {
				conn.write(testTime + key + i, testData[i]);
			}
			
			// now try to delete the data:
			for (int i = 0; i < testData.length; ++i) {
				long deleted = conn.delete(testTime + key + i);
				DeleteResult result = conn.getLastDeleteResult();
				assertEquals(4, deleted);
				assertEquals(4, result.ok);
				assertEquals(0, result.locks_set);
				assertEquals(0, result.undef);
				
				// try again (should be successful with 0 deletes)
				deleted = conn.delete(testTime + key + i);
				result = conn.getLastDeleteResult();
				assertEquals(0, deleted);
				assertEquals(0, result.ok);
				assertEquals(0, result.locks_set);
				assertEquals(4, result.undef);
			}
		} finally {
			conn.closeConnection();
		}
	}
	
	/**
	 * Test method for {@link Scalaris#delete(String)} and
	 * {@link Scalaris#write(String, String)}.
	 * Inserts some values, tries to delete them afterwards, inserts them again
	 * and tries to delete them again (twice).
	 * 
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 * @throws NodeNotFoundException
	 */
	@Test
	public void testDelete2() throws ConnectionException,
			TimeoutException, UnknownException, NodeNotFoundException {
		String key = "_Delete2";
		Scalaris conn = new Scalaris();

		try {
			for (int i = 0; i < testData.length; ++i) {
				conn.write(testTime + key + i, testData[i]);
			}
			
			// now try to delete the data:
			for (int i = 0; i < testData.length; ++i) {
				long deleted = conn.delete(testTime + key + i);
				DeleteResult result = conn.getLastDeleteResult();
				assertEquals(4, deleted);
				assertEquals(4, result.ok);
				assertEquals(0, result.locks_set);
				assertEquals(0, result.undef);
			}
			
			for (int i = 0; i < testData.length; ++i) {
				conn.write(testTime + key + i, testData[i]);
			}
			
			// now try to delete the data:
			for (int i = 0; i < testData.length; ++i) {
				long deleted = conn.delete(testTime + key + i);
				DeleteResult result = conn.getLastDeleteResult();
				assertEquals(4, deleted);
				assertEquals(4, result.ok);
				assertEquals(0, result.locks_set);
				assertEquals(0, result.undef);
				
				// try again (should be successful with 0 deletes)
				deleted = conn.delete(testTime + key + i);
				result = conn.getLastDeleteResult();
				assertEquals(0, deleted);
				assertEquals(0, result.ok);
				assertEquals(0, result.locks_set);
				assertEquals(4, result.undef);
			}
		} finally {
			conn.closeConnection();
		}
	}

	/**
	 * Test method for
	 * {@link Scalaris#publish(OtpErlangString, OtpErlangString)} with a closed
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
		Scalaris conn = new Scalaris();
		conn.closeConnection();
		conn.publish(
				new OtpErlangString(testTime + topic),
				new OtpErlangString(testData[0]));
	}
	
	/**
	 * Test method for
	 * {@link Scalaris#publish(OtpErlangString, OtpErlangString)}.
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
		Scalaris conn = new Scalaris();

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
	 * {@link Scalaris#publish(OtpErlangString, OtpErlangString)}.
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
		Scalaris conn = new Scalaris();

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
	 * Test method for {@link Scalaris#publish(String, String)} with a closed
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
		Scalaris conn = new Scalaris();
		conn.closeConnection();
		conn.publish(testTime + topic, testData[0]);
	}
	
	/**
	 * Test method for
	 * {@link Scalaris#publish(String, String)}.
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
		Scalaris conn = new Scalaris();

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
	 * {@link Scalaris#publish(String, String)}.
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
		Scalaris conn = new Scalaris();

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
	
	// getSubscribers() test methods for not existing topics begin
	
	/**
	 * Test method for
	 * {@link Scalaris#getSubscribers(OtpErlangString)} with a closed
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
		Scalaris conn = new Scalaris();
		conn.closeConnection();
		conn.getSubscribers(new OtpErlangString(testTime + topic));
	}
	
	/**
	 * Test method for
	 * {@link Scalaris#getSubscribers(OtpErlangString)}.
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
		Scalaris conn = new Scalaris();

		try {
			conn.getSubscribers(new OtpErlangString(testTime + topic));
		} finally {
			conn.closeConnection();
		}
	}
	
	/**
	 * Test method for
	 * {@link Scalaris#getSubscribers(String)} with a closed
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
		Scalaris conn = new Scalaris();
		conn.closeConnection();
		conn.getSubscribers(testTime + topic);
	}
	
	/**
	 * Test method for
	 * {@link Scalaris#getSubscribers(String)}.
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
		Scalaris conn = new Scalaris();
		
		try {
			conn.getSubscribers(testTime + topic);
		} finally {
			conn.closeConnection();
		}
	}
	
	// getSubscribers() test methods for not existing topics end
	// subscribe() test methods begin

	/**
	 * Test method for
	 * {@link Scalaris#subscribe(OtpErlangString, OtpErlangString)} with a
	 * closed connection.
	 * 
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 * @throws NotFoundException
	 */
	@Test(expected=ConnectionException.class)
	public void testSubscribeOtp_NotConnected() throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException {
		String topic = "_SubscribeOtp_NotConnected";
		Scalaris conn = new Scalaris();
		conn.closeConnection();
		conn.subscribe(
				new OtpErlangString(testTime + topic),
				new OtpErlangString(testData[0]) );
	}
	
	/**
	 * Test method for
	 * {@link Scalaris#subscribe(OtpErlangString, OtpErlangString)} and
	 * {@link Scalaris#getSubscribers(OtpErlangString)}.
	 * Subscribes some "random" URLs to "random" topics and uses a distinct topic for each URL.
	 * 
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 * @throws NotFoundException 
	 */
	@Test
	public void testSubscribeOtp1() throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException {
		String topic = "_SubscribeOtp1_";
		Scalaris conn = new Scalaris();

		try {
			for (int i = 0; i < testData.length; ++i) {
				conn.subscribe(
						new OtpErlangString(testTime + topic + i),
						new OtpErlangString(testData[i]) );
			}
			
			// check if the subscribers were successfully saved:
			
			for (int i = 0; i < testData.length; ++i) {
				String topic1 = topic + i;
				OtpErlangList subscribers = conn
						.getSubscribers(new OtpErlangString(testTime
								+ topic1));
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
	 * {@link Scalaris#subscribe(OtpErlangString, OtpErlangString)} and
	 * {@link Scalaris#getSubscribers(OtpErlangString)}.
	 * Subscribes some "random" URLs to "random" topics and uses a single topic for all URLs.
	 * 
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 * @throws NotFoundException 
	 */
	@Test
	public void testSubscribeOtp2() throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException {
		String topic = "_SubscribeOtp2";
		Scalaris conn = new Scalaris();

		try {
			for (int i = 0; i < testData.length; ++i) {
				conn.subscribe(
						new OtpErlangString(testTime + topic),
						new OtpErlangString(testData[i]) );
			}
			
			// check if the subscribers were successfully saved:
			
			OtpErlangList subscribers = conn
					.getSubscribers(new OtpErlangString(testTime + topic));
			for (int i = 0; i < testData.length; ++i) {
				assertTrue("Subscriber \"" + testData[i]
						+ "\" does not exist for topic \"" + topic + "\"", checkSubscribers(
						subscribers, testData[i]));
			}
			
			if (subscribers.arity() > testData.length) {
				fail("\"" + getDiffElement(subscribers, testData) + " should not be a subscriber of " + topic);
			}
		} finally {
			conn.closeConnection();
		}
	}

	/**
	 * Test method for {@link Scalaris#subscribe(String, String)} with a closed
	 * connection.
	 * 
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 * @throws NotFoundException
	 */
	@Test(expected=ConnectionException.class)
	public void testSubscribe_NotConnected() throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException {
		String topic = "_Subscribe_NotConnected";
		Scalaris conn = new Scalaris();
		conn.closeConnection();
		conn.subscribe(testTime + topic, testData[0]);
	}
	
	/**
	 * Test method for
	 * {@link Scalaris#subscribe(String, String)} and
	 * {@link Scalaris#getSubscribers(String)}.
	 * Subscribes some "random" URLs to "random" topics and uses a distinct topic for each URL.
	 * 
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 * @throws NotFoundException 
	 */
	@Test
	public void testSubscribe1() throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException {
		String topic = "_Subscribe1_";
		Scalaris conn = new Scalaris();

		try {
			for (int i = 0; i < testData.length; ++i) {
				conn.subscribe(testTime + topic + i, testData[i]);
			}
			
			// check if the subscribers were successfully saved:
			
			for (int i = 0; i < testData.length; ++i) {
				String topic1 = topic + i;
				List<String> subscribers = conn.getSubscribers(testTime
						+ topic1);
				assertTrue("Subscriber \"" + testData[i]
						+ "\" does not exist for \"topic " + topic1 + "\"", checkSubscribers(
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
	 * {@link Scalaris#subscribe(String, String)} and
	 * {@link Scalaris#getSubscribers(String)}.
	 * Subscribes some "random" URLs to "random" topics and uses a single topic for all URLs.
	 * 
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 * @throws NotFoundException 
	 */
	@Test
	public void testSubscribe2() throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException {
		String topic = "_Subscribe2";
		Scalaris conn = new Scalaris();

		try {
			for (int i = 0; i < testData.length; ++i) {
				conn.subscribe(
						testTime + topic,
						testData[i] );
			}
			
			// check if the subscribers were successfully saved:
			
			List<String> subscribers = conn
					.getSubscribers(testTime + topic);
			for (int i = 0; i < testData.length; ++i) {
				assertTrue("Subscriber " + testData[i]
						+ " does not exist for topic " + topic, checkSubscribers(
						subscribers, testData[i]));
			}
			
			if (subscribers.size() > testData.length) {
				fail("\"" + getDiffElement(subscribers, testData) + "\" should not be a subscriber of " + topic);
			}
		} finally {
			conn.closeConnection();
		}
	}
	
	// subscribe() test methods end
	// unsubscribe() test methods begin

	/**
	 * Test method for
	 * {@link Scalaris#unsubscribe(OtpErlangString, OtpErlangString)} with a
	 * closed connection.
	 * 
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 * @throws NotFoundException
	 */
	@Test(expected=ConnectionException.class)
	public void testUnsubscribeOtp_NotConnected() throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException {
		String topic = "_UnsubscribeOtp_NotConnected";
		Scalaris conn = new Scalaris();
		conn.closeConnection();
		conn.unsubscribe(
				new OtpErlangString(testTime + topic),
				new OtpErlangString(testData[0]) );
	}
	
	/**
	 * Test method for
	 * {@link Scalaris#unsubscribe(OtpErlangString, OtpErlangString)} and
	 * {@link Scalaris#getSubscribers(OtpErlangString)}.
	 * Tries to unsubscribe an URL from a non-existing topic and tries to get
	 * the subscriber list afterwards.
	 * 
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 * @throws NotFoundException 
	 */
	@Test(expected=NotFoundException.class)
	public void testUnsubscribeOtp_NotExistingTopic() throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException {
		String topic = "_UnsubscribeOtp_NotExistingTopic";
		Scalaris conn = new Scalaris();

		try {
			// unsubscribe test "url":
			conn.unsubscribe(
					new OtpErlangString(testTime + topic),
					new OtpErlangString(testData[0]) );
			
			// check whether the unsubscribed urls were unsubscribed:
			OtpErlangList subscribers = conn
					.getSubscribers(new OtpErlangString(testTime + topic));
			assertFalse("Subscriber \"" + testData[0]
					+ "\" should have been unsubscribed from topic \"" + topic
					+ "\"", checkSubscribers(subscribers, testData[0]));
			
			assertEquals("Subscribers of topic (" + topic
					+ ") should only be [\"\"], but is: "
					+ subscribers.toString(), 0, subscribers.arity());
		} finally {
			conn.closeConnection();
		}
	}
	
	/**
	 * Test method for
	 * {@link Scalaris#unsubscribe(OtpErlangString, OtpErlangString)} and
	 * {@link Scalaris#getSubscribers(OtpErlangString)}.
	 * Tries to unsubscribe an unsubscribed URL from an existing topic and compares
	 * the subscriber list afterwards.
	 * 
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 * @throws NotFoundException 
	 */
	@Test(expected=NotFoundException.class)
	public void testUnsubscribeOtp_NotExistingUrl()
			throws ConnectionException, TimeoutException, UnknownException,
			NotFoundException {
		String topic = "_UnsubscribeOtp_NotExistingUrl";
		Scalaris conn = new Scalaris();

		try {
			// first subscribe test "urls"...
			conn.unsubscribe(new OtpErlangString(testTime + topic),
					new OtpErlangString(testData[0]));
			conn.unsubscribe(new OtpErlangString(testTime + topic),
					new OtpErlangString(testData[1]));

			// then unsubscribe another "url":
			conn.unsubscribe(new OtpErlangString(testTime + topic),
					new OtpErlangString(testData[2]));

			OtpErlangList subscribers = conn
					.getSubscribers(new OtpErlangString(testTime + topic));

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
					+ testData[0] + ", " + testData[1] + "\"], but is: "
					+ subscribers.toString(), 2, subscribers.arity());
		} finally {
			conn.closeConnection();
		}
	}
	
	/**
	 * Test method for
	 * {@link Scalaris#subscribe(OtpErlangString, OtpErlangString)},
	 * {@link Scalaris#unsubscribe(OtpErlangString, OtpErlangString)} and
	 * {@link Scalaris#getSubscribers(OtpErlangString)}.
	 * Subscribes some "random" URLs to "random" topics and uses a distinct topic for each URL.
	 * Unsubscribes every second subscribed URL.
	 * 
	 * @see #testSubscribeOtp1()
	 * 
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 * @throws NotFoundException 
	 */
	@Test
	public void testUnsubscribeOtp1() throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException {
		String topic = "_UnsubscribeOtp1_";
		Scalaris conn = new Scalaris();

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
				OtpErlangList subscribers = conn
						.getSubscribers(new OtpErlangString(testTime + topic1));
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
				OtpErlangList subscribers = conn
						.getSubscribers(new OtpErlangString(testTime + topic1));
				assertFalse("Subscriber \"" + testData[i]
						+ "\" should have been unsubscribed from topic \"" + topic1
						+ "\"", checkSubscribers(subscribers, testData[i]));
				
				assertEquals("Subscribers of topic (" + topic1
						+ ") should only be [\"\"], but is: "
						+ subscribers.toString(), 0, subscribers.arity());
			}
		} finally {
			conn.closeConnection();
		}
	}
	
	/**
	 * Test method for
	 * {@link Scalaris#subscribe(OtpErlangString, OtpErlangString)},
	 * {@link Scalaris#unsubscribe(OtpErlangString, OtpErlangString)} and
	 * {@link Scalaris#getSubscribers(OtpErlangString)}.
	 * Subscribes some "random" URLs to "random" topics and uses a single topic for all URLs.
	 * Unsubscribes every second subscribed URL.
	 * 
	 * @see #testSubscribeOtp2()
	 * 
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 * @throws NotFoundException 
	 */
	@Test
	public void testUnsubscribeOtp2() throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException {
		String topic = "_UnubscribeOtp2";
		Scalaris conn = new Scalaris();

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
			
			OtpErlangList subscribers = conn
					.getSubscribers(new OtpErlangString(testTime + topic));
			String[] subscribers_expected = new String[testData.length / 2];
			// check if the subscribers were successfully saved:
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

			if (subscribers.arity() > testData.length) {
				fail("\"" + getDiffElement(subscribers, subscribers_expected)
						+ " should not be a subscriber of " + topic);
			}
		} finally {
			conn.closeConnection();
		}
	}

	/**
	 * Test method for {@link Scalaris#unsubscribe(String, String)} with a
	 * closed connection.
	 * 
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 * @throws NotFoundException
	 */
	@Test(expected=ConnectionException.class)
	public void testUnsubscribe_NotConnected() throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException {
		String topic = "_Unsubscribe_NotConnected";
		Scalaris conn = new Scalaris();
		conn.closeConnection();
		conn.unsubscribe(testTime + topic, testData[0]);
	}
	
	/**
	 * Test method for
	 * {@link Scalaris#unsubscribe(String, String)} and
	 * {@link Scalaris#getSubscribers(String)}.
	 * Tries to unsubscribe an URL from a non-existing topic and tries to get
	 * the subscriber list afterwards.
	 * 
	 * @see #testUnsubscribeOtp_NotExistingTopic()
	 * 
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 * @throws NotFoundException 
	 */
	@Test(expected=NotFoundException.class)
	public void testUnsubscribe_NotExistingTopic() throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException {
		String topic = "_Unsubscribe_NotExistingTopic";
		Scalaris conn = new Scalaris();

		try {
			// unsubscribe test "url":
			conn.unsubscribe(testTime + topic, testData[0]);
			
			// check whether the unsubscribed urls were unsubscribed:
			List<String> subscribers = conn.getSubscribers(testTime + topic);
			assertFalse("Subscriber \"" + testData[0]
					+ "\" should have been unsubscribed from topic \"" + topic
					+ "\"", checkSubscribers(subscribers, testData[0]));
			
			assertEquals("Subscribers of topic (" + topic
					+ ") should only be [\"\"], but is: "
					+ subscribers.toString(), 0, subscribers.size());
		} finally {
			conn.closeConnection();
		}
	}
	
	/**
	 * Test method for
	 * {@link Scalaris#subscribe(String, String)},
	 * {@link Scalaris#unsubscribe(String, String)} and
	 * {@link Scalaris#getSubscribers(String)}.
	 * Tries to unsubscribe an unsubscribed URL from an existing topic and compares
	 * the subscriber list afterwards.
	 * 
	 * @see #testUnsubscribeOtp_NotExistingUrl()
	 * 
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 * @throws NotFoundException 
	 */
	@Test(expected=NotFoundException.class)
	public void testUnsubscribe_NotExistingUrl()
			throws ConnectionException, TimeoutException, UnknownException,
			NotFoundException {
		String topic = "_Unsubscribe_NotExistingUrl";
		Scalaris conn = new Scalaris();

		try {
			// first subscribe test "urls"...
			conn.subscribe(testTime + topic, testData[0]);
			conn.subscribe(testTime + topic, testData[1]);

			// then unsubscribe another "url":
			conn.unsubscribe(testTime + topic, testData[2]);

			List<String> subscribers = conn.getSubscribers(testTime + topic);

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
					+ testData[0] + ", " + testData[1] + "\"], but is: "
					+ subscribers.toString(), 2, subscribers.size());
		} finally {
			conn.closeConnection();
		}
	}
	
	/**
	 * Test method for
	 * {@link Scalaris#subscribe(String, String)},
	 * {@link Scalaris#unsubscribe(String, String)} and
	 * {@link Scalaris#getSubscribers(String)}.
	 * Subscribes some "random" URLs to "random" topics and uses a distinct topic for each URL.
	 * Unsubscribes every second subscribed URL.
	 * 
	 * @see #testSubscribe1()
	 * 
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 * @throws NotFoundException 
	 */
	@Test
	public void testUnsubscribe1() throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException {
		String topic = "_SingleUnsubscribeString1_";
		Scalaris conn = new Scalaris();

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
				List<String> subscribers = conn.getSubscribers(testTime + topic1);
				assertTrue("Subscriber \"" + testData[i]
	  					+ "\" does not exist for \"topic " + topic1 + "\"", checkSubscribers(
	  					subscribers, testData[i]));
				
				assertEquals("Subscribers of topic (" + topic1
						+ ") should only be [" + testData[i] + "], but is: "
						+ subscribers.toString(), 1, subscribers.size());
			}
			// check whether the unsubscribed urls were unsubscribed:
			for (int i = 0; i < testData.length; i += 2) {
				String topic1 = topic + i;
				List<String> subscribers = conn.getSubscribers(testTime
						+ topic1);
				assertFalse("Subscriber \"" + testData[i]
						+ "\" should have been unsubscribed from topic \"" + topic1 + "\"", checkSubscribers(
						subscribers, testData[i]));
				
				assertEquals("Subscribers of topic (" + topic1
						+ ") should only be [\"\"], but is: "
						+ subscribers.toString(), 0, subscribers.size());
			}
		} finally {
			conn.closeConnection();
		}
	}

	/**
	 * Test method for
	 * {@link Scalaris#subscribe(String, String)},
	 * {@link Scalaris#unsubscribe(String, String)} and
	 * {@link Scalaris#getSubscribers(String)}.
	 * Subscribes some "random" URLs to "random" topics and uses a single topic for all URLs.
	 * Unsubscribes every second subscribed URL.
	 * 
	 * @see #testSubscribe2()
	 * 
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 * @throws NotFoundException 
	 */
	@Test
	public void testUnsubscribe2() throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException {
		String topic = "_SingleUnubscribeString2";
		Scalaris conn = new Scalaris();

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
			
			List<String> subscribers = conn.getSubscribers(testTime + topic);
			String[] subscribers_expected = new String[testData.length / 2];
			// check if the subscribers were successfully saved:
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

			if (subscribers.size() > testData.length) {
				fail("\"" + getDiffElement(subscribers, subscribers_expected)
						+ " should not be a subscriber of " + topic);
			}
		} finally {
			conn.closeConnection();
		}
	}
	
	// unsubscribe() test methods end
	
	/**
	 * Test method for the publish/subscribe system.
	 * Single server, subscription to one topic, multiple publishs.
	 * 
	 * @throws Exception 
	 */
	@Test
	public void testSubscription1() throws Exception {
		String topic = testTime + "_Subscription1";
		Scalaris conn = new Scalaris();
		notifications_server1 = new HashMap<String, Vector<String>>();
		Server server1 = new Server(startPort);

		try {
			server1.setHandler(new SubscriptionHandler(notifications_server1));
			server1.start();
			
			conn.subscribe(topic, "http://127.0.0.1:" + startPort);
			
			for (int i = 0; i < testData.length; ++i) {
				conn.publish(topic, testData[i]);
			}
			
			// wait max 'notifications_timeout' seconds for notifications:
			for (int i = 0; i < notifications_timeout
					&& (notifications_server1.get(topic) == null || notifications_server1.get(topic).size() < testData.length); ++i) {
				TimeUnit.SECONDS.sleep(1);
			}
			
			Vector<String> successfullNotifications1 = notifications_server1.get(topic);
			for (int i = 0; i < testData.length; ++i) {
				assertTrue("subscription (" + topic + ", " + testData[i]
						+ ") not received by server)", successfullNotifications1
						.contains(testData[i]));
				successfullNotifications1.remove(testData[i]);
			}
			if (successfullNotifications1.size() > 0) {
				fail("Received element (" + topic + ", "
						+ successfullNotifications1.get(0)
						+ ") which is not part of the subscription.");
			}
			
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
		Scalaris conn = new Scalaris();
		notifications_server1 = new HashMap<String, Vector<String>>();
		notifications_server2 = new HashMap<String, Vector<String>>();
		notifications_server3 = new HashMap<String, Vector<String>>();
		Server server1 = new Server(startPort);
		Server server2 = new Server(startPort + 1);
		Server server3 = new Server(startPort + 2);

		try {
			server1.setHandler(new SubscriptionHandler(notifications_server1));
			server1.start();
			server2.setHandler(new SubscriptionHandler(notifications_server2));
			server2.start();
			server3.setHandler(new SubscriptionHandler(notifications_server3));
			server3.start();
			
			conn.subscribe(topic, "http://127.0.0.1:" + startPort);
			conn.subscribe(topic, "http://127.0.0.1:" + (startPort + 1));
			conn.subscribe(topic, "http://127.0.0.1:" + (startPort + 2));
			
			for (int i = 0; i < testData.length; ++i) {
				conn.publish(topic, testData[i]);
			}
			
			// wait max 'notifications_timeout' seconds for notifications:
			for (int i = 0; i < notifications_timeout
					&& (notifications_server1.get(topic) == null || 
						notifications_server1.get(topic).size() < testData.length ||
						notifications_server2.get(topic) == null ||
						notifications_server2.get(topic).size() < testData.length ||
						notifications_server3.get(topic) == null ||
						notifications_server3.get(topic).size() < testData.length); ++i) {
				TimeUnit.SECONDS.sleep(1);
			}
			
			Vector<String> successfullNotifications1 = notifications_server1.get(topic);
			for (int i = 0; i < testData.length; ++i) {
				assertTrue("subscription (" + topic + ", " + testData[i]
						+ ") not received by server)", successfullNotifications1
						.contains(testData[i]));
				successfullNotifications1.remove(testData[i]);
			}
			if (successfullNotifications1.size() > 0) {
				fail("Received element (" + topic + ", "
						+ successfullNotifications1.get(0)
						+ ") which is not part of the subscription.");
			}
			
			Vector<String> successfullNotifications2 = notifications_server2.get(topic);
			for (int i = 0; i < testData.length; ++i) {
				assertTrue("subscription (" + topic + ", " + testData[i]
						+ ") not received by server)", successfullNotifications2
						.contains(testData[i]));
				successfullNotifications2.remove(testData[i]);
			}
			if (successfullNotifications2.size() > 0) {
				fail("Received element (" + topic + ", "
						+ successfullNotifications2.get(0)
						+ ") which is not part of the subscription.");
			}
			
			Vector<String> successfullNotifications3 = notifications_server3.get(topic);
			for (int i = 0; i < testData.length; ++i) {
				assertTrue("subscription (" + topic + ", " + testData[i]
						+ ") not received by server)", successfullNotifications3
						.contains(testData[i]));
				successfullNotifications3.remove(testData[i]);
			}
			if (successfullNotifications3.size() > 0) {
				fail("Received element (" + topic + ", "
						+ successfullNotifications3.get(0)
						+ ") which is not part of the subscription.");
			}
			
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
		Scalaris conn = new Scalaris();
		notifications_server1 = new HashMap<String, Vector<String>>();
		notifications_server2 = new HashMap<String, Vector<String>>();
		notifications_server3 = new HashMap<String, Vector<String>>();
		Server server1 = new Server(startPort);
		Server server2 = new Server(startPort + 1);
		Server server3 = new Server(startPort + 2);

		try {
			server1.setHandler(new SubscriptionHandler(notifications_server1));
			server1.start();
			server2.setHandler(new SubscriptionHandler(notifications_server2));
			server2.start();
			server3.setHandler(new SubscriptionHandler(notifications_server3));
			server3.start();
			
			conn.subscribe(topic1, "http://127.0.0.1:" + startPort);
			conn.subscribe(topic2, "http://127.0.0.1:" + (startPort + 1));
			conn.subscribe(topic3, "http://127.0.0.1:" + (startPort + 2));
			
			int topic1_elements = 0;
			int topic3_elements = 0;
			int topic2_elements = 0;
			for (int i = 0; i < testData.length; ++i) {
				if (i % 2 == 0) {
					conn.publish(topic1, testData[i]);
					++topic1_elements;
				}
				if (i % 3 == 0) {
					conn.publish(topic2, testData[i]);
					++topic2_elements;
				}
				if (i % 5 == 0) {
					conn.publish(topic3, testData[i]);
					++topic3_elements;
				}
			}
			
			// wait max 'notifications_timeout' seconds for notifications:
			for (int i = 0; i < notifications_timeout
					&& (notifications_server1.get(topic1) == null || 
						notifications_server1.get(topic1).size() < topic1_elements ||
						notifications_server2.get(topic2) == null ||
						notifications_server2.get(topic2).size() < topic2_elements ||
						notifications_server3.get(topic3) == null ||
						notifications_server3.get(topic3).size() < topic3_elements); ++i) {
				TimeUnit.SECONDS.sleep(1);
			}
			
			Vector<String> successfullNotifications1 = notifications_server1.get(topic1);
			for (int i = 0; i < testData.length; i += 2) {
				assertTrue("subscription (" + topic1 + ", " + testData[i]
						+ ") not received by server)", successfullNotifications1
						.contains(testData[i]));
				successfullNotifications1.remove(testData[i]);
			}
			if (successfullNotifications1.size() > 0) {
				fail("Received element (" + topic1 + ", "
						+ successfullNotifications1.get(0)
						+ ") which is not part of the subscription.");
			}
			
			Vector<String> successfullNotifications2 = notifications_server2.get(topic2);
			for (int i = 0; i < testData.length; i += 3) {
				assertTrue("subscription (" + topic2 + ", " + testData[i]
						+ ") not received by server)", successfullNotifications2
						.contains(testData[i]));
				successfullNotifications2.remove(testData[i]);
			}
			if (successfullNotifications2.size() > 0) {
				fail("Received element (" + topic2 + ", "
						+ successfullNotifications2.get(0)
						+ ") which is not part of the subscription.");
			}
			
			
			Vector<String> successfullNotifications3 = notifications_server3.get(topic3);
			for (int i = 0; i < testData.length; i += 5) {
				assertTrue("subscription (" + topic3 + ", " + testData[i]
						+ ") not received by server)", successfullNotifications3
						.contains(testData[i]));
				successfullNotifications3.remove(testData[i]);
			}

			if (successfullNotifications3.size() > 0) {
				fail("Received element (" + topic3 + ", "
						+ successfullNotifications3.get(0)
						+ ") which is not part of the subscription.");
			}
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
		Scalaris conn = new Scalaris();
		notifications_server1 = new HashMap<String, Vector<String>>();
		notifications_server2 = new HashMap<String, Vector<String>>();
		notifications_server3 = new HashMap<String, Vector<String>>();
		Server server1 = new Server(startPort);
		Server server2 = new Server(startPort + 1);
		Server server3 = new Server(startPort + 2);

		try {
			server1.setHandler(new SubscriptionHandler(notifications_server1));
			server1.start();
			server2.setHandler(new SubscriptionHandler(notifications_server2));
			server2.start();
			server3.setHandler(new SubscriptionHandler(notifications_server3));
			server3.start();
			
			conn.subscribe(topic1, "http://127.0.0.1:" + startPort);
			conn.subscribe(topic2, "http://127.0.0.1:" + (startPort + 1));
			conn.subscribe(topic3, "http://127.0.0.1:" + (startPort + 2));
			conn.unsubscribe(topic2, "http://127.0.0.1:" + (startPort + 1));
			
			int topic1_elements = 0;
			int topic3_elements = 0;
			int topic2_elements = 0;
			for (int i = 0; i < testData.length; ++i) {
				if (i % 2 == 0) {
					conn.publish(topic1, testData[i]);
					++topic1_elements;
				}
				if (i % 3 == 0) {
					conn.publish(topic2, testData[i]);
					++topic2_elements;
				}
				if (i % 5 == 0) {
					conn.publish(topic3, testData[i]);
					++topic3_elements;
				}
			}
			
			// wait max 'notifications_timeout' seconds for notifications:
			for (int i = 0; i < notifications_timeout
					&& (notifications_server1.get(topic1) == null || 
						notifications_server1.get(topic1).size() < topic1_elements ||
						notifications_server3.get(topic3) == null ||
						notifications_server3.get(topic3).size() < topic3_elements); ++i) {
				TimeUnit.SECONDS.sleep(1);
			}
			
			Vector<String> successfullNotifications1 = notifications_server1.get(topic1);
			for (int i = 0; i < testData.length; i += 2) {
				assertTrue("subscription (" + topic1 + ", " + testData[i]
						+ ") not received by server)", successfullNotifications1
						.contains(testData[i]));
				successfullNotifications1.remove(testData[i]);
			}
			if (successfullNotifications1.size() > 0) {
				fail("Received element (" + topic1 + ", "
						+ successfullNotifications1.get(0)
						+ ") which is not part of the subscription.");
			}
			
			Vector<String> successfullNotifications2 = notifications_server2.get(topic2);
			if (successfullNotifications2 != null && successfullNotifications2.size() > 0) {
				fail("Received element (" + topic2 + ", "
						+ successfullNotifications2.get(0)
						+ ") although the server was unsubscribed.");
			}
			
			Vector<String> successfullNotifications3 = notifications_server3.get(topic3);
			for (int i = 0; i < testData.length; i += 5) {
				assertTrue("subscription (" + topic3 + ", " + testData[i]
						+ ") not received by server)", successfullNotifications3
						.contains(testData[i]));
				successfullNotifications3.remove(testData[i]);
			}

			if (successfullNotifications3.size() > 0) {
				fail("Received element (" + topic3 + ", "
						+ successfullNotifications3.get(0)
						+ ") which is not part of the subscription.");
			}
			
		} finally {
			server1.stop();
			server2.stop();
			server3.stop();
			conn.closeConnection();
		}
	}
	
	private class SubscriptionHandler extends AbstractHandler {
		public Map<String, Vector<String>> notifications;
		
		public SubscriptionHandler(Map<String, Vector<String>> notifications) {
			this.notifications = notifications;
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

		public void handle(String target, HttpServletRequest request,
				HttpServletResponse response, int dispatch) throws IOException,
				ServletException {
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
//				notifications.put(topic, content);
			}

			out.println("{}");

			((Request) request).setHandled(true);
		}
	}
}
