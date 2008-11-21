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
package de.zib.chordsharp;

import static org.junit.Assert.*;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Reader;
import java.util.Arrays;
import java.util.HashMap;
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
 * Tests correct behaviour of the ChordSharpConnection class.
 * 
 * @author Nico Kruber, kruber@zib.de
 * 
 * @version 1.4
 */
public class ChordSharpConnectionTest {
	private static long testTime = System.currentTimeMillis();
	
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
	 * {@link ChordSharpConnection#ChordSharpConnection()}.
	 * @throws ConnectionException 
	 */
	@Test
	public void testChordSharpConnection() throws ConnectionException {
		ChordSharpConnection conn = new ChordSharpConnection();
		conn.getClass(); // just some random call on conn to suppress unused warning
	}

	/**
	 * Test method for
	 * {@link ChordSharpConnection#singleReadString(OtpErlangString)}.
	 * 
	 * @throws NotFoundException
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 */
	@Test(expected=NotFoundException.class)
	public void testSingleReadStringOtp_NotFound() throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException {
		ChordSharpConnection conn = new ChordSharpConnection();
		conn.singleReadString(new OtpErlangString(testTime + "_notFound"));
	}

	/**
	 * Test method for {@link ChordSharpConnection#singleReadString(String)}.
	 * 
	 * @throws NotFoundException
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 */
	@Test(expected=NotFoundException.class)
	public void testSingleReadString_NotFound() throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException {
		ChordSharpConnection conn = new ChordSharpConnection();
		conn.singleReadString(testTime + "_notFound");
	}

	/**
	 * Test method for {@link ChordSharpConnection#readString(OtpErlangString)}.
	 * 
	 * @throws NotFoundException
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 */
	@Test(expected=NotFoundException.class)
	public void testReadStringOtp_NotFound() throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException {
		ChordSharpConnection.readString(new OtpErlangString(testTime + "_notFound"));
	}

	/**
	 * Test method for {@link ChordSharpConnection#readString(String)}.
	 * 
	 * @throws NotFoundException
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 */
	@Test(expected=NotFoundException.class)
	public void testReadString_NotFound() throws ConnectionException, TimeoutException,
			UnknownException, NotFoundException {
		ChordSharpConnection.readString(testTime + "_notFound");
	}

	/**
	 * Test method for
	 * {@link ChordSharpConnection#singleWrite(OtpErlangString, OtpErlangObject)}
	 * and {@link ChordSharpConnection#singleReadString(OtpErlangString)}.
	 * Writes strings and uses a distinct key for each value. Tries to read the data afterwards.
	 * 
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 * @throws NotFoundException 
	 */
	@Test
	public void testSingleWriteOtpString1() throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException {
		ChordSharpConnection conn = new ChordSharpConnection();

		for (int i = 0; i < testData.length; ++i) {
			conn.singleWrite(
					new OtpErlangString(testTime + "_SingleWriteOtpString1_" + i),
					new OtpErlangString(testData[i]) );
		}
		
		// now try to read the data:
		
		for (int i = 0; i < testData.length; ++i) {
			String actual = conn.singleReadString(
					new OtpErlangString(testTime + "_SingleWriteOtpString1_" + i)).stringValue();
			assertEquals(testData[i], actual);
		}
	}
	
	/**
	 * Test method for
	 * {@link ChordSharpConnection#singleWrite(OtpErlangString, OtpErlangObject)}
	 * and {@link ChordSharpConnection#singleReadString(OtpErlangString)}.
	 * Writes strings and uses a single key for all the values. Tries to read the data afterwards.
	 * 
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 * @throws NotFoundException 
	 */
	@Test
	public void testSingleWriteOtpString2() throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException {
		ChordSharpConnection conn = new ChordSharpConnection();

		for (int i = 0; i < testData.length; ++i) {
			conn.singleWrite(
					new OtpErlangString(testTime + "_SingleWriteOtpString2"),
					new OtpErlangString(testData[i]) );
		}
		
		// now try to read the data:
		
		String actual = conn.singleReadString(
				new OtpErlangString(testTime + "_SingleWriteOtpString2")).stringValue();
		assertEquals(testData[testData.length - 1], actual);
	}
	
	/**
	 * Test method for
	 * {@link ChordSharpConnection#singleWrite(OtpErlangString, OtpErlangObject)}
	 * and {@link ChordSharpConnection#singleReadObject(OtpErlangString)}.
	 * Writes erlang tuples and uses a distinct key for each value. Tries to read the data afterwards.
	 * 
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 * @throws NotFoundException 
	 */
	@Test
	public void testSingleWriteOtpObject1() throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException {
		ChordSharpConnection conn = new ChordSharpConnection();

		for (int i = 0; i < testData.length - 1; i += 2) {
			OtpErlangObject[] data = new OtpErlangObject[] {
					new OtpErlangString(testData[i]),
					new OtpErlangString(testData[i + 1]) };
			conn.singleWrite(
					new OtpErlangString(testTime + "_SingleWriteOtpObject1_" + i),
					new OtpErlangTuple(data) );
		}
		
		// now try to read the data:
		
		for (int i = 0; i < testData.length - 1; i += 2) {
			OtpErlangObject[] data = new OtpErlangObject[] {
					new OtpErlangString(testData[i]),
					new OtpErlangString(testData[i + 1]) };
			OtpErlangObject actual = conn.singleReadObject(
					new OtpErlangString(testTime + "_SingleWriteOtpObject1_" + i));
			OtpErlangTuple expected = new OtpErlangTuple(data);
			
			assertEquals(expected, actual);
		}
	}
	
	/**
	 * Test method for
	 * {@link ChordSharpConnection#singleWrite(OtpErlangString, OtpErlangObject)}
	 * and {@link ChordSharpConnection#singleReadObject(OtpErlangString)}.
	 * Writes erlang tuples and uses a single key for all the values. Tries to read the data afterwards.
	 * 
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 * @throws NotFoundException 
	 */
	@Test
	public void testSingleWriteOtpObject2() throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException {
		ChordSharpConnection conn = new ChordSharpConnection();

		OtpErlangObject[] data = new OtpErlangObject[0];
		for (int i = 0; i < testData.length - 1; i += 2) {
			data = new OtpErlangObject[] {
					new OtpErlangString(testData[i]),
					new OtpErlangString(testData[i + 1]) };
			conn.singleWrite(
					new OtpErlangString(testTime + "_SingleWriteOtpObject2"),
					new OtpErlangTuple(data) );
		}
		
		// now try to read the data:
		
		OtpErlangObject actual = conn.singleReadObject(
				new OtpErlangString(testTime + "_SingleWriteOtpObject2"));
		OtpErlangTuple expected = new OtpErlangTuple(data);
		
		assertEquals(expected, actual);
	}

	/**
	 * Test method for
	 * {@link ChordSharpConnection#singleWrite(String, String)}
	 * and {@link ChordSharpConnection#singleReadString(String)}.
	 * Writes strings and uses a distinct key for each value. Tries to read the data afterwards.
	 * 
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 * @throws NotFoundException 
	 */
	@Test
	public void testSingleWriteString1() throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException {
		ChordSharpConnection conn = new ChordSharpConnection();

		for (int i = 0; i < testData.length; ++i) {
			conn.singleWrite(
					testTime + "_SingleWriteString1_" + i,
					testData[i] );
		}
		
		// now try to read the data:
		
		for (int i = 0; i < testData.length; ++i) {
			String actual = conn.singleReadString(
					testTime + "_SingleWriteString1_" + i);
			assertEquals(testData[i], actual);
		}
	}
	
	/**
	 * Test method for
	 * {@link ChordSharpConnection#singleWrite(String, String)}
	 * and {@link ChordSharpConnection#singleReadString(String)}.
	 * Writes strings and uses a single key for all the values. Tries to read the data afterwards.
	 * 
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 * @throws NotFoundException 
	 */
	@Test
	public void testSingleWriteString2() throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException {
		ChordSharpConnection conn = new ChordSharpConnection();

		for (int i = 0; i < testData.length; ++i) {
			conn.singleWrite(
					testTime + "_SingleWriteString2",
					testData[i] );
		}
		
		// now try to read the data:
		
		String actual = conn.singleReadString(
				testTime + "_SingleWriteString2");
		assertEquals(testData[testData.length - 1], actual);
	}

	/**
	 * Test method for
	 * {@link ChordSharpConnection#write(OtpErlangString, OtpErlangObject)}
	 * and {@link ChordSharpConnection#readString(OtpErlangString)}.
	 * Writes strings and uses a distinct key for each value. Tries to read the data afterwards.
	 * 
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 * @throws NotFoundException 
	 */
	@Test
	public void testWriteOtpString1() throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException {
		for (int i = 0; i < testData.length; ++i) {
			ChordSharpConnection.write(
					new OtpErlangString(testTime + "_WriteOtpString1_" + i),
					new OtpErlangString(testData[i]) );
		}
		
		// now try to read the data:
		
		for (int i = 0; i < testData.length; ++i) {
			String actual = ChordSharpConnection.readString(
					new OtpErlangString(testTime + "_WriteOtpString1_" + i)).stringValue();
			assertEquals(testData[i], actual);
		}
	}
	
	/**
	 * Test method for
	 * {@link ChordSharpConnection#write(OtpErlangString, OtpErlangObject)}
	 * and {@link ChordSharpConnection#readString(OtpErlangString)}.
	 * Writes strings and uses a single key for all the values. Tries to read the data afterwards.
	 * 
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 * @throws NotFoundException 
	 */
	@Test
	public void testWriteOtpString2() throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException {
		for (int i = 0; i < testData.length; ++i) {
			ChordSharpConnection.write(
					new OtpErlangString(testTime + "_WriteOtpString2"),
					new OtpErlangString(testData[i]) );
		}
		
		// now try to read the data:
		
		String actual = ChordSharpConnection.readString(
				new OtpErlangString(testTime + "_WriteOtpString2")).stringValue();
		assertEquals(testData[testData.length - 1], actual);
	}
	
	/**
	 * Test method for
	 * {@link ChordSharpConnection#write(OtpErlangString, OtpErlangObject)}
	 * and {@link ChordSharpConnection#readObject(OtpErlangString)}.
	 * Writes erlang tuples and uses a distinct key for each value. Tries to read the data afterwards.
	 * 
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 * @throws NotFoundException 
	 */
	@Test
	public void testwriteOtpObject1() throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException {
		for (int i = 0; i < testData.length - 1; i += 2) {
			OtpErlangObject[] data = new OtpErlangObject[] {
					new OtpErlangString(testData[i]),
					new OtpErlangString(testData[i + 1]) };
			ChordSharpConnection.write(
					new OtpErlangString(testTime + "_WriteOtpObject1_" + i),
					new OtpErlangTuple(data) );
		}
		
		// now try to read the data:
		
		for (int i = 0; i < testData.length - 1; i += 2) {
			OtpErlangObject[] data = new OtpErlangObject[] {
					new OtpErlangString(testData[i]),
					new OtpErlangString(testData[i + 1]) };
			OtpErlangObject actual = ChordSharpConnection.readObject(
					new OtpErlangString(testTime + "_WriteOtpObject1_" + i));
			OtpErlangTuple expected = new OtpErlangTuple(data);
			
			assertEquals(expected, actual);
		}
	}
	
	/**
	 * Test method for
	 * {@link ChordSharpConnection#write(OtpErlangString, OtpErlangObject)}
	 * and {@link ChordSharpConnection#readObject(OtpErlangString)}.
	 * Writes erlang tuples and uses a single key for all the values. Tries to read the data afterwards.
	 * 
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 * @throws NotFoundException 
	 */
	@Test
	public void testWriteOtpObject2() throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException {
		OtpErlangObject[] data = new OtpErlangObject[0];
		for (int i = 0; i < testData.length - 1; i += 2) {
			data = new OtpErlangObject[] {
					new OtpErlangString(testData[i]),
					new OtpErlangString(testData[i + 1]) };
			ChordSharpConnection.write(
					new OtpErlangString(testTime + "_WriteOtpObject2"),
					new OtpErlangTuple(data) );
		}
		
		// now try to read the data:
		
		OtpErlangObject actual = ChordSharpConnection.readObject(
				new OtpErlangString(testTime + "_WriteOtpObject2"));
		OtpErlangTuple expected = new OtpErlangTuple(data);
		
		assertEquals(expected, actual);
	}

	/**
	 * Test method for
	 * {@link ChordSharpConnection#write(String, String)}
	 * and {@link ChordSharpConnection#readString(String)}.
	 * Writes strings and uses a distinct key for each value. Tries to read the data afterwards.
	 * 
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 * @throws NotFoundException 
	 */
	@Test
	public void testWriteString1() throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException {
		for (int i = 0; i < testData.length; ++i) {
			ChordSharpConnection.write(
					testTime + "_WriteString1_" + i,
					testData[i] );
		}
		
		// now try to read the data:
		
		for (int i = 0; i < testData.length; ++i) {
			String actual = ChordSharpConnection.readString(
					testTime + "_WriteString1_" + i);
			assertEquals(testData[i], actual);
		}
	}
	
	/**
	 * Test method for
	 * {@link ChordSharpConnection#write(String, String)}
	 * and {@link ChordSharpConnection#readString(String)}.
	 * Writes strings and uses a single key for all the values. Tries to read the data afterwards.
	 * 
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 * @throws NotFoundException 
	 */
	@Test
	public void testWriteString2() throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException {
		for (int i = 0; i < testData.length; ++i) {
			ChordSharpConnection.write(
					testTime + "_WriteString2",
					testData[i] );
		}
		
		// now try to read the data:
		
		String actual = ChordSharpConnection.readString(
				testTime + "_WriteString2");
		assertEquals(testData[testData.length - 1], actual);
	}
	
	/**
	 * Test method for
	 * {@link ChordSharpConnection#singlePublish(OtpErlangString, OtpErlangString)}.
	 * Publishes some topics and uses a distinct key for each value.
	 * 
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 * @throws NotFoundException 
	 */
	@Test
	public void testSinglePublishOtpString1() throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException {
		ChordSharpConnection conn = new ChordSharpConnection();

		for (int i = 0; i < testData.length; ++i) {
			conn.singlePublish(
					new OtpErlangString(testTime + "_SinglePublishOtpString1_" + i),
					new OtpErlangString(testData[i]) );
		}
	}
	
	/**
	 * Test method for
	 * {@link ChordSharpConnection#singlePublish(OtpErlangString, OtpErlangString)}.
	 * Publishes some topics and uses a single key for all the values.
	 * 
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 * @throws NotFoundException 
	 */
	@Test
	public void testSinglePublishOtpString2() throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException {
		ChordSharpConnection conn = new ChordSharpConnection();

		for (int i = 0; i < testData.length; ++i) {
			conn.singlePublish(
					new OtpErlangString(testTime + "_SinglePublishOtpString2"),
					new OtpErlangString(testData[i]) );
		}
	}

	/**
	 * Test method for
	 * {@link ChordSharpConnection#singlePublish(String, String)}.
	 * Publishes some topics and uses a distinct key for each value.
	 * 
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 * @throws NotFoundException 
	 */
	@Test
	public void testSinglePublishString1() throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException {
		ChordSharpConnection conn = new ChordSharpConnection();

		for (int i = 0; i < testData.length; ++i) {
			conn.singlePublish(
					testTime + "_SinglePublishString1_" + i,
					testData[i] );
		}
	}
	
	/**
	 * Test method for
	 * {@link ChordSharpConnection#singlePublish(String, String)}.
	 * Publishes some topics and uses a single key for all the values.
	 * 
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 * @throws NotFoundException 
	 */
	@Test
	public void testSinglePublishString2() throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException {
		ChordSharpConnection conn = new ChordSharpConnection();

		for (int i = 0; i < testData.length; ++i) {
			conn.singlePublish(
					testTime + "_SinglePublishString2",
					testData[i] );
		}
	}
	
	/**
	 * Test method for
	 * {@link ChordSharpConnection#publish(OtpErlangString, OtpErlangString)}.
	 * Publishes some topics and uses a distinct key for each value.
	 * 
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 * @throws NotFoundException 
	 */
	@Test
	public void testPublishOtpString1() throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException {
		for (int i = 0; i < testData.length; ++i) {
			ChordSharpConnection.publish(
					new OtpErlangString(testTime + "_PublishOtpString1_" + i),
					new OtpErlangString(testData[i]) );
		}
	}
	
	/**
	 * Test method for
	 * {@link ChordSharpConnection#publish(OtpErlangString, OtpErlangString)}.
	 * Publishes some topics and uses a single key for all the values.
	 * 
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 * @throws NotFoundException 
	 */
	@Test
	public void testPublishOtpString2() throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException {
		for (int i = 0; i < testData.length; ++i) {
			ChordSharpConnection.publish(
					new OtpErlangString(testTime + "_PublishOtpString2"),
					new OtpErlangString(testData[i]) );
		}
	}

	/**
	 * Test method for
	 * {@link ChordSharpConnection#publish(String, String)}.
	 * Publishes some topics and uses a distinct key for each value.
	 * 
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 * @throws NotFoundException 
	 */
	@Test
	public void testPublishString1() throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException {
		for (int i = 0; i < testData.length; ++i) {
			ChordSharpConnection.publish(
					testTime + "_PublishString1_" + i,
					testData[i] );
		}
	}
	
	/**
	 * Test method for
	 * {@link ChordSharpConnection#publish(String, String)}.
	 * Publishes some topics and uses a single key for all the values.
	 * 
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 * @throws NotFoundException 
	 */
	@Test
	public void testPublishString2() throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException {
		for (int i = 0; i < testData.length; ++i) {
			ChordSharpConnection.publish(
					testTime + "_PublishString2",
					testData[i] );
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
	private boolean checkSubscribers(Vector<String> list, String subscriber) {
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
	private String getDiffElement(Vector<String> list, String[] expectedElements) {
		Vector<String> expectedElements2 = new Vector<String>(Arrays.asList(expectedElements));
		list.removeAll(expectedElements2);
		
		if (list.size() > 0) {
			return list.elementAt(0);
		} else {
			return null;
		}
	}
	
	// getSubscribers() test methods for not existing topics begin
	
	/**
	 * Test method for
	 * {@link ChordSharpConnection#singleGetSubscribers(OtpErlangString)}.
	 * Tries to get a subscriber list from an empty topic.
	 * 
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 * @throws NotFoundException 
	 */
	@Test
	public void testSingleGetSubscribersOtpString_NotExistingTopic() throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException {
		String topic = "_SingleGetSubscribersOtpString_NotExistingTopic";
		ChordSharpConnection conn = new ChordSharpConnection();
		
		conn.singleGetSubscribers(new OtpErlangString(testTime + topic));
	}
	
	/**
	 * Test method for
	 * {@link ChordSharpConnection#singleGetSubscribers(String)}.
	 * Tries to get a subscriber list from an empty topic.
	 * 
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 * @throws NotFoundException 
	 */
	@Test
	public void testSingleGetSubscribersString_NotExistingTopic() throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException {
		String topic = "_SingleGetSubscribersString_NotExistingTopic";
		ChordSharpConnection conn = new ChordSharpConnection();
		
		conn.singleGetSubscribers(testTime + topic);
	}
	
	/**
	 * Test method for
	 * {@link ChordSharpConnection#getSubscribers(OtpErlangString)}.
	 * Tries to get a subscriber list from an empty topic.
	 * 
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 * @throws NotFoundException 
	 */
	@Test
	public void testGetSubscribersOtpString_NotExistingTopic() throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException {
		String topic = "_GetSubscribersOtpString_NotExistingTopic";
		ChordSharpConnection.getSubscribers(new OtpErlangString(testTime + topic));
	}
	
	/**
	 * Test method for
	 * {@link ChordSharpConnection#getSubscribers(String)}.
	 * Tries to get a subscriber list from an empty topic.
	 * 
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 * @throws NotFoundException 
	 */
	@Test
	public void testGetSubscribersString_NotExistingTopic() throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException {
		String topic = "_GetSubscribersString_NotExistingTopic";
		ChordSharpConnection.getSubscribers(testTime + topic);
	}
	
	// getSubscribers() test methods for not existing topics begin
	// subscribe() test methods begin
	
	/**
	 * Test method for
	 * {@link ChordSharpConnection#singleSubscribe(OtpErlangString, OtpErlangString)} and
	 * {@link ChordSharpConnection#singleGetSubscribers(OtpErlangString)}.
	 * Subscribes some "random" URLs to "random" topics and uses a distinct topic for each URL.
	 * 
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 * @throws NotFoundException 
	 */
	@Test
	public void testSingleSubscribeOtpString1() throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException {
		ChordSharpConnection conn = new ChordSharpConnection();

		for (int i = 0; i < testData.length; ++i) {
			conn.singleSubscribe(
					new OtpErlangString(testTime + "_SingleSubscribeOtpString1_" + i),
					new OtpErlangString(testData[i]) );
		}
		
		// check if the subscribers were successfully saved:
		
		for (int i = 0; i < testData.length; ++i) {
			String topic = "_SingleSubscribeOtpString1_" + i;
			OtpErlangList subscribers = conn
					.singleGetSubscribers(new OtpErlangString(testTime
							+ topic));
			assertTrue("Subscriber \"" + testData[i]
					+ "\" does not exist for topic \"" + topic + "\"", checkSubscribers(
					subscribers, testData[i]));
			
			assertEquals("Subscribers of topic (" + topic
					+ ") should only be [\"" + testData[i] + "\"], but is: "
					+ subscribers.toString(), 1, subscribers.arity());
		}
	}
	
	/**
	 * Test method for
	 * {@link ChordSharpConnection#singleSubscribe(OtpErlangString, OtpErlangString)} and
	 * {@link ChordSharpConnection#singleGetSubscribers(OtpErlangString)}.
	 * Subscribes some "random" URLs to "random" topics and uses a single topic for all URLs.
	 * 
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 * @throws NotFoundException 
	 */
	@Test
	public void testSingleSubscribeOtpString2() throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException {
		ChordSharpConnection conn = new ChordSharpConnection();

		for (int i = 0; i < testData.length; ++i) {
			conn.singleSubscribe(
					new OtpErlangString(testTime + "_SingleSubscribeOtpString2"),
					new OtpErlangString(testData[i]) );
		}
		
		// check if the subscribers were successfully saved:
		
		String topic = "_SingleSubscribeOtpString2";
		OtpErlangList subscribers = conn
				.singleGetSubscribers(new OtpErlangString(testTime + topic));
		for (int i = 0; i < testData.length; ++i) {
			assertTrue("Subscriber \"" + testData[i]
					+ "\" does not exist for topic \"" + topic + "\"", checkSubscribers(
					subscribers, testData[i]));
		}
		
		if (subscribers.arity() > testData.length) {
			fail("\"" + getDiffElement(subscribers, testData) + " should not be a subscriber of " + topic);
		}
	}
	
	/**
	 * Test method for
	 * {@link ChordSharpConnection#singleSubscribe(String, String)} and
	 * {@link ChordSharpConnection#singleGetSubscribers(String)}.
	 * Subscribes some "random" URLs to "random" topics and uses a distinct topic for each URL.
	 * 
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 * @throws NotFoundException 
	 */
	@Test
	public void testSingleSubscribeString1() throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException {
		ChordSharpConnection conn = new ChordSharpConnection();

		for (int i = 0; i < testData.length; ++i) {
			conn.singleSubscribe(
					testTime + "_SingleSubscribeString1_" + i,
					testData[i] );
		}
		
		// check if the subscribers were successfully saved:
		
		for (int i = 0; i < testData.length; ++i) {
			String topic = "_SingleSubscribeString1_" + i;
			Vector<String> subscribers = conn.singleGetSubscribers(testTime
					+ topic);
			assertTrue("Subscriber \"" + testData[i]
					+ "\" does not exist for \"topic " + topic + "\"", checkSubscribers(
					subscribers, testData[i]));
			
			assertEquals("Subscribers of topic (" + topic
					+ ") should only be [" + testData[i] + "], but is: "
					+ subscribers.toString(), 1, subscribers.size());
		}
	}
	
	/**
	 * Test method for
	 * {@link ChordSharpConnection#singleSubscribe(String, String)} and
	 * {@link ChordSharpConnection#singleGetSubscribers(String)}.
	 * Subscribes some "random" URLs to "random" topics and uses a single topic for all URLs.
	 * 
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 * @throws NotFoundException 
	 */
	@Test
	public void testSingleSubscribeString2() throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException {
		ChordSharpConnection conn = new ChordSharpConnection();

		for (int i = 0; i < testData.length; ++i) {
			conn.singleSubscribe(
					testTime + "_SingleSubscribeString2",
					testData[i] );
		}
		
		// check if the subscribers were successfully saved:
		
		String topic = "_SingleSubscribeString2";
		Vector<String> subscribers = conn
				.singleGetSubscribers(testTime + topic);
		for (int i = 0; i < testData.length; ++i) {
			assertTrue("Subscriber " + testData[i]
					+ " does not exist for topic " + topic, checkSubscribers(
					subscribers, testData[i]));
		}
		
		if (subscribers.size() > testData.length) {
			fail("\"" + getDiffElement(subscribers, testData) + "\" should not be a subscriber of " + topic);
		}
	}
	
	/**
	 * Test method for
	 * {@link ChordSharpConnection#subscribe(OtpErlangString, OtpErlangString)} and
	 * {@link ChordSharpConnection#getSubscribers(OtpErlangString)}.
	 * Subscribes some "random" URLs to "random" topics and uses a distinct topic for each URL.
	 * 
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 * @throws NotFoundException 
	 */
	@Test
	public void testSubscribeOtpString1() throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException {
		for (int i = 0; i < testData.length; ++i) {
			ChordSharpConnection.subscribe(
					new OtpErlangString(testTime + "_SubscribeOtpString1_" + i),
					new OtpErlangString(testData[i]) );
		}
		
		// check if the subscribers were successfully saved:
		
		for (int i = 0; i < testData.length; ++i) {
			String topic = "_SubscribeOtpString1_" + i;
			OtpErlangList subscribers = ChordSharpConnection
					.getSubscribers(new OtpErlangString(testTime + topic));
			assertTrue("Subscriber \"" + testData[i]
					+ "\" does not exist for topic \"" + topic + "\"", checkSubscribers(
					subscribers, testData[i]));
			
			assertEquals("Subscribers of topic (" + topic
					+ ") should only be [\"" + testData[i] + "\"], but is: "
					+ subscribers.toString(), 1, subscribers.arity());
		}
	}
	
	/**
	 * Test method for
	 * {@link ChordSharpConnection#subscribe(OtpErlangString, OtpErlangString)} and
	 * {@link ChordSharpConnection#getSubscribers(OtpErlangString)}.
	 * Subscribes some "random" URLs to "random" topics and uses a single topic for all URLs.
	 * 
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 * @throws NotFoundException 
	 */
	@Test
	public void testSubscribeOtpString2() throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException {
		for (int i = 0; i < testData.length; ++i) {
			ChordSharpConnection.subscribe(
					new OtpErlangString(testTime + "_SubscribeOtpString2"),
					new OtpErlangString(testData[i]) );
		}
		
		// check if the subscribers were successfully saved:
		
		String topic = "_SubscribeOtpString2";
		OtpErlangList subscribers = ChordSharpConnection
				.getSubscribers(new OtpErlangString(testTime + topic));
		for (int i = 0; i < testData.length; ++i) {
			assertTrue("Subscriber \"" + testData[i]
					+ "\" does not exist for topic \"" + topic + "\"", checkSubscribers(
					subscribers, testData[i]));
		}
		
		if (subscribers.arity() > testData.length) {
			fail("\"" + getDiffElement(subscribers, testData) + "\" should not be a subscriber of " + topic);
		}
	}

	/**
	 * Test method for
	 * {@link ChordSharpConnection#subscribe(String, String)} and
	 * {@link ChordSharpConnection#getSubscribers(String)}.
	 * Subscribes some "random" URLs to "random" topics and uses a distinct topic for each URL.
	 * 
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 * @throws NotFoundException 
	 */
	@Test
	public void testSubscribeString1() throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException {
		for (int i = 0; i < testData.length; ++i) {
			ChordSharpConnection.subscribe(
					testTime + "_SubscribeString1_" + i,
					testData[i] );
		}
		
		// check if the subscribers were successfully saved:
		
		for (int i = 0; i < testData.length; ++i) {
			String topic = "_SubscribeString1_" + i;
			Vector<String> subscribers = ChordSharpConnection
					.getSubscribers(testTime + topic);
			assertTrue("Subscriber \"" + testData[i]
					+ "\" does not exist for topic \"" + topic + "\"", checkSubscribers(
					subscribers, testData[i]));
			
			assertEquals("Subscribers of topic (" + topic
					+ ") should only be [" + testData[i] + "], but is: "
					+ subscribers.toString(), 1, subscribers.size());
		}
	}
	
	/**
	 * Test method for
	 * {@link ChordSharpConnection#subscribe(String, String)} and
	 * {@link ChordSharpConnection#getSubscribers(String)}.
	 * Subscribes some "random" URLs to "random" topics and uses a single topic for all URLs.
	 * 
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 * @throws NotFoundException 
	 */
	@Test
	public void testSubscribeString2() throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException {
		for (int i = 0; i < testData.length; ++i) {
			ChordSharpConnection.subscribe(
					testTime + "_SubscribeString2",
					testData[i] );
		}
		
		// check if the subscribers were successfully saved:
		
		String topic = "_SubscribeString2";
		Vector<String> subscribers = ChordSharpConnection
				.getSubscribers(testTime + topic);
		for (int i = 0; i < testData.length; ++i) {
			assertTrue("Subscriber " + testData[i]
					+ " does not exist for topic " + topic, checkSubscribers(
					subscribers, testData[i]));
		}
		
		if (subscribers.size() > testData.length) {
			fail("\"" + getDiffElement(subscribers, testData) + "\" should not be a subscriber of " + topic);
		}
	}
	
	// subscribe() test methods end
	// unsubscribe() test methods begin
	
	/**
	 * Test method for
	 * {@link ChordSharpConnection#singleUnsubscribe(OtpErlangString, OtpErlangString)} and
	 * {@link ChordSharpConnection#singleGetSubscribers(OtpErlangString)}.
	 * Tries to unsubscribe an URL from a non-existing topic and tries to get
	 * the subscriber list afterwards.
	 * 
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 * @throws NotFoundException 
	 */
	@Test(expected=NotFoundException.class)
	public void testSingleUnsubscribeOtpString_NotExistingTopic() throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException {
		String topic = "_SingleUnsubscribeOtpString_NotExistingTopic";
		ChordSharpConnection conn = new ChordSharpConnection();
		
		// unsubscribe test "url":
		conn.singleUnsubscribe(
				new OtpErlangString(testTime + topic),
				new OtpErlangString(testData[0]) );
		
		// check whether the unsubscribed urls were unsubscribed:
		OtpErlangList subscribers = conn
				.singleGetSubscribers(new OtpErlangString(testTime + topic));
		assertFalse("Subscriber \"" + testData[0]
				+ "\" should have been unsubscribed from topic \"" + topic
				+ "\"", checkSubscribers(subscribers, testData[0]));
		
		assertEquals("Subscribers of topic (" + topic
				+ ") should only be [\"\"], but is: "
				+ subscribers.toString(), 0, subscribers.arity());
	}
	
	/**
	 * Test method for
	 * {@link ChordSharpConnection#singleUnsubscribe(OtpErlangString, OtpErlangString)} and
	 * {@link ChordSharpConnection#singleGetSubscribers(OtpErlangString)}.
	 * Tries to unsubscribe an unsubscribed URL from an existing topic and compares
	 * the subscriber list afterwards.
	 * 
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 * @throws NotFoundException 
	 */
	@Test(expected=NotFoundException.class)
	public void testSingleUnsubscribeOtpString_NotExistingUrl()
			throws ConnectionException, TimeoutException, UnknownException,
			NotFoundException {
		String topic = "_SingleUnsubscribeOtpString_NotExistingUrl";
		ChordSharpConnection conn = new ChordSharpConnection();

		// first subscribe test "urls"...
		conn.singleSubscribe(new OtpErlangString(testTime + topic),
				new OtpErlangString(testData[0]));
		conn.singleSubscribe(new OtpErlangString(testTime + topic),
				new OtpErlangString(testData[1]));

		// then unsubscribe another "url":
		conn.singleUnsubscribe(new OtpErlangString(testTime + topic),
				new OtpErlangString(testData[2]));

		OtpErlangList subscribers = conn
				.singleGetSubscribers(new OtpErlangString(testTime + topic));

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
	}
	
	/**
	 * Test method for
	 * {@link ChordSharpConnection#singleSubscribe(OtpErlangString, OtpErlangString)},
	 * {@link ChordSharpConnection#singleUnsubscribe(OtpErlangString, OtpErlangString)} and
	 * {@link ChordSharpConnection#singleGetSubscribers(OtpErlangString)}.
	 * Subscribes some "random" URLs to "random" topics and uses a distinct topic for each URL.
	 * Unsubscribes every second subscribed URL.
	 * 
	 * @see #testSingleSubscribeOtpString1()
	 * 
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 * @throws NotFoundException 
	 */
	@Test
	public void testSingleUnsubscribeOtpString1() throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException {
		ChordSharpConnection conn = new ChordSharpConnection();

		// first subscribe all test "urls"...
		for (int i = 0; i < testData.length; ++i) {
			conn.singleSubscribe(
					new OtpErlangString(testTime + "_SingleUnsubscribeOtpString1_" + i),
					new OtpErlangString(testData[i]) );
		}
		// ... then unsubscribe every second url:
		for (int i = 0; i < testData.length; i += 2) {
			conn.singleUnsubscribe(
					new OtpErlangString(testTime + "_SingleUnsubscribeOtpString1_" + i),
					new OtpErlangString(testData[i]) );
		}
		
		// check whether the subscribers were successfully saved:
		for (int i = 1; i < testData.length; i += 2) {
			String topic = "_SingleSubscribeOtpString1_" + i;
			OtpErlangList subscribers = conn
					.singleGetSubscribers(new OtpErlangString(testTime + topic));
			assertTrue("Subscriber \"" + testData[i]
					+ "\" does not exist for topic \"" + topic + "\"",
					checkSubscribers(subscribers, testData[i]));
			
			assertEquals("Subscribers of topic (" + topic
					+ ") should only be [\"" + testData[i] + "\"], but is: "
					+ subscribers.toString(), 1, subscribers.arity());
		}
		// check whether the unsubscribed urls were unsubscribed:
		for (int i = 0; i < testData.length; i += 2) {
			String topic = "_SingleUnsubscribeOtpString1_" + i;
			OtpErlangList subscribers = conn
					.singleGetSubscribers(new OtpErlangString(testTime + topic));
			assertFalse("Subscriber \"" + testData[i]
					+ "\" should have been unsubscribed from topic \"" + topic
					+ "\"", checkSubscribers(subscribers, testData[i]));
			
			assertEquals("Subscribers of topic (" + topic
					+ ") should only be [\"\"], but is: "
					+ subscribers.toString(), 0, subscribers.arity());
		}
	}
	
	/**
	 * Test method for
	 * {@link ChordSharpConnection#singleSubscribe(OtpErlangString, OtpErlangString)},
	 * {@link ChordSharpConnection#singleUnsubscribe(OtpErlangString, OtpErlangString)} and
	 * {@link ChordSharpConnection#singleGetSubscribers(OtpErlangString)}.
	 * Subscribes some "random" URLs to "random" topics and uses a single topic for all URLs.
	 * Unsubscribes every second subscribed URL.
	 * 
	 * @see #testSingleSubscribeOtpString2()
	 * 
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 * @throws NotFoundException 
	 */
	@Test
	public void testSingleUnsubscribeOtpString2() throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException {
		ChordSharpConnection conn = new ChordSharpConnection();

		// first subscribe all test "urls"...
		for (int i = 0; i < testData.length; ++i) {
			conn.singleSubscribe(
					new OtpErlangString(testTime + "_SingleUnubscribeOtpString2"),
					new OtpErlangString(testData[i]) );
		}
		// ... then unsubscribe every second url:
		for (int i = 0; i < testData.length; i += 2) {
			conn.singleUnsubscribe(
					new OtpErlangString(testTime + "_SingleUnubscribeOtpString2"),
					new OtpErlangString(testData[i]) );
		}
		
		String topic = "_SingleUnubscribeOtpString2";
		OtpErlangList subscribers = conn
				.singleGetSubscribers(new OtpErlangString(testTime + topic));
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
	}
	
	/**
	 * Test method for
	 * {@link ChordSharpConnection#singleUnsubscribe(String, String)} and
	 * {@link ChordSharpConnection#singleGetSubscribers(String)}.
	 * Tries to unsubscribe an URL from a non-existing topic and tries to get
	 * the subscriber list afterwards.
	 * 
	 * @see #testSingleUnsubscribeOtpString_NotExistingTopic()
	 * 
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 * @throws NotFoundException 
	 */
	@Test(expected=NotFoundException.class)
	public void testSingleUnsubscribeString_NotExistingTopic() throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException {
		String topic = "_SingleUnsubscribeString_NotExistingTopic";
		ChordSharpConnection conn = new ChordSharpConnection();
		
		// unsubscribe test "url":
		conn.singleUnsubscribe(
				testTime + topic,
				testData[0]);
		
		// check whether the unsubscribed urls were unsubscribed:
		Vector<String> subscribers = conn
				.singleGetSubscribers(testTime + topic);
		assertFalse("Subscriber \"" + testData[0]
				+ "\" should have been unsubscribed from topic \"" + topic
				+ "\"", checkSubscribers(subscribers, testData[0]));
		
		assertEquals("Subscribers of topic (" + topic
				+ ") should only be [\"\"], but is: "
				+ subscribers.toString(), 0, subscribers.size());
	}
	
	/**
	 * Test method for
	 * {@link ChordSharpConnection#singleUnsubscribe(String, String)} and
	 * {@link ChordSharpConnection#singleGetSubscribers(String)}.
	 * Tries to unsubscribe an unsubscribed URL from an existing topic and compares
	 * the subscriber list afterwards.
	 * 
	 * @see #testSingleUnsubscribeOtpString_NotExistingUrl()
	 * 
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 * @throws NotFoundException 
	 */
	@Test(expected=NotFoundException.class)
	public void testSingleUnsubscribeString_NotExistingUrl()
			throws ConnectionException, TimeoutException, UnknownException,
			NotFoundException {
		String topic = "_SingleUnsubscribeString_NotExistingUrl";
		ChordSharpConnection conn = new ChordSharpConnection();

		// first subscribe test "urls"...
		conn.singleSubscribe(testTime + topic, testData[0]);
		conn.singleSubscribe(testTime + topic, testData[1]);

		// then unsubscribe another "url":
		conn.singleUnsubscribe(testTime + topic, testData[2]);

		Vector<String> subscribers = conn
				.singleGetSubscribers(testTime + topic);

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
	}
	
	/**
	 * Test method for
	 * {@link ChordSharpConnection#singleSubscribe(String, String)},
	 * {@link ChordSharpConnection#singleUnsubscribe(String, String)} and
	 * {@link ChordSharpConnection#singleGetSubscribers(String)}.
	 * Subscribes some "random" URLs to "random" topics and uses a distinct topic for each URL.
	 * Unsubscribes every second subscribed URL.
	 * 
	 * @see #testSingleSubscribeString1()
	 * 
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 * @throws NotFoundException 
	 */
	@Test
	public void testSingleUnsubscribeString1() throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException {
		ChordSharpConnection conn = new ChordSharpConnection();

		// first subscribe all test "urls"...
		for (int i = 0; i < testData.length; ++i) {
			conn.singleSubscribe(
					testTime + "_SingleUnsubscribeString1_" + i,
					testData[i]);
		}
		// ... then unsubscribe every second url:
		for (int i = 0; i < testData.length; i += 2) {
			conn.singleUnsubscribe(
					testTime + "_SingleUnsubscribeString1_" + i,
					testData[i]);
		}
		
		// check whether the subscribers were successfully saved:
		for (int i = 1; i < testData.length; i += 2) {
			String topic = "_SingleUnsubscribeString1_" + i;
			Vector<String> subscribers = conn.singleGetSubscribers(testTime
					+ topic);
			assertTrue("Subscriber \"" + testData[i]
  					+ "\" does not exist for \"topic " + topic + "\"", checkSubscribers(
  					subscribers, testData[i]));
			
			assertEquals("Subscribers of topic (" + topic
					+ ") should only be [" + testData[i] + "], but is: "
					+ subscribers.toString(), 1, subscribers.size());
		}
		// check whether the unsubscribed urls were unsubscribed:
		for (int i = 0; i < testData.length; i += 2) {
			String topic = "_SingleUnsubscribeString1_" + i;
			Vector<String> subscribers = conn.singleGetSubscribers(testTime
					+ topic);
			assertFalse("Subscriber \"" + testData[i]
					+ "\" should have been unsubscribed from topic \"" + topic + "\"", checkSubscribers(
					subscribers, testData[i]));
			
			assertEquals("Subscribers of topic (" + topic
					+ ") should only be [\"\"], but is: "
					+ subscribers.toString(), 0, subscribers.size());
		}
	}

	/**
	 * Test method for
	 * {@link ChordSharpConnection#singleSubscribe(String, String)},
	 * {@link ChordSharpConnection#singleSubscribe(String, String)} and
	 * {@link ChordSharpConnection#singleGetSubscribers(String)}.
	 * Subscribes some "random" URLs to "random" topics and uses a single topic for all URLs.
	 * Unsubscribes every second subscribed URL.
	 * 
	 * @see #testSingleSubscribeString2()
	 * 
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 * @throws NotFoundException 
	 */
	@Test
	public void testSingleUnsubscribeString2() throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException {
		ChordSharpConnection conn = new ChordSharpConnection();

		// first subscribe all test "urls"...
		for (int i = 0; i < testData.length; ++i) {
			conn.singleSubscribe(
					testTime + "_SingleUnubscribeString2",
					testData[i]);
		}
		// ... then unsubscribe every second url:
		for (int i = 0; i < testData.length; i += 2) {
			conn.singleUnsubscribe(
					testTime + "_SingleUnubscribeString2",
					testData[i]);
		}
		
		String topic = "_SingleUnubscribeString2";
		Vector<String> subscribers = conn
				.singleGetSubscribers(testTime + topic);
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
	}

	/**
	 * Test method for
	 * {@link ChordSharpConnection#unsubscribe(OtpErlangString, OtpErlangString)} and
	 * {@link ChordSharpConnection#getSubscribers(OtpErlangString)}.
	 * Tries to unsubscribe an URL from a non-existing topic and tries to get
	 * the subscriber list afterwards.
	 * 
	 * @see #testSingleUnsubscribeOtpString_NotExistingTopic()
	 * 
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 * @throws NotFoundException 
	 */
	@Test(expected=NotFoundException.class)
	public void testUnsubscribeOtpString_NotExistingTopic() throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException {
		String topic = "_UnsubscribeOtpString_NotExistingTopic";
		
		// unsubscribe test "url":
		ChordSharpConnection.unsubscribe(
				new OtpErlangString(testTime + topic),
				new OtpErlangString(testData[0]) );
		
		// check whether the unsubscribed urls were unsubscribed:
		OtpErlangList subscribers = ChordSharpConnection
				.getSubscribers(new OtpErlangString(testTime + topic));
		assertFalse("Subscriber \"" + testData[0]
				+ "\" should have been unsubscribed from topic \"" + topic
				+ "\"", checkSubscribers(subscribers, testData[0]));
		
		assertEquals("Subscribers of topic (" + topic
				+ ") should only be [\"\"], but is: "
				+ subscribers.toString(), 0, subscribers.arity());
	}
	
	/**
	 * Test method for
	 * {@link ChordSharpConnection#unsubscribe(OtpErlangString, OtpErlangString)} and
	 * {@link ChordSharpConnection#getSubscribers(OtpErlangString)}.
	 * Tries to unsubscribe an unsubscribed URL from an existing topic and compares
	 * the subscriber list afterwards.
	 * 
	 * @see #testSingleUnsubscribeOtpString_NotExistingUrl()
	 * 
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 * @throws NotFoundException 
	 */
	@Test(expected=NotFoundException.class)
	public void testUnsubscribeOtpString_NotExistingUrl()
			throws ConnectionException, TimeoutException, UnknownException,
			NotFoundException {
		String topic = "_UnsubscribeOtpString_NotExistingUrl";

		// first subscribe test "urls"...
		ChordSharpConnection.subscribe(new OtpErlangString(testTime + topic),
				new OtpErlangString(testData[0]));
		ChordSharpConnection.subscribe(new OtpErlangString(testTime + topic),
				new OtpErlangString(testData[1]));

		// then unsubscribe another "url":
		ChordSharpConnection.unsubscribe(new OtpErlangString(testTime + topic),
				new OtpErlangString(testData[2]));

		OtpErlangList subscribers = ChordSharpConnection
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
	}
	
	/**
	 * Test method for
	 * {@link ChordSharpConnection#subscribe(OtpErlangString, OtpErlangString)},
	 * {@link ChordSharpConnection#unsubscribe(OtpErlangString, OtpErlangString)} and
	 * {@link ChordSharpConnection#getSubscribers(OtpErlangString)}.
	 * Subscribes some "random" URLs to "random" topics and uses a distinct topic for each URL.
	 * Unsubscribes every second subscribed URL.
	 * 
	 * @see #testSubscribeOtpString1()
	 * @see #testSingleUnsubscribeOtpString1()
	 * 
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 * @throws NotFoundException 
	 */
	@Test
	public void testUnsubscribeOtpString1() throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException {
		// first subscribe all test "urls"...
		for (int i = 0; i < testData.length; ++i) {
			ChordSharpConnection.subscribe(
					new OtpErlangString(testTime + "_UnsubscribeOtpString1_" + i),
					new OtpErlangString(testData[i]) );
		}
		// ... then unsubscribe every second url:
		for (int i = 0; i < testData.length; i += 2) {
			ChordSharpConnection.unsubscribe(
					new OtpErlangString(testTime + "_UnsubscribeOtpString1_" + i),
					new OtpErlangString(testData[i]) );
		}
		
		// check whether the subscribers were successfully saved:
		for (int i = 1; i < testData.length; i += 2) {
			String topic = "_SingleSubscribeOtpString1_" + i;
			OtpErlangList subscribers = ChordSharpConnection
					.getSubscribers(new OtpErlangString(testTime + topic));
			assertTrue("Subscriber \"" + testData[i]
					+ "\" does not exist for topic \"" + topic + "\"",
					checkSubscribers(subscribers, testData[i]));

			assertEquals("Subscribers of topic (" + topic
					+ ") should only be [\"" + testData[i] + "\"], but is: "
					+ subscribers.toString(), 1, subscribers.arity());
		}
		// check whether the unsubscribed urls were unsubscribed:
		for (int i = 0; i < testData.length; i += 2) {
			String topic = "_SingleUnsubscribeOtpString1_" + i;
			OtpErlangList subscribers = ChordSharpConnection
					.getSubscribers(new OtpErlangString(testTime + topic));
			assertFalse("Subscriber \"" + testData[i]
					+ "\" should have been unsubscribed from topic \"" + topic
					+ "\"", checkSubscribers(subscribers, testData[i]));

			assertEquals("Subscribers of topic (" + topic
					+ ") should only be [\"\"], but is: "
					+ subscribers.toString(), 0, subscribers.arity());
		}
	}

	/**
	 * Test method for
	 * {@link ChordSharpConnection#subscribe(OtpErlangString, OtpErlangString)},
	 * {@link ChordSharpConnection#unsubscribe(OtpErlangString, OtpErlangString)} and
	 * {@link ChordSharpConnection#getSubscribers(OtpErlangString)}.
	 * Subscribes some "random" URLs to "random" topics and uses a single topic for all URLs.
	 * Unsubscribes every second subscribed URL.
	 * 
	 * @see #testSubscribeOtpString2()
	 * @see #testSingleUnsubscribeOtpString2()
	 * 
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 * @throws NotFoundException 
	 */
	@Test
	public void testUnsubscribeOtpString2() throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException {
		// first subscribe all test "urls"...
		for (int i = 0; i < testData.length; ++i) {
			ChordSharpConnection.subscribe(
					new OtpErlangString(testTime + "_UnubscribeOtpString2"),
					new OtpErlangString(testData[i]) );
		}
		// ... then unsubscribe every second url:
		for (int i = 0; i < testData.length; i += 2) {
			ChordSharpConnection.unsubscribe(
					new OtpErlangString(testTime + "_UnubscribeOtpString2"),
					new OtpErlangString(testData[i]) );
		}
		
		String topic = "_UnubscribeOtpString2";
		OtpErlangList subscribers = ChordSharpConnection
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
	}

	/**
	 * Test method for
	 * {@link ChordSharpConnection#unsubscribe(String, String)} and
	 * {@link ChordSharpConnection#getSubscribers(String)}.
	 * Tries to unsubscribe an URL from a non-existing topic and tries to get
	 * the subscriber list afterwards.
	 * 
	 * @see #testSingleUnsubscribeOtpString_NotExistingTopic()
	 * 
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 * @throws NotFoundException 
	 */
	@Test(expected=NotFoundException.class)
	public void testUnsubscribeString_NotExistingTopic() throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException {
		String topic = "_UnsubscribeString_NotExistingTopic";
		
		// unsubscribe test "url":
		ChordSharpConnection.unsubscribe(
				testTime + topic,
				testData[0]);
		
		// check whether the unsubscribed urls were unsubscribed:
		Vector<String> subscribers = ChordSharpConnection
				.getSubscribers(testTime + topic);
		assertFalse("Subscriber \"" + testData[0]
				+ "\" should have been unsubscribed from topic \"" + topic
				+ "\"", checkSubscribers(subscribers, testData[0]));
		
		assertEquals("Subscribers of topic (" + topic
				+ ") should only be [\"\"], but is: "
				+ subscribers.toString(), 0, subscribers.size());
	}
	
	/**
	 * Test method for
	 * {@link ChordSharpConnection#unsubscribe(String, String)} and
	 * {@link ChordSharpConnection#getSubscribers(String)}.
	 * Tries to unsubscribe an unsubscribed URL from an existing topic and compares
	 * the subscriber list afterwards.
	 * 
	 * @see #testSingleUnsubscribeOtpString_NotExistingUrl()
	 * 
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 * @throws NotFoundException 
	 */
	@Test(expected=NotFoundException.class)
	public void testUnsubscribeString_NotExistingUrl()
			throws ConnectionException, TimeoutException, UnknownException,
			NotFoundException {
		String topic = "_UnsubscribeString_NotExistingUrl";

		// first subscribe test "urls"...
		ChordSharpConnection.subscribe(testTime + topic, testData[0]);
		ChordSharpConnection.subscribe(testTime + topic, testData[1]);

		// then unsubscribe another "url":
		ChordSharpConnection.unsubscribe(testTime + topic, testData[2]);

		Vector<String> subscribers = ChordSharpConnection
				.getSubscribers(testTime + topic);

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
	}
	
	/**
	 * Test method for
	 * {@link ChordSharpConnection#subscribe(String, String)},
	 * {@link ChordSharpConnection#unsubscribe(String, String)} and
	 * {@link ChordSharpConnection#getSubscribers(String)}.
	 * Subscribes some "random" URLs to "random" topics and uses a distinct topic for each URL.
	 * Unsubscribes every second subscribed URL.
	 * 
	 * @see #testSubscribeString1()
	 * @see #testSingleUnsubscribeString1()
	 * 
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 * @throws NotFoundException 
	 */
	@Test
	public void testUnsubscribeString1() throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException {
		// first subscribe all test "urls"...
		for (int i = 0; i < testData.length; ++i) {
			ChordSharpConnection.subscribe(
					testTime + "_UnsubscribeString1_" + i,
					testData[i]);
		}
		// ... then unsubscribe every second url:
		for (int i = 0; i < testData.length; i += 2) {
			ChordSharpConnection.unsubscribe(
					new OtpErlangString(testTime + "_UnsubscribeString1_" + i),
					new OtpErlangString(testData[i]) );
		}
		
		// check whether the subscribers were successfully saved:
		for (int i = 1; i < testData.length; i += 2) {
			String topic = "_UnsubscribeString1_" + i;
			Vector<String> subscribers = ChordSharpConnection
					.getSubscribers(testTime + topic);
			assertTrue("Subscriber \"" + testData[i]
					+ "\" does not exist for topic \"" + topic + "\"",
					checkSubscribers(subscribers, testData[i]));
			
			assertEquals("Subscribers of topic (" + topic
					+ ") should only be [\"" + testData[i] + "\"], but is: "
					+ subscribers.toString(), 1, subscribers.size());
		}
		// check whether the unsubscribed urls were unsubscribed:
		for (int i = 0; i < testData.length; i += 2) {
			String topic = "_SingleUnsubscribeOtpString1_" + i;
			Vector<String> subscribers = ChordSharpConnection
					.getSubscribers(testTime + topic);
			assertFalse("Subscriber \"" + testData[i]
					+ "\" should have been unsubscribed from topic \"" + topic
					+ "\"", checkSubscribers(subscribers, testData[i]));
			
			assertEquals("Subscribers of topic (" + topic
					+ ") should only be [\"\"], but is: "
					+ subscribers.toString(), 0, subscribers.size());
		}
	}

	/**
	 * Test method for
	 * {@link ChordSharpConnection#subscribe(String, String)},
	 * {@link ChordSharpConnection#unsubscribe(String, String)} and
	 * {@link ChordSharpConnection#getSubscribers(String)}.
	 * Subscribes some "random" URLs to "random" topics and uses a single topic for all URLs.
	 * Unsubscribes every second subscribed URL.
	 * 
	 * @see #testSubscribeString2()
	 * @see #testSingleUnsubscribeString2()
	 * 
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 * @throws NotFoundException 
	 */
	@Test
	public void testUnsubscribeString2() throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException {
		// first subscribe all test "urls"...
		for (int i = 0; i < testData.length; ++i) {
			ChordSharpConnection.subscribe(
					testTime + "_UnubscribeString2",
					testData[i]);
		}
		// ... then unsubscribe every second url:
		for (int i = 0; i < testData.length; i += 2) {
			ChordSharpConnection.unsubscribe(
					testTime + "_UnubscribeString2",
					testData[i]);
		}
		
		String topic = "_UnubscribeString2";
		Vector<String> subscribers = ChordSharpConnection
				.getSubscribers(testTime + topic);
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
		notifications_server1 = new HashMap<String, Vector<String>>();
		String topic = testTime + "_Subscription1";
		
		Server server1 = new Server(8080);
		server1.setHandler(new SubscriptionHandler(notifications_server1));
		server1.start();
		
		ChordSharpConnection.subscribe(topic, "http://127.0.0.1:8080");
		
		for (int i = 0; i < testData.length; ++i) {
			ChordSharpConnection.publish(topic, testData[i]);
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
		
		server1.stop();
	}
	
	/**
	 * Test method for the publish/subscribe system.
	 * Three servers, subscription to one topic, multiple publishs.
	 * 
	 * @throws Exception 
	 */
	@Test
	public void testSubscription2() throws Exception {
		notifications_server1 = new HashMap<String, Vector<String>>();
		notifications_server2 = new HashMap<String, Vector<String>>();
		notifications_server3 = new HashMap<String, Vector<String>>();
		String topic = testTime + "_Subscription2";
		
		Server server1 = new Server(8081);
		server1.setHandler(new SubscriptionHandler(notifications_server1));
		server1.start();
		
		Server server2 = new Server(8082);
		server2.setHandler(new SubscriptionHandler(notifications_server2));
		server2.start();
		
		Server server3 = new Server(8083);
		server3.setHandler(new SubscriptionHandler(notifications_server3));
		server3.start();
		
		ChordSharpConnection.subscribe(topic, "http://127.0.0.1:8081");
		ChordSharpConnection.subscribe(topic, "http://127.0.0.1:8082");
		ChordSharpConnection.subscribe(topic, "http://127.0.0.1:8083");
		
		for (int i = 0; i < testData.length; ++i) {
			ChordSharpConnection.publish(topic, testData[i]);
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
		
		server1.stop();
		server2.stop();
		server3.stop();
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
		notifications_server1 = new HashMap<String, Vector<String>>();
		notifications_server2 = new HashMap<String, Vector<String>>();
		notifications_server3 = new HashMap<String, Vector<String>>();
		String topic1 = testTime + "_Subscription3_1";
		String topic2 = testTime + "_Subscription3_2";
		String topic3 = testTime + "_Subscription3_3";
		
		Server server1 = new Server(8084);
		server1.setHandler(new SubscriptionHandler(notifications_server1));
		server1.start();
		
		Server server2 = new Server(8085);
		server2.setHandler(new SubscriptionHandler(notifications_server2));
		server2.start();
		
		Server server3 = new Server(8086);
		server3.setHandler(new SubscriptionHandler(notifications_server3));
		server3.start();
		
		ChordSharpConnection.subscribe(topic1, "http://127.0.0.1:8084");
		ChordSharpConnection.subscribe(topic2, "http://127.0.0.1:8085");
		ChordSharpConnection.subscribe(topic3, "http://127.0.0.1:8086");
		
		int topic1_elements = 0;
		int topic3_elements = 0;
		int topic2_elements = 0;
		for (int i = 0; i < testData.length; ++i) {
			if (i % 2 == 0) {
				ChordSharpConnection.publish(topic1, testData[i]);
				++topic1_elements;
			}
			if (i % 3 == 0) {
				ChordSharpConnection.publish(topic2, testData[i]);
				++topic2_elements;
			}
			if (i % 5 == 0) {
				ChordSharpConnection.publish(topic3, testData[i]);
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
		
		server1.stop();
		server2.stop();
		server3.stop();
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
		notifications_server1 = new HashMap<String, Vector<String>>();
		notifications_server2 = new HashMap<String, Vector<String>>();
		notifications_server3 = new HashMap<String, Vector<String>>();
		String topic1 = testTime + "_Subscription4_1";
		String topic2 = testTime + "_Subscription4_2";
		String topic3 = testTime + "_Subscription4_3";
		
		Server server1 = new Server(8087);
		server1.setHandler(new SubscriptionHandler(notifications_server1));
		server1.start();
		
		Server server2 = new Server(8088);
		server2.setHandler(new SubscriptionHandler(notifications_server2));
		server2.start();
		
		Server server3 = new Server(8089);
		server3.setHandler(new SubscriptionHandler(notifications_server3));
		server3.start();
		
		ChordSharpConnection.subscribe(topic1, "http://127.0.0.1:8087");
		ChordSharpConnection.subscribe(topic2, "http://127.0.0.1:8088");
		ChordSharpConnection.subscribe(topic3, "http://127.0.0.1:8089");
		ChordSharpConnection.unsubscribe(topic2, "http://127.0.0.1:8088");
		
		int topic1_elements = 0;
		int topic3_elements = 0;
		int topic2_elements = 0;
		for (int i = 0; i < testData.length; ++i) {
			if (i % 2 == 0) {
				ChordSharpConnection.publish(topic1, testData[i]);
				++topic1_elements;
			}
			if (i % 3 == 0) {
				ChordSharpConnection.publish(topic2, testData[i]);
				++topic2_elements;
			}
			if (i % 5 == 0) {
				ChordSharpConnection.publish(topic3, testData[i]);
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
		
		server1.stop();
		server2.stop();
		server3.stop();
	}

	// TODO: add another subscription test with unsubscribed topics
	
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
			if (json instanceof Map) {
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
