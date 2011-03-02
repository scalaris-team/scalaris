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

import org.junit.Test;

/**
 * Unit test for the {@link ReplicatedDHT} class.
 * 
 * @author Nico Kruber, kruber@zib.de
 * @version 2.6
 * @since 2.6
 */
public class ReplicatedDHTTest {
	private final static long testTime = System.currentTimeMillis();
	
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
	
	static {
		// set not to automatically try reconnects (auto-retries prevent ConnectionException tests from working): 
		((DefaultConnectionPolicy) ConnectionFactory.getInstance().getConnectionPolicy()).setMaxRetries(0);
	}

	/**
	 * Test method for
	 * {@link ReplicatedDHT#ReplicatedDHT()}.
	 * @throws ConnectionException 
	 */
	@Test
	public void testReplicatedDHT1() throws ConnectionException {
		ReplicatedDHT conn = new ReplicatedDHT();
		conn.closeConnection();
	}
	
	/**
	 * Test method for
	 * {@link ReplicatedDHT#ReplicatedDHT(Connection)}.
	 * @throws ConnectionException 
	 */
	@Test
	public void testReplicatedDHT2() throws ConnectionException {
		ReplicatedDHT conn = new ReplicatedDHT(ConnectionFactory.getInstance().createConnection("test"));
		conn.closeConnection();
	}
	
	/**
	 * Test method for {@link ReplicatedDHT#delete(String)}.
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
		ReplicatedDHT conn = new ReplicatedDHT();

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
	 * Test method for {@link ReplicatedDHT#delete(String)} and
	 * {@link Scalaris#write(String, String)}.
	 * Inserts some values, tries to delete them afterwards and tries the
	 * delete again.
	 * 
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 * @throws NodeNotFoundException
	 * @throws AbortException 
	 */
	@Test
	public void testDelete1() throws ConnectionException,
			TimeoutException, UnknownException, NodeNotFoundException, AbortException {
		String key = "_Delete1";
		Connection c = ConnectionFactory.getInstance().createConnection("test");
		ReplicatedDHT rdht = new ReplicatedDHT(c);
        Scalaris sc = new Scalaris(c);

		try {
			for (int i = 0; i < testData.length; ++i) {
				sc.write(testTime + key + i, testData[i]);
			}
			
			// now try to delete the data:
			for (int i = 0; i < testData.length; ++i) {
				long deleted = rdht.delete(testTime + key + i);
				DeleteResult result = rdht.getLastDeleteResult();
				assertEquals(4, deleted);
				assertEquals(4, result.ok);
				assertEquals(0, result.locks_set);
				assertEquals(0, result.undef);
				
				// try again (should be successful with 0 deletes)
				deleted = rdht.delete(testTime + key + i);
				result = rdht.getLastDeleteResult();
				assertEquals(0, deleted);
				assertEquals(0, result.ok);
				assertEquals(0, result.locks_set);
				assertEquals(4, result.undef);
			}
		} finally {
		    c.close();
		}
	}
	
	/**
	 * Test method for {@link ReplicatedDHT#delete(String)} and
	 * {@link Scalaris#write(String, String)}.
	 * Inserts some values, tries to delete them afterwards, inserts them again
	 * and tries to delete them again (twice).
	 * 
	 * @throws UnknownException
	 * @throws TimeoutException
	 * @throws ConnectionException
	 * @throws NodeNotFoundException
	 * @throws AbortException 
	 */
	@Test
	public void testDelete2() throws ConnectionException,
			TimeoutException, UnknownException, NodeNotFoundException, AbortException {
		String key = "_Delete2";
        Connection c = ConnectionFactory.getInstance().createConnection("test");
        ReplicatedDHT rdht = new ReplicatedDHT(c);
        Scalaris sc = new Scalaris(c);

		try {
			for (int i = 0; i < testData.length; ++i) {
				sc.write(testTime + key + i, testData[i]);
			}
			
			// now try to delete the data:
			for (int i = 0; i < testData.length; ++i) {
				long deleted = rdht.delete(testTime + key + i);
				DeleteResult result = rdht.getLastDeleteResult();
				assertEquals(4, deleted);
				assertEquals(4, result.ok);
				assertEquals(0, result.locks_set);
				assertEquals(0, result.undef);
			}
			
			for (int i = 0; i < testData.length; ++i) {
				sc.write(testTime + key + i, testData[i]);
			}
			
			// now try to delete the data:
			for (int i = 0; i < testData.length; ++i) {
				long deleted = rdht.delete(testTime + key + i);
				DeleteResult result = rdht.getLastDeleteResult();
				assertEquals(4, deleted);
				assertEquals(4, result.ok);
				assertEquals(0, result.locks_set);
				assertEquals(0, result.undef);
				
				// try again (should be successful with 0 deletes)
				deleted = rdht.delete(testTime + key + i);
				result = rdht.getLastDeleteResult();
				assertEquals(0, deleted);
				assertEquals(0, result.ok);
				assertEquals(0, result.locks_set);
				assertEquals(4, result.undef);
			}
		} finally {
            c.close();
		}
	}
}
