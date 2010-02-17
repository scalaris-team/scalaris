/**
 *  Copyright 2007-2010 Konrad-Zuse-Zentrum für Informationstechnik Berlin
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

import java.util.Date;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.ericsson.otp.erlang.OtpPeer;

/**
 * @author Nico Kruber, kruber@zib.de
 *
 */
public class PeerNodeTest {

	/**
	 * @throws java.lang.Exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
	}

	/**
	 * Test method for {@link de.zib.scalaris.PeerNode#PeerNode(com.ericsson.otp.erlang.OtpPeer)}.
	 */
	@Test
	public final void testPeerNodeOtpPeer() {
		PeerNode p = new PeerNode(new OtpPeer("test@localhost"));
		assertEquals("test@localhost", p.getNode().node());
	}

	/**
	 * Test method for {@link de.zib.scalaris.PeerNode#PeerNode(java.lang.String)}.
	 */
	@Test
	public final void testPeerNodeString() {
		PeerNode p = new PeerNode("test@localhost");
		assertEquals("test@localhost", p.getNode().node());
	}

	/**
	 * Test method for {@link de.zib.scalaris.PeerNode#getLastFailedConnect()}.
	 */
	@Test
	public final void testGetLastFailedConnect() {
		PeerNode p = new PeerNode("test");
		p.setLastFailedConnect();
		Date d1 = p.getLastFailedConnect();
		p.setLastFailedConnect();
		Date d2 = p.getLastFailedConnect();
		
		assertEquals(2, p.getFailureCount());
		assertTrue(d1.getTime() < d2.getTime());
	}

}
