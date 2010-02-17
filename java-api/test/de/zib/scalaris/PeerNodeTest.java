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
import java.util.List;

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
	 * Test method for {@link de.zib.scalaris.PeerNode#getFailedConnections()}.
	 */
	@Test
	public final void testGetFailedConnections() {
		PeerNode p = new PeerNode("test");
		p.addFailedConnection(new Date(1000));
		p.addFailedConnection(new Date(2000));
		p.addFailedConnection(new Date(3000));
		p.addFailedConnection(new Date(4000));
		p.addFailedConnection(new Date(5000));
		p.addFailedConnection(new Date(6000));
		p.addFailedConnection(new Date(7000));
		p.addFailedConnection(new Date(8000));
		p.addFailedConnection(new Date(9000));
		p.addFailedConnection(new Date(10000));
		p.addFailedConnection(new Date(11000));
		p.addFailedConnection(new Date(12000));
		p.addFailedConnection(new Date(13000));
		p.addFailedConnection(new Date(14000));
		p.addFailedConnection(new Date(15000));
		
		List<Date> failedConns = p.getFailedConnections();
//		System.out.println(failedConns);
		assertEquals(10, failedConns.size());
		assertEquals(10, p.getFailedConnectionsCount());
		assertArrayEquals(new Date[] { new Date(6000), new Date(7000),
				new Date(8000), new Date(9000), new Date(10000),
				new Date(11000), new Date(12000), new Date(13000),
				new Date(14000), new Date(15000) }, failedConns.toArray());
	}
	
	/**
	 * Test method for {@link de.zib.scalaris.PeerNode#getLastFailedConnection()}.
	 */
	@Test
	public final void testGetLastFailedConnection() {
		PeerNode p = new PeerNode("test");
		assertEquals(null, p.getLastFailedConnection());
		p.addFailedConnection(new Date(1000));
		assertEquals(new Date(1000), p.getLastFailedConnection());
		p.addFailedConnection(new Date(2000));
		assertEquals(new Date(2000), p.getLastFailedConnection());
		p.addFailedConnection(new Date(3000));
		assertEquals(new Date(3000), p.getLastFailedConnection());
		p.addFailedConnection(new Date(500));
		assertEquals(new Date(3000), p.getLastFailedConnection());
	}

}
