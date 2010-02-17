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

import java.io.IOException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.ericsson.otp.erlang.OtpAuthException;
import com.ericsson.otp.erlang.OtpErlangExit;
import com.ericsson.otp.erlang.OtpErlangInt;
import com.ericsson.otp.erlang.OtpErlangList;
import com.ericsson.otp.erlang.OtpErlangLong;
import com.ericsson.otp.erlang.OtpErlangObject;
import com.ericsson.otp.erlang.OtpErlangRangeException;
import com.ericsson.otp.erlang.OtpSelf;

/**
 * @author Nico Kruber, kruber@zib.de
 * 
 */
public class ConnectionTest {

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
	 * Test method for
	 * {@link de.zib.scalaris.Connection#Connection(OtpSelf, PeerNode)}.
	 * 
	 * @throws ConnectionException
	 *             if the connection fails
	 * @throws IOException
	 *             if the connection is not active or a communication error
	 *             occurs
	 * @throws OtpErlangExit
	 *             if an exit signal is received from a process on the peer node
	 * @throws OtpAuthException
	 *             if the remote node sends a message containing an invalid
	 *             cookie
	 */
	@Test
	public final void testConnection() throws ConnectionException,
			OtpErlangExit, OtpAuthException, IOException {
		OtpSelf self = new OtpSelf("testConnection", ConnectionFactory
				.getInstance().getCookie());
		PeerNode other = new PeerNode(ConnectionFactory.getInstance()
				.getNodes().get(0).getNode().node());
		Connection c = new Connection(self, other);
		c.close();
	}

	/**
	 * Test method for
	 * {@link de.zib.scalaris.Connection#Connection(OtpSelf, PeerNode)}.
	 * 
	 * Tries several kinds of connections that fail and avaluates the statistics
	 * in the {@link PeerNode} object.
	 * 
	 * @throws IOException
	 *             if the connection is not active or a communication error
	 *             occurs
	 */
	@Test
	public final void testFailedConnection() throws IOException {
		OtpSelf self;
		PeerNode remote;
		Connection c;

		// wrong cookie:
		self = new OtpSelf("testFailedConnection",
				ConnectionFactory.getInstance().getCookie() + "someWrongCookieValue");
		remote = new PeerNode(ConnectionFactory.getInstance().getNodes().get(0).getNode().node());
		try {
			c = new Connection(self, remote);
			c.close();
		} catch (Exception e) {
		}
		assertEquals(1, remote.getFailedConnectionsCount());
		try {
			c = new Connection(self, remote);
			c.close();
		} catch (Exception e) {
		}
		assertEquals(2, remote.getFailedConnectionsCount());

		// unknown host name:
		self = new OtpSelf("testFailedConnection",
				ConnectionFactory.getInstance().getCookie());
		remote = new PeerNode(ConnectionFactory.getInstance().getNodes().get(0).getNode().node() + "noneExistingHost");
		try {
			c = new Connection(self, remote);
			c.close();
		} catch (Exception e) {
		}
		assertEquals(1, remote.getFailedConnectionsCount());
		try {
			c = new Connection(self, remote);
			c.close();
		} catch (Exception e) {
		}
		assertEquals(2, remote.getFailedConnectionsCount());

		// non-existing node name:
		self = new OtpSelf("testFailedConnection",
				ConnectionFactory.getInstance().getCookie());
		remote = new PeerNode("noneExistingNode" + ConnectionFactory.getInstance().getNodes().get(0).getNode().node());
		try {
			c = new Connection(self, remote);
			c.close();
		} catch (Exception e) {
		}
		assertEquals(1, remote.getFailedConnectionsCount());
		try {
			c = new Connection(self, remote);
			c.close();
		} catch (Exception e) {
		}
		assertEquals(2, remote.getFailedConnectionsCount());
	}

	/**
	 * Test method for
	 * {@link de.zib.scalaris.Connection#doRPC(String, String, OtpErlangList)}.
	 * 
	 * @throws ConnectionException
	 *             if the connection fails
	 * @throws IOException
	 *             if the connection is not active or a communication error
	 *             occurs
	 * @throws OtpErlangExit
	 *             if an exit signal is received from a process on the peer node
	 * @throws OtpAuthException
	 *             if the remote node sends a message containing an invalid
	 *             cookie
	 * @throws OtpErlangRangeException
	 *             if the value is too large to be represented as an int
	 */
	@Test
	public final void testDoRPCStringStringOtpErlangList()
			throws ConnectionException, OtpErlangExit, OtpAuthException,
			IOException, OtpErlangRangeException {
		OtpSelf self = new OtpSelf("testDoRPCStringStringOtpErlangList",
				ConnectionFactory.getInstance().getCookie());
		PeerNode remote = new PeerNode(ConnectionFactory.getInstance()
				.getNodes().get(0).getNode().node());
		Connection c = new Connection(self, remote);

		OtpErlangObject raw_result = c.doRPC("lists", "sum", new OtpErlangList(
				new OtpErlangList(new OtpErlangObject[] { new OtpErlangInt(1),
						new OtpErlangInt(2), new OtpErlangInt(3) })));
		OtpErlangLong result = (OtpErlangLong) raw_result;
		assertEquals(6, result.intValue());
		c.close();
	}

	/**
	 * Test method for
	 * {@link de.zib.scalaris.Connection#doRPC(String, String, OtpErlangList)}.
	 * 
	 * 
	 * 
	 * @throws ConnectionException
	 *             if the connection fails
	 * @throws IOException
	 *             if the connection is not active or a communication error
	 *             occurs
	 * @throws OtpErlangExit
	 *             if an exit signal is received from a process on the peer node
	 * @throws OtpAuthException
	 *             if the remote node sends a message containing an invalid
	 *             cookie
	 * @throws OtpErlangRangeException
	 *             if the value is too large to be represented as an int
	 */
	@Test
	public final void testDoRPCStringStringOtpErlangList_fail()
			throws ConnectionException, OtpErlangExit, OtpAuthException,
			IOException, OtpErlangRangeException {
		OtpSelf self = new OtpSelf("testDoRPCStringStringOtpErlangList",
				ConnectionFactory.getInstance().getCookie());
		PeerNode remote = new PeerNode(ConnectionFactory.getInstance()
				.getNodes().get(0).getNode().node());
		Connection c = new Connection(self, remote);

		c.close();

		try {
			OtpErlangObject raw_result = c.doRPC("lists", "sum",
					new OtpErlangList(new OtpErlangList(new OtpErlangObject[] {
							new OtpErlangInt(1), new OtpErlangInt(2),
							new OtpErlangInt(3) })));
			OtpErlangLong result = (OtpErlangLong) raw_result;
			assertEquals(6, result.intValue());
			c.close();
		} catch (Exception e) {
		}
		assertEquals(1, remote.getFailedConnectionsCount());
	}

	/**
	 * Test method for
	 * {@link de.zib.scalaris.Connection#doRPC(String, String, OtpErlangObject[])}
	 * .
	 * 
	 * @throws ConnectionException
	 *             if the connection fails
	 * @throws IOException
	 *             if the connection is not active or a communication error
	 *             occurs
	 * @throws OtpErlangExit
	 *             if an exit signal is received from a process on the peer node
	 * @throws OtpAuthException
	 *             if the remote node sends a message containing an invalid
	 *             cookie
	 * @throws OtpErlangRangeException
	 *             if the value is too large to be represented as an int
	 */
	@Test
	public final void testDoRPCStringStringOtpErlangObjectArray()
			throws ConnectionException, OtpErlangExit, OtpAuthException,
			IOException, OtpErlangRangeException {
		OtpSelf self = new OtpSelf("testDoRPCStringStringOtpErlangObjectArray",
				ConnectionFactory.getInstance().getCookie());
		PeerNode remote = new PeerNode(ConnectionFactory.getInstance()
				.getNodes().get(0).getNode().node());
		Connection c = new Connection(self, remote);

		OtpErlangObject raw_result = c.doRPC("lists", "sum",
				new OtpErlangObject[] { new OtpErlangList(
						new OtpErlangObject[] { new OtpErlangInt(1),
								new OtpErlangInt(2), new OtpErlangInt(3) }) });
		OtpErlangLong result = (OtpErlangLong) raw_result;
		assertEquals(6, result.intValue());
		c.close();
	}

}
