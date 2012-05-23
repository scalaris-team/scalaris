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

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.TimeUnit;

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
 * Test cases for the {@link Connection} class.
 *
 * @author Nico Kruber, kruber@zib.de
 *
 * @version 2.3
 * @since 2.3
 */
public class ConnectionTest {
    /**
     * Test method for {@link Connection#Connection(OtpSelf, PeerNode)}.
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
     * @throws InterruptedException if the sleep is interrupted
     */
    @Test
    public final void testConnectionOtpSelfPeerNode() throws ConnectionException,
            OtpErlangExit, OtpAuthException, IOException, InterruptedException {
        final OtpSelf self = new OtpSelf("testConnection@" + ConnectionFactory.getLocalhostName(), ConnectionFactory
                .getInstance().getCookie());
        final PeerNode remote = new PeerNode(ConnectionFactory.getInstance()
                .getNodes().get(0).getNode().node());
        final Date d0 = new Date();
        TimeUnit.MILLISECONDS.sleep(10);
        final Connection c = new Connection(self, remote);

        assertEquals(self, c.getSelf());
        assertEquals(remote, c.getRemote());
        assertTrue(c.getConnection().isConnected());
        assertNotNull(remote.getLastConnectSuccess());
        assertTrue(d0.getTime() < remote.getLastConnectSuccess().getTime());
        assertEquals(0, remote.getFailureCount());
        assertNull(remote.getLastFailedConnect());

        c.close();
    }

    /**
     * Test method for {@link Connection#Connection(OtpSelf, ConnectionPolicy)}.
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
     * @throws InterruptedException if the sleep is interrupted
     */
    @Test
    public final void testConnectionOtpSelfConnectionPolicy() throws ConnectionException,
            OtpErlangExit, OtpAuthException, IOException, InterruptedException {
        final OtpSelf self = new OtpSelf("testConnection@" + ConnectionFactory.getLocalhostName(), ConnectionFactory
                .getInstance().getCookie());
        final PeerNode remote = new PeerNode(ConnectionFactory.getInstance()
                .getNodes().get(0).getNode().node());
        final Date d0 = new Date();
        TimeUnit.MILLISECONDS.sleep(10);
        final Connection c = new Connection(self, new DefaultConnectionPolicy(remote));

        assertEquals(self, c.getSelf());
        assertEquals(remote, c.getRemote());
        assertTrue(c.getConnection().isConnected());
        assertNotNull(remote.getLastConnectSuccess());
        assertTrue(d0.getTime() < remote.getLastConnectSuccess().getTime());
        assertEquals(0, remote.getFailureCount());
        assertNull(remote.getLastFailedConnect());

        c.close();
    }

    /**
     * Test method for
     * {@link Connection#Connection(OtpSelf, PeerNode)}.
     *
     * Tries several kinds of connections that fail and evaluates the statistics
     * of the {@link PeerNode} object.
     *
     * @throws IOException
     *             if the connection is not active or a communication error
     *             occurs (should not happen)
     * @throws InterruptedException if the sleep is interrupted
     */
    @Test
    public final void testFailedConnection() throws IOException, InterruptedException {
        OtpSelf self;
        PeerNode remote;
        Connection c;
        DefaultConnectionPolicy connectionPolicy;
        final Date d0 = new Date();
        Date d1, d2;
        TimeUnit.MILLISECONDS.sleep(10);

        // wrong cookie:
        self = new OtpSelf("testFailedConnection@" + ConnectionFactory.getLocalhostName(),
                ConnectionFactory.getInstance().getCookie() + "someWrongCookieValue");
        remote = new PeerNode(ConnectionFactory.getInstance().getNodes().get(0).getNode().node());
        connectionPolicy = new DefaultConnectionPolicy(remote);
        connectionPolicy.setMaxRetries(0);
        try {
            c = new Connection(self, connectionPolicy);
            // this should have failed!
            fail();
            c.close();
        } catch (final Exception e) {
        }
        assertEquals(1, remote.getFailureCount());
        d1 = remote.getLastFailedConnect();
        assertNotNull(d1);
        assertTrue(d0.getTime() < d1.getTime());
        TimeUnit.MILLISECONDS.sleep(10);
        try {
            c = new Connection(self, connectionPolicy);
            // this should have failed!
            fail();
            c.close();
        } catch (final Exception e) {
        }
        assertEquals(2, remote.getFailureCount());
        d2 = remote.getLastFailedConnect();
        assertNotNull(d2);
        assertTrue(d0.getTime() < d2.getTime());
        assertTrue(d1.getTime() < d2.getTime());
        TimeUnit.MILLISECONDS.sleep(10);

        // unknown host name:
        self = new OtpSelf("testFailedConnection@" + ConnectionFactory.getLocalhostName(),
                ConnectionFactory.getInstance().getCookie());
        remote = new PeerNode(ConnectionFactory.getInstance().getNodes().get(0).getNode().node() + "noneExistingHost");
        connectionPolicy = new DefaultConnectionPolicy(remote);
        connectionPolicy.setMaxRetries(0);
        try {
            c = new Connection(self, connectionPolicy);
            // this should have failed!
            fail();
            c.close();
        } catch (final Exception e) {
        }
        assertEquals(1, remote.getFailureCount());
        d1 = remote.getLastFailedConnect();
        assertNotNull(d1);
        assertTrue(d0.getTime() < d1.getTime());
        TimeUnit.MILLISECONDS.sleep(10);
        try {
            c = new Connection(self, connectionPolicy);
            // this should have failed!
            fail();
            c.close();
        } catch (final Exception e) {
        }
        assertEquals(2, remote.getFailureCount());
        d2 = remote.getLastFailedConnect();
        assertNotNull(d2);
        assertTrue(d0.getTime() < d2.getTime());
        assertTrue(d1.getTime() < d2.getTime());
        TimeUnit.MILLISECONDS.sleep(10);

        // non-existing node name:
        self = new OtpSelf("testFailedConnection@" + ConnectionFactory.getLocalhostName(),
                ConnectionFactory.getInstance().getCookie());
        remote = new PeerNode("noneExistingNode" + ConnectionFactory.getInstance().getNodes().get(0).getNode().node());
        connectionPolicy = new DefaultConnectionPolicy(remote);
        connectionPolicy.setMaxRetries(0);
        try {
            c = new Connection(self, connectionPolicy);
            // this should have failed!
            fail();
            c.close();
        } catch (final Exception e) {
        }
        assertEquals(1, remote.getFailureCount());
        d1 = remote.getLastFailedConnect();
        assertNotNull(d1);
        assertTrue(d0.getTime() < d1.getTime());
        TimeUnit.MILLISECONDS.sleep(10);
        try {
            c = new Connection(self, connectionPolicy);
            // this should have failed!
            fail();
            c.close();
        } catch (final Exception e) {
        }
        assertEquals(2, remote.getFailureCount());
        d2 = remote.getLastFailedConnect();
        assertNotNull(d2);
        assertTrue(d0.getTime() < d2.getTime());
        assertTrue(d1.getTime() < d2.getTime());
        TimeUnit.MILLISECONDS.sleep(10);
    }

    /**
     * Test method for
     * {@link Connection#doRPC(String, String, OtpErlangList)}.
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
        final OtpSelf self = new OtpSelf("testDoRPCStringStringOtpErlangList@" + ConnectionFactory.getLocalhostName(),
                ConnectionFactory.getInstance().getCookie());
        final PeerNode remote = new PeerNode(ConnectionFactory.getInstance()
                .getNodes().get(0).getNode().node());
        final Connection c = new Connection(self, remote);

        final OtpErlangObject raw_result = c.doRPC("lists", "sum", new OtpErlangList(
                new OtpErlangList(new OtpErlangObject[] { new OtpErlangInt(1),
                        new OtpErlangInt(2), new OtpErlangInt(3) })));
        final OtpErlangLong result = (OtpErlangLong) raw_result;

        assertEquals(6, result.intValue());
        assertNotNull(remote.getLastConnectSuccess());
        assertEquals(0, remote.getFailureCount());
        assertNull(remote.getLastFailedConnect());

        c.close();
    }

    /**
     * Test method for
     * {@link Connection#doRPC(String, String, OtpErlangList)}.
     *
     * Closes the connection before doing the RPC which thus fails. Evaluates
     * the statistics of the {@link PeerNode} object.
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
     * @throws InterruptedException if the sleep is interrupted
     */
    @Test
    public final void testDoRPCStringStringOtpErlangList_fail()
            throws ConnectionException, OtpErlangExit, OtpAuthException,
            IOException, OtpErlangRangeException, InterruptedException {
        final OtpSelf self = new OtpSelf("testDoRPCStringStringOtpErlangList@" + ConnectionFactory.getLocalhostName(),
                ConnectionFactory.getInstance().getCookie());
        final PeerNode remote = new PeerNode(ConnectionFactory.getInstance()
                .getNodes().get(0).getNode().node());
        final DefaultConnectionPolicy connectionPolicy = new DefaultConnectionPolicy(remote);
        connectionPolicy.setMaxRetries(0);
        final Date d0 = new Date();
        Date d1;
        TimeUnit.MILLISECONDS.sleep(10);
        final Connection c = new Connection(self, connectionPolicy);

        c.close();

        try {
            c.doRPC("lists", "sum",
                    new OtpErlangList(new OtpErlangList(new OtpErlangObject[] {
                            new OtpErlangInt(1), new OtpErlangInt(2),
                            new OtpErlangInt(3) })));
            c.close();
            // this should have failed!
            fail();
        } catch (final Exception e) {
        }
        assertEquals(1, remote.getFailureCount());
        d1 = remote.getLastFailedConnect();
        assertNotNull(d1);
        assertTrue(d0.getTime() < d1.getTime());
    }

    /**
     * Test method for
     * {@link Connection#doRPC(String, String, OtpErlangObject[])}
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
        final OtpSelf self = new OtpSelf("testDoRPCStringStringOtpErlangObjectArray@" + ConnectionFactory.getLocalhostName(),
                ConnectionFactory.getInstance().getCookie());
        final PeerNode remote = new PeerNode(ConnectionFactory.getInstance()
                .getNodes().get(0).getNode().node());
        final Connection c = new Connection(self, remote);

        final OtpErlangObject raw_result = c.doRPC("lists", "sum",
                new OtpErlangObject[] { new OtpErlangList(
                        new OtpErlangObject[] { new OtpErlangInt(1),
                                new OtpErlangInt(2), new OtpErlangInt(3) }) });
        final OtpErlangLong result = (OtpErlangLong) raw_result;

        assertEquals(6, result.intValue());
        assertNotNull(remote.getLastConnectSuccess());
        assertEquals(0, remote.getFailureCount());
        assertNull(remote.getLastFailedConnect());

        c.close();
    }

    /**
     * Test method for
     * {@link Connection#doRPC(String, String, OtpErlangObject[])}.
     *
     * Closes the connection before doing the RPC which thus fails. Evaluates
     * the statistics of the {@link PeerNode} object.
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
     * @throws InterruptedException if the sleep is interrupted
     */
    @Test
    public final void testDoRPCStringStringOtpErlangObjectArray_fail()
            throws ConnectionException, OtpErlangExit, OtpAuthException,
            IOException, OtpErlangRangeException, InterruptedException {
        final OtpSelf self = new OtpSelf("testDoRPCStringStringOtpErlangList@" + ConnectionFactory.getLocalhostName(),
                ConnectionFactory.getInstance().getCookie());
        final PeerNode remote = new PeerNode(ConnectionFactory.getInstance()
                .getNodes().get(0).getNode().node());
        final DefaultConnectionPolicy connectionPolicy = new DefaultConnectionPolicy(remote);
        connectionPolicy.setMaxRetries(0);
        final Date d0 = new Date();
        Date d1;
        TimeUnit.MILLISECONDS.sleep(10);
        final Connection c = new Connection(self, connectionPolicy);

        c.close();

        try {
            c.doRPC("lists", "sum",
                    new OtpErlangObject[] { new OtpErlangList(
                            new OtpErlangObject[] { new OtpErlangInt(1),
                                    new OtpErlangInt(2), new OtpErlangInt(3) }) });
            c.close();
            // this should have failed!
            fail();
        } catch (final Exception e) {
        }
        assertEquals(1, remote.getFailureCount());
        d1 = remote.getLastFailedConnect();
        assertNotNull(d1);
        assertTrue(d0.getTime() < d1.getTime());
    }

}
