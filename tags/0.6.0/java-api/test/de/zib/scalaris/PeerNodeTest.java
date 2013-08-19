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

import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.ericsson.otp.erlang.OtpPeer;

/**
 * Test cases for the {@link PeerNode} class.
 *
 * @author Nico Kruber, kruber@zib.de
 *
 * @version 2.3
 * @since 2.3
 */
public class PeerNodeTest {
    /**
     * Test method for {@link PeerNode#PeerNode(OtpPeer)}.
     */
    @Test
    public final void testPeerNodeOtpPeer() {
        final PeerNode p = new PeerNode(new OtpPeer("test@localhost"));
        assertEquals("test@localhost", p.getNode().node());
    }

    /**
     * Test method for {@link PeerNode#PeerNode(String)}.
     */
    @Test
    public final void testPeerNodeString() {
        final PeerNode p = new PeerNode("test@localhost");
        assertEquals("test@localhost", p.getNode().node());
    }

    /**
     * Test method for {@link PeerNode#getLastFailedConnect()},
     * {@link PeerNode#setLastFailedConnect()} and
     * {@link PeerNode#getFailureCount()}.
     *
     * @throws InterruptedException
     *             if the sleep is interrupted
     */
    @Test
    public final void testGetSetLastFailedConnect() throws InterruptedException {
        final PeerNode p = new PeerNode("test");
        final Date d0 = new Date();

        assertEquals(0, p.getFailureCount());
        assertEquals(null, p.getLastFailedConnect());
        TimeUnit.MILLISECONDS.sleep(10);

        p.setLastFailedConnect();
        final Date d1 = p.getLastFailedConnect();
        assertEquals(1, p.getFailureCount());
        assertNotNull(d1);
        assertTrue(d0.getTime() < d1.getTime());
        TimeUnit.MILLISECONDS.sleep(10);

        p.setLastFailedConnect();
        final Date d2 = p.getLastFailedConnect();
        assertEquals(2, p.getFailureCount());
        assertNotNull(d2);
        assertTrue(d0.getTime() < d2.getTime());
        assertTrue(d1.getTime() < d2.getTime());
    }

    /**
     * Test method for {@link PeerNode#resetFailureCount()}.
     */
    @Test
    public final void testResetFailureCount() {
        final PeerNode p = new PeerNode("test");

        p.resetFailureCount();
        assertEquals(0, p.getFailureCount());
        assertEquals(null, p.getLastFailedConnect());

        p.setLastFailedConnect();

        p.resetFailureCount();
        assertEquals(0, p.getFailureCount());
        assertEquals(null, p.getLastFailedConnect());

        p.setLastFailedConnect();
        p.setLastFailedConnect();

        p.resetFailureCount();
        assertEquals(0, p.getFailureCount());
        assertEquals(null, p.getLastFailedConnect());
    }

    /**
     * Test method for {@link PeerNode#getLastConnectSuccess()} and
     * {@link PeerNode#setLastConnectSuccess()}.
     *
     * @throws InterruptedException
     *             if the sleep is interrupted
     */
    @Test
    public final void testGetSetLastConnectSuccess()
            throws InterruptedException {
        final PeerNode p = new PeerNode("test");
        final Date d0 = new Date();

        assertEquals(null, p.getLastConnectSuccess());

        TimeUnit.MILLISECONDS.sleep(10);
        p.setLastConnectSuccess();
        final Date d1 = p.getLastConnectSuccess();
        assertNotNull(d1);
        assertTrue(d0.getTime() < d1.getTime());

        TimeUnit.MILLISECONDS.sleep(10);
        p.setLastConnectSuccess();
        final Date d2 = p.getLastConnectSuccess();
        assertNotNull(d2);
        assertTrue(d0.getTime() < d2.getTime());
        assertTrue(d1.getTime() < d2.getTime());
    }

}
