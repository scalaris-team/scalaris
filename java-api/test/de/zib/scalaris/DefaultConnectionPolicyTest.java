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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

/**
 * Test cases for the {@link DefaultConnectionPolicy} class.
 *
 * @author Nico Kruber, kruber@zib.de
 *
 * @version 2.3
 * @since 2.3
 */
public class DefaultConnectionPolicyTest {
    /**
     * Test method for {@link DefaultConnectionPolicy#DefaultConnectionPolicy(PeerNode)}.
     */
    @Test
    public final void testDefaultConnectionPolicyPeerNode() {
        PeerNode remote;
        DefaultConnectionPolicy p;
        List<PeerNode> goodNodes, badNodes;

        remote = new PeerNode("test@localhost");
        p = new DefaultConnectionPolicy(remote);
        goodNodes = p.getGoodNodes();
        badNodes = p.getBadNodes();
        assertTrue(goodNodes.contains(remote));
        assertEquals(0, badNodes.size());
        assertEquals(1, goodNodes.size());

        remote = new PeerNode("test@localhost");
        remote.setLastConnectSuccess();
        p = new DefaultConnectionPolicy(remote);
        goodNodes = p.getGoodNodes();
        badNodes = p.getBadNodes();
        assertTrue(goodNodes.contains(remote));
        assertEquals(0, badNodes.size());
        assertEquals(1, goodNodes.size());

        remote = new PeerNode("test@localhost");
        remote.setLastFailedConnect();
        p = new DefaultConnectionPolicy(remote);
        goodNodes = p.getGoodNodes();
        badNodes = p.getBadNodes();
        assertTrue(badNodes.contains(remote));
        assertEquals(0, goodNodes.size());
        assertEquals(1, badNodes.size());
    }

    /**
     * Test method for {@link DefaultConnectionPolicy#DefaultConnectionPolicy(List)}.
     * @throws InterruptedException if the sleep is interrupted
     */
    @Test
    public final void testDefaultConnectionPolicyListOfPeerNode() throws InterruptedException {
        final List<PeerNode> remotes = new ArrayList<PeerNode>();
        DefaultConnectionPolicy p;
        PeerNode p1, p2, p3;
        List<PeerNode> goodNodes, badNodes;

        // single node list:

        remotes.clear();
        p1 = new PeerNode("test1@localhost");
        remotes.add(p1);
        p = new DefaultConnectionPolicy(remotes);
        goodNodes = p.getGoodNodes();
        badNodes = p.getBadNodes();
        assertTrue(goodNodes.contains(p1));
        assertEquals(0, badNodes.size());
        assertEquals(1, goodNodes.size());

        remotes.clear();
        p1 = new PeerNode("test1@localhost");
        p1.setLastConnectSuccess();
        remotes.add(p1);
        p = new DefaultConnectionPolicy(remotes);
        goodNodes = p.getGoodNodes();
        badNodes = p.getBadNodes();
        assertTrue(goodNodes.contains(p1));
        assertEquals(0, badNodes.size());
        assertEquals(1, goodNodes.size());

        remotes.clear();
        p1 = new PeerNode("test1@localhost");
        p1.setLastFailedConnect();
        remotes.add(p1);
        p = new DefaultConnectionPolicy(remotes);
        goodNodes = p.getGoodNodes();
        badNodes = p.getBadNodes();
        assertTrue(badNodes.contains(p1));
        assertEquals(1, badNodes.size());
        assertEquals(0, goodNodes.size());

        // more nodes:

        remotes.clear();
        p1 = new PeerNode("test1@localhost");
        p2 = new PeerNode("test2@localhost");
        p3 = new PeerNode("test3@localhost");
        p2.setLastConnectSuccess();
        p3.setLastFailedConnect();
        remotes.add(p1);
        remotes.add(p2);
        remotes.add(p3);
        p = new DefaultConnectionPolicy(remotes);
        goodNodes = p.getGoodNodes();
        badNodes = p.getBadNodes();
        assertTrue(goodNodes.contains(p1));
        assertTrue(goodNodes.contains(p2));
        assertTrue(badNodes.contains(p3));
        assertEquals(1, badNodes.size());
        assertEquals(2, goodNodes.size());

        // different order in the list should yield to the same result:
        remotes.clear();
        remotes.add(p2);
        remotes.add(p3);
        remotes.add(p1);
        p = new DefaultConnectionPolicy(remotes);
        goodNodes = p.getGoodNodes();
        badNodes = p.getBadNodes();
        assertTrue(goodNodes.contains(p1));
        assertTrue(goodNodes.contains(p2));
        assertTrue(badNodes.contains(p3));
        assertEquals(1, badNodes.size());
        assertEquals(2, goodNodes.size());

        // try more failed nodes, also check order:
        remotes.clear();
        p1 = new PeerNode("test1@localhost");
        p2 = new PeerNode("test2@localhost");
        p3 = new PeerNode("test3@localhost");
        p2.setLastFailedConnect();
        TimeUnit.MILLISECONDS.sleep(10);
        p3.setLastFailedConnect();
        remotes.add(p1);
        remotes.add(p2);
        remotes.add(p3);
        p = new DefaultConnectionPolicy(remotes);
        goodNodes = p.getGoodNodes();
        badNodes = p.getBadNodes();
        assertTrue(goodNodes.contains(p1));
        assertTrue(badNodes.contains(p2));
        assertTrue(badNodes.contains(p3));
        assertEquals(2, badNodes.size());
        assertEquals(1, goodNodes.size());
        assertEquals(p2, badNodes.get(0));

        // all failed nodes, also check order:
        remotes.clear();
        p1 = new PeerNode("test1@localhost");
        p2 = new PeerNode("test2@localhost");
        p3 = new PeerNode("test3@localhost");
        p1.setLastFailedConnect();
        TimeUnit.MILLISECONDS.sleep(10);
        p2.setLastFailedConnect();
        TimeUnit.MILLISECONDS.sleep(10);
        p3.setLastFailedConnect();
        remotes.add(p1);
        remotes.add(p2);
        remotes.add(p3);
        p = new DefaultConnectionPolicy(remotes);
        goodNodes = p.getGoodNodes();
        badNodes = p.getBadNodes();
        assertTrue(badNodes.contains(p1));
        assertTrue(badNodes.contains(p2));
        assertTrue(badNodes.contains(p3));
        assertEquals(3, badNodes.size());
        assertEquals(0, goodNodes.size());
        assertEquals(p1, badNodes.get(0));
        assertEquals(p3, badNodes.get(badNodes.size() - 1));

        // different order in the list should yield to the same result:
        remotes.clear();
        remotes.add(p2);
        remotes.add(p3);
        remotes.add(p1);
        p = new DefaultConnectionPolicy(remotes);
        goodNodes = p.getGoodNodes();
        badNodes = p.getBadNodes();
        assertTrue(badNodes.contains(p1));
        assertTrue(badNodes.contains(p2));
        assertTrue(badNodes.contains(p3));
        assertEquals(3, badNodes.size());
        assertEquals(0, goodNodes.size());
        assertEquals(p1, badNodes.get(0));
        assertEquals(p3, badNodes.get(badNodes.size() - 1));

        // all failed nodes at the same time, also check order:
        remotes.clear();
        p1 = new PeerNode("test1@localhost");
        p2 = new PeerNode("test2@localhost");
        p3 = new PeerNode("test3@localhost");
        p1.setLastFailedConnect();
        p2.setLastFailedConnect();
        p3.setLastFailedConnect();
        remotes.add(p1);
        remotes.add(p2);
        remotes.add(p3);
        p = new DefaultConnectionPolicy(remotes);
        goodNodes = p.getGoodNodes();
        badNodes = p.getBadNodes();
        assertTrue(badNodes.contains(p1));
        assertTrue(badNodes.contains(p2));
        assertTrue(badNodes.contains(p3));
        assertEquals(3, badNodes.size());
        assertEquals(0, goodNodes.size());
        // the actual order now depends on the nodes' hash codes and can't be
        // checked here
//        assertEquals(p1, badNodes.get(0));
//        assertEquals(p3, badNodes.get(badNodes.size() - 1));
    }

    /**
     * Test method for {@link DefaultConnectionPolicy#availableNodeAdded(PeerNode)}.
     */
    @Test
    public final void testAvailableNodeAdded() {
        final PeerNode remote = new PeerNode("test@localhost");
        DefaultConnectionPolicy p;
        PeerNode p1, p2;

        p = new DefaultConnectionPolicy(remote);
        p1 = new PeerNode("test1@localhost");
        p.availableNodeAdded(p1);
        assertTrue(p.getGoodNodes().contains(remote));
        assertTrue(p.getGoodNodes().contains(p1));
        assertEquals(0, p.getBadNodes().size());
        assertEquals(2, p.getGoodNodes().size());

        p = new DefaultConnectionPolicy(remote);
        p1 = new PeerNode("test1@localhost");
        p1.setLastConnectSuccess();
        p.availableNodeAdded(p1);
        assertTrue(p.getGoodNodes().contains(remote));
        assertTrue(p.getGoodNodes().contains(p1));
        assertEquals(0, p.getBadNodes().size());
        assertEquals(2, p.getGoodNodes().size());

        p = new DefaultConnectionPolicy(remote);
        p1 = new PeerNode("test1@localhost");
        p1.setLastFailedConnect();
        p.availableNodeAdded(p1);
        assertTrue(p.getGoodNodes().contains(remote));
        assertTrue(p.getBadNodes().contains(p1));
        assertEquals(1, p.getBadNodes().size());
        assertEquals(1, p.getGoodNodes().size());

        p = new DefaultConnectionPolicy(remote);
        p1 = new PeerNode("test1@localhost");
        p2 = new PeerNode("test2@localhost");
        p.availableNodeAdded(p1);
        p.availableNodeAdded(p2);
        assertTrue(p.getGoodNodes().contains(remote));
        assertTrue(p.getGoodNodes().contains(p1));
        assertTrue(p.getGoodNodes().contains(p2));
        assertEquals(0, p.getBadNodes().size());
        assertEquals(3, p.getGoodNodes().size());
    }

    /**
     * Test method for {@link DefaultConnectionPolicy#availableNodeRemoved(PeerNode)}.
     */
    @Test
    public final void testAvailableNodeRemoved() {
        final List<PeerNode> remotes = new ArrayList<PeerNode>();
        DefaultConnectionPolicy p;
        PeerNode p1, p2, p3;
        List<PeerNode> goodNodes, badNodes;

        remotes.clear();
        p1 = new PeerNode("test1@localhost");
        p2 = new PeerNode("test2@localhost");
        p3 = new PeerNode("test3@localhost");
        p2.setLastConnectSuccess();
        p3.setLastFailedConnect();
        remotes.add(p1);
        remotes.add(p2);
        remotes.add(p3);
        p = new DefaultConnectionPolicy(remotes);

        p.availableNodeRemoved(p1);
        goodNodes = p.getGoodNodes();
        badNodes = p.getBadNodes();
        assertFalse(goodNodes.contains(p1));
        assertTrue(goodNodes.contains(p2));
        assertTrue(badNodes.contains(p3));
        assertEquals(1, badNodes.size());
        assertEquals(1, goodNodes.size());

        p.availableNodeRemoved(p2);
        goodNodes = p.getGoodNodes();
        badNodes = p.getBadNodes();
        assertFalse(goodNodes.contains(p1));
        assertFalse(goodNodes.contains(p2));
        assertTrue(badNodes.contains(p3));
        assertEquals(1, badNodes.size());
        assertEquals(0, goodNodes.size());

        p.availableNodeRemoved(p3);
        goodNodes = p.getGoodNodes();
        badNodes = p.getBadNodes();
        assertFalse(goodNodes.contains(p1));
        assertFalse(goodNodes.contains(p2));
        assertFalse(badNodes.contains(p3));
        assertEquals(0, badNodes.size());
        assertEquals(0, goodNodes.size());
    }

    /**
     * Test method for {@link DefaultConnectionPolicy#availableNodesReset()}.
     */
    @Test
    public final void testAvailableNodesReset() {
        final List<PeerNode> remotes = new ArrayList<PeerNode>();
        DefaultConnectionPolicy p;
        PeerNode p1, p2, p3;
        List<PeerNode> goodNodes, badNodes;

        remotes.clear();
        p1 = new PeerNode("test1@localhost");
        p2 = new PeerNode("test2@localhost");
        p3 = new PeerNode("test3@localhost");
        p2.setLastConnectSuccess();
        p3.setLastFailedConnect();
        remotes.add(p1);
        remotes.add(p2);
        remotes.add(p3);
        p = new DefaultConnectionPolicy(remotes);

        p.availableNodesReset();
        goodNodes = p.getGoodNodes();
        badNodes = p.getBadNodes();
        assertEquals(0, badNodes.size());
        assertEquals(0, goodNodes.size());
    }

    /**
     * Test method for {@link DefaultConnectionPolicy#nodeFailed(PeerNode)}.
     * @throws InterruptedException  if the sleep is interrupted
     */
    @Test
    public final void testNodeFailed() throws InterruptedException {
        final List<PeerNode> remotes = new ArrayList<PeerNode>();
        DefaultConnectionPolicy p;
        PeerNode p1, p2, p3;
        List<PeerNode> goodNodes, badNodes;

        // without time:

        remotes.clear();
        p1 = new PeerNode("test1@localhost");
        p2 = new PeerNode("test2@localhost");
        p3 = new PeerNode("test3@localhost");
        p2.setLastConnectSuccess();
        p3.setLastFailedConnect();
        remotes.add(p1);
        remotes.add(p2);
        remotes.add(p3);
        p = new DefaultConnectionPolicy(remotes);
        p.nodeFailed(p1);
        goodNodes = p.getGoodNodes();
        badNodes = p.getBadNodes();
        assertEquals(1, p1.getFailureCount());
        assertEquals(0, p2.getFailureCount());
        assertEquals(1, p3.getFailureCount());
        assertTrue(badNodes.contains(p1));
        assertTrue(goodNodes.contains(p2));
        assertTrue(badNodes.contains(p3));
        assertEquals(2, badNodes.size());
        assertEquals(1, goodNodes.size());

        remotes.clear();
        p1 = new PeerNode("test1@localhost");
        p2 = new PeerNode("test2@localhost");
        p3 = new PeerNode("test3@localhost");
        p2.setLastConnectSuccess();
        p3.setLastFailedConnect();
        remotes.add(p1);
        remotes.add(p2);
        remotes.add(p3);
        p = new DefaultConnectionPolicy(remotes);
        p.nodeFailed(p2);
        goodNodes = p.getGoodNodes();
        badNodes = p.getBadNodes();
        assertEquals(0, p1.getFailureCount());
        assertEquals(1, p2.getFailureCount());
        assertEquals(1, p3.getFailureCount());
        assertTrue(goodNodes.contains(p1));
        assertTrue(badNodes.contains(p2));
        assertTrue(badNodes.contains(p3));
        assertEquals(2, badNodes.size());
        assertEquals(1, goodNodes.size());

        remotes.clear();
        p1 = new PeerNode("test1@localhost");
        p2 = new PeerNode("test2@localhost");
        p3 = new PeerNode("test3@localhost");
        p2.setLastConnectSuccess();
        p3.setLastFailedConnect();
        remotes.add(p1);
        remotes.add(p2);
        remotes.add(p3);
        p = new DefaultConnectionPolicy(remotes);
        p.nodeFailed(p3);
        goodNodes = p.getGoodNodes();
        badNodes = p.getBadNodes();
        assertEquals(0, p1.getFailureCount());
        assertEquals(0, p2.getFailureCount());
        assertEquals(2, p3.getFailureCount());
        assertTrue(goodNodes.contains(p1));
        assertTrue(goodNodes.contains(p2));
        assertTrue(badNodes.contains(p3));
        assertEquals(1, badNodes.size());
        assertEquals(2, goodNodes.size());

        // with time:
        remotes.clear();
        p1 = new PeerNode("test1@localhost");
        p2 = new PeerNode("test2@localhost");
        p3 = new PeerNode("test3@localhost");
        p2.setLastConnectSuccess();
        p3.setLastFailedConnect();
        TimeUnit.MILLISECONDS.sleep(10);
        remotes.add(p1);
        remotes.add(p2);
        remotes.add(p3);
        p = new DefaultConnectionPolicy(remotes);
        p.nodeFailed(p1);
        goodNodes = p.getGoodNodes();
        badNodes = p.getBadNodes();
        assertEquals(1, p1.getFailureCount());
        assertEquals(0, p2.getFailureCount());
        assertEquals(1, p3.getFailureCount());
        assertTrue(badNodes.contains(p1));
        assertTrue(goodNodes.contains(p2));
        assertTrue(badNodes.contains(p3));
        assertEquals(2, badNodes.size());
        assertEquals(1, goodNodes.size());
        assertEquals(p3, badNodes.get(0));
        assertEquals(p1, badNodes.get(badNodes.size() - 1));

        remotes.clear();
        p1 = new PeerNode("test1@localhost");
        p2 = new PeerNode("test2@localhost");
        p3 = new PeerNode("test3@localhost");
        p2.setLastConnectSuccess();
        p3.setLastFailedConnect();
        TimeUnit.MILLISECONDS.sleep(10);
        remotes.add(p1);
        remotes.add(p2);
        remotes.add(p3);
        p = new DefaultConnectionPolicy(remotes);
        p.nodeFailed(p2);
        goodNodes = p.getGoodNodes();
        badNodes = p.getBadNodes();
        assertEquals(0, p1.getFailureCount());
        assertEquals(1, p2.getFailureCount());
        assertEquals(1, p3.getFailureCount());
        assertTrue(goodNodes.contains(p1));
        assertTrue(badNodes.contains(p2));
        assertTrue(badNodes.contains(p3));
        assertEquals(2, badNodes.size());
        assertEquals(1, goodNodes.size());
        assertEquals(p3, badNodes.get(0));
        assertEquals(p2, badNodes.get(badNodes.size() - 1));

        remotes.clear();
        p1 = new PeerNode("test1@localhost");
        p2 = new PeerNode("test2@localhost");
        p3 = new PeerNode("test3@localhost");
        p2.setLastConnectSuccess();
        p3.setLastFailedConnect();
        TimeUnit.MILLISECONDS.sleep(10);
        remotes.add(p1);
        remotes.add(p2);
        remotes.add(p3);
        p = new DefaultConnectionPolicy(remotes);
        p.nodeFailed(p3);
        goodNodes = p.getGoodNodes();
        badNodes = p.getBadNodes();
        assertEquals(0, p1.getFailureCount());
        assertEquals(0, p2.getFailureCount());
        assertEquals(2, p3.getFailureCount());
        assertTrue(goodNodes.contains(p1));
        assertTrue(goodNodes.contains(p2));
        assertTrue(badNodes.contains(p3));
        assertEquals(1, badNodes.size());
        assertEquals(2, goodNodes.size());
        assertEquals(p3, badNodes.get(0));
    }

    /**
     * Test method for {@link DefaultConnectionPolicy#nodeFailReset(PeerNode)}.
     */
    @Test
    public final void testnodeFailReset() {
        final List<PeerNode> remotes = new ArrayList<PeerNode>();
        DefaultConnectionPolicy p;
        PeerNode p1, p2, p3;
        List<PeerNode> goodNodes, badNodes;

        remotes.clear();
        p1 = new PeerNode("test1@localhost");
        p2 = new PeerNode("test2@localhost");
        p3 = new PeerNode("test3@localhost");
        p2.setLastConnectSuccess();
        p3.setLastFailedConnect();
        remotes.add(p1);
        remotes.add(p2);
        remotes.add(p3);
        p = new DefaultConnectionPolicy(remotes);
        p.nodeFailReset(p1);
        goodNodes = p.getGoodNodes();
        badNodes = p.getBadNodes();
        assertEquals(0, p1.getFailureCount());
        assertEquals(0, p2.getFailureCount());
        assertEquals(1, p3.getFailureCount());
        assertTrue(goodNodes.contains(p1));
        assertTrue(goodNodes.contains(p2));
        assertTrue(badNodes.contains(p3));
        assertEquals(1, badNodes.size());
        assertEquals(2, goodNodes.size());

        remotes.clear();
        p1 = new PeerNode("test1@localhost");
        p2 = new PeerNode("test2@localhost");
        p3 = new PeerNode("test3@localhost");
        p2.setLastConnectSuccess();
        p3.setLastFailedConnect();
        remotes.add(p1);
        remotes.add(p2);
        remotes.add(p3);
        p = new DefaultConnectionPolicy(remotes);
        p.nodeFailReset(p2);
        goodNodes = p.getGoodNodes();
        badNodes = p.getBadNodes();
        assertEquals(0, p1.getFailureCount());
        assertEquals(0, p2.getFailureCount());
        assertEquals(1, p3.getFailureCount());
        assertTrue(goodNodes.contains(p1));
        assertTrue(goodNodes.contains(p2));
        assertTrue(badNodes.contains(p3));
        assertEquals(1, badNodes.size());
        assertEquals(2, goodNodes.size());

        remotes.clear();
        p1 = new PeerNode("test1@localhost");
        p2 = new PeerNode("test2@localhost");
        p3 = new PeerNode("test3@localhost");
        p2.setLastConnectSuccess();
        p3.setLastFailedConnect();
        remotes.add(p1);
        remotes.add(p2);
        remotes.add(p3);
        p = new DefaultConnectionPolicy(remotes);
        p.nodeFailReset(p3);
        goodNodes = p.getGoodNodes();
        badNodes = p.getBadNodes();
        assertEquals(0, p1.getFailureCount());
        assertEquals(0, p2.getFailureCount());
        assertEquals(0, p3.getFailureCount());
        assertTrue(goodNodes.contains(p1));
        assertTrue(goodNodes.contains(p2));
        assertTrue(goodNodes.contains(p3));
        assertEquals(0, badNodes.size());
        assertEquals(3, goodNodes.size());
    }

    /**
     * Test method for {@link DefaultConnectionPolicy#nodeConnectSuccess(PeerNode)}.
     * @throws InterruptedException if the sleep is interrupted
     */
    @Test
    public final void testNodeConnectSuccess() throws InterruptedException {
        final List<PeerNode> remotes = new ArrayList<PeerNode>();
        DefaultConnectionPolicy p;
        PeerNode p1, p2, p3;
        List<PeerNode> goodNodes, badNodes;
        Date d0, d2;

        remotes.clear();
        p1 = new PeerNode("test1@localhost");
        p2 = new PeerNode("test2@localhost");
        p3 = new PeerNode("test3@localhost");
        p2.setLastConnectSuccess();
        p3.setLastFailedConnect();
        remotes.add(p1);
        remotes.add(p2);
        remotes.add(p3);
        d0 = new Date();
        TimeUnit.MILLISECONDS.sleep(10);
        p = new DefaultConnectionPolicy(remotes);
        p.nodeConnectSuccess(p1);
        goodNodes = p.getGoodNodes();
        badNodes = p.getBadNodes();
        assertEquals(0, p1.getFailureCount());
        assertEquals(0, p2.getFailureCount());
        assertEquals(1, p3.getFailureCount());
        assertTrue(goodNodes.contains(p1));
        assertTrue(goodNodes.contains(p2));
        assertTrue(badNodes.contains(p3));
        assertEquals(1, badNodes.size());
        assertEquals(2, goodNodes.size());
        assertNotNull(p1.getLastConnectSuccess());
        assertTrue(d0.getTime() < p1.getLastConnectSuccess().getTime());

        remotes.clear();
        p1 = new PeerNode("test1@localhost");
        p2 = new PeerNode("test2@localhost");
        p3 = new PeerNode("test3@localhost");
        p2.setLastConnectSuccess();
        p3.setLastFailedConnect();
        remotes.add(p1);
        remotes.add(p2);
        remotes.add(p3);
        d0 = new Date();
        TimeUnit.MILLISECONDS.sleep(10);
        d2 = p2.getLastConnectSuccess();
        TimeUnit.MILLISECONDS.sleep(10);
        p = new DefaultConnectionPolicy(remotes);
        p.nodeConnectSuccess(p2);
        goodNodes = p.getGoodNodes();
        badNodes = p.getBadNodes();
        assertEquals(0, p1.getFailureCount());
        assertEquals(0, p2.getFailureCount());
        assertEquals(1, p3.getFailureCount());
        assertTrue(goodNodes.contains(p1));
        assertTrue(goodNodes.contains(p2));
        assertTrue(badNodes.contains(p3));
        assertEquals(1, badNodes.size());
        assertEquals(2, goodNodes.size());
        assertNotNull(p2.getLastConnectSuccess());
        assertTrue(d0.getTime() < p2.getLastConnectSuccess().getTime());
        assertTrue(d2.getTime() < p2.getLastConnectSuccess().getTime());

        remotes.clear();
        p1 = new PeerNode("test1@localhost");
        p2 = new PeerNode("test2@localhost");
        p3 = new PeerNode("test3@localhost");
        p2.setLastConnectSuccess();
        p3.setLastFailedConnect();
        remotes.add(p1);
        remotes.add(p2);
        remotes.add(p3);
        d0 = new Date();
        TimeUnit.MILLISECONDS.sleep(10);
        p = new DefaultConnectionPolicy(remotes);
        p.nodeConnectSuccess(p3);
        goodNodes = p.getGoodNodes();
        badNodes = p.getBadNodes();
        assertEquals(0, p1.getFailureCount());
        assertEquals(0, p2.getFailureCount());
        assertEquals(0, p3.getFailureCount());
        assertTrue(goodNodes.contains(p1));
        assertTrue(goodNodes.contains(p2));
        assertTrue(goodNodes.contains(p3));
        assertEquals(0, badNodes.size());
        assertEquals(3, goodNodes.size());
        assertNotNull(p3.getLastConnectSuccess());
        assertTrue(d0.getTime() < p3.getLastConnectSuccess().getTime());
    }

    /**
     * Test method for {@link DefaultConnectionPolicy#selectNode()}.
     * @throws InterruptedException if the sleep is interrupted
     */
    @Test
    public final void testSelectNode() throws InterruptedException {
        final List<PeerNode> remotes = new ArrayList<PeerNode>();
        DefaultConnectionPolicy p;
        PeerNode p1, p2, p3;

        remotes.clear();
        p1 = new PeerNode("test1@localhost");
        p2 = new PeerNode("test2@localhost");
        p3 = new PeerNode("test3@localhost");
        p1.setLastFailedConnect();
        TimeUnit.MILLISECONDS.sleep(10);
        p2.setLastFailedConnect();
        TimeUnit.MILLISECONDS.sleep(10);
        p3.setLastFailedConnect();
        TimeUnit.MILLISECONDS.sleep(10);
        remotes.add(p1);
        remotes.add(p2);
        remotes.add(p3);
        p = new DefaultConnectionPolicy(remotes);
        p.setMaxRetries(0);

        // cycling through bad nodes:
        try {
            assertEquals(p1, p.selectNode());
            assertEquals(p1, p.selectNode());
        } catch (final Exception e) {
            fail();
        }

        try {
            assertEquals(p1, p.selectNode());
            p.nodeFailed(p1);
            TimeUnit.MILLISECONDS.sleep(10);
            assertEquals(p2, p.selectNode());
            p.nodeFailed(p2);
            TimeUnit.MILLISECONDS.sleep(10);
            assertEquals(p3, p.selectNode());
            p.nodeFailed(p3);
            TimeUnit.MILLISECONDS.sleep(10);
            assertEquals(p1, p.selectNode());
        } catch (final RuntimeException e) {
            fail();
        }

        // getting random good nodes:
        remotes.clear();
        p1 = new PeerNode("test1@localhost");
        p2 = new PeerNode("test2@localhost");
        p3 = new PeerNode("test3@localhost");
        p2.setLastConnectSuccess();
        TimeUnit.MILLISECONDS.sleep(10);
        p3.setLastFailedConnect();
        TimeUnit.MILLISECONDS.sleep(10);
        remotes.add(p1);
        remotes.add(p2);
        remotes.add(p3);
        p = new DefaultConnectionPolicy(remotes);
        p.setMaxRetries(0);
        int p1Found = 0;
        int p2Found = 0;
        int p3Found = 0;

        try {
            // get 100 random nodes:
            for (int i = 0; i < 100; ++i) {
                final PeerNode n = p.selectNode();
                if (n == p1) {
                    ++p1Found;
                }
                if (n == p2) {
                    ++p2Found;
                }
                if (n == p3) {
                    ++p3Found;
                }
            }
            assertTrue(p1Found > 0);
            assertTrue(p2Found > 0);
            assertTrue(p3Found == 0);
            // this may fail but the uniform random number distribution should
            // at result in p1 and p2 to be found 40 times:
            if ((p1Found < 40) || (p2Found < 40)) {
                System.err
                        .println("Warning: DefaultConnectionPolicyTest::testSelectNodeIntPeerNodeE(): 100 selects, p1="
                                + p1Found + ", p2=" + p2Found);
            }
        } catch (final Exception e) {
            fail();
        }
    }

    /**
     * Test method for {@link DefaultConnectionPolicy#selectNode(int, PeerNode, Exception)}.
     * @throws InterruptedException if the sleep is interrupted
     */
    @Test
    public final void testSelectNodeIntPeerNodeE() throws InterruptedException {
        final List<PeerNode> remotes = new ArrayList<PeerNode>();
        DefaultConnectionPolicy p;
        PeerNode p1, p2, p3;
        final PeerNode failedNode = new PeerNode("failedNode@localhost");

        remotes.clear();
        p1 = new PeerNode("test1@localhost");
        p2 = new PeerNode("test2@localhost");
        p3 = new PeerNode("test3@localhost");
        p1.setLastFailedConnect();
        TimeUnit.MILLISECONDS.sleep(10);
        p2.setLastFailedConnect();
        TimeUnit.MILLISECONDS.sleep(10);
        p3.setLastFailedConnect();
        TimeUnit.MILLISECONDS.sleep(10);
        remotes.add(p1);
        remotes.add(p2);
        remotes.add(p3);
        p = new DefaultConnectionPolicy(remotes);
        p.setMaxRetries(0);

        // cycling through bad nodes:
        try {
            assertEquals(p1, p.selectNode(0, failedNode, new Exception()));
            assertEquals(p1, p.selectNode(0, failedNode, new Exception()));
        } catch (final Exception e) {
            fail();
        }

        try {
            assertEquals(p1, p.selectNode(0, failedNode, new Exception()));
            p.nodeFailed(p1);
            TimeUnit.MILLISECONDS.sleep(10);
            assertEquals(p2, p.selectNode(0, failedNode, new Exception()));
            p.nodeFailed(p2);
            TimeUnit.MILLISECONDS.sleep(10);
            assertEquals(p3, p.selectNode(0, failedNode, new Exception()));
            p.nodeFailed(p3);
            TimeUnit.MILLISECONDS.sleep(10);
            assertEquals(p1, p.selectNode(0, failedNode, new Exception()));
        } catch (final Exception e) {
            fail();
        }

        // getting random good nodes:
        remotes.clear();
        p1 = new PeerNode("test1@localhost");
        p2 = new PeerNode("test2@localhost");
        p3 = new PeerNode("test3@localhost");
        p2.setLastConnectSuccess();
        TimeUnit.MILLISECONDS.sleep(10);
        p3.setLastFailedConnect();
        TimeUnit.MILLISECONDS.sleep(10);
        remotes.add(p1);
        remotes.add(p2);
        remotes.add(p3);
        p = new DefaultConnectionPolicy(remotes);
        p.setMaxRetries(0);
        int p1Found = 0;
        int p2Found = 0;
        int p3Found = 0;

        try {
            // get 100 random nodes:
            for (int i = 0; i < 100; ++i) {
                final PeerNode n = p.selectNode(0, failedNode, new Exception());
                if (n == p1) {
                    ++p1Found;
                }
                if (n == p2) {
                    ++p2Found;
                }
                if (n == p3) {
                    ++p3Found;
                }
            }
            assertTrue(p1Found > 0);
            assertTrue(p2Found > 0);
            assertTrue(p3Found == 0);
            // this may fail but the uniform random number distribution should
            // at result in p1 and p2 to be found 40 times:
            if ((p1Found < 40) || (p2Found < 40)) {
                System.err
                        .println("Warning: DefaultConnectionPolicyTest::testSelectNodeIntPeerNodeE(): 100 selects, p1="
                                + p1Found + ", p2=" + p2Found);
            }
        } catch (final Exception e) {
            fail();
        }
    }

    /**
     * Test method for {@link DefaultConnectionPolicy#getMaxRetries()}.
     */
    @Test
    public final void testGetMaxRetries() {
        final DefaultConnectionPolicy p = new DefaultConnectionPolicy(new PeerNode("test@localhost"));
        assertEquals(3, p.getMaxRetries());
        p.setMaxRetries(5);
        assertEquals(5, p.getMaxRetries());
        p.setMaxRetries(1);
        assertEquals(1, p.getMaxRetries());
        p.setMaxRetries(0);
        assertEquals(0, p.getMaxRetries());
    }

    /**
     * Test method for {@link DefaultConnectionPolicy#setMaxRetries(int)}.
     *
     * Tries to set 3, 5, 1 and 0 retries and checks whether
     * {@link DefaultConnectionPolicy#selectNode(int, PeerNode, Exception)}
     * performs this many retries.
     */
    @Test
    public final void testSetMaxRetries() {
        PeerNode remote = new PeerNode("test@localhost");
        final DefaultConnectionPolicy p = new DefaultConnectionPolicy(remote);
        boolean exceptionThrown = false;

        ///// 3 retries:
        do {
            p.setMaxRetries(3);

            try {
                remote = p.selectNode(0, remote, new Exception());
            } catch (final Exception e) {
                fail();
            }

            try {
                remote = p.selectNode(2, remote, new Exception());
            } catch (final Exception e) {
                fail();
            }

            try {
                remote = p.selectNode(3, remote, new Exception());
            } catch (final Exception e) {
                fail();
            }

            exceptionThrown = false;
            try {
                remote = p.selectNode(4, remote, new Exception());
            } catch (final Exception e) {
                exceptionThrown = true;
            }
            assertTrue(exceptionThrown);

            exceptionThrown = false;
            try {
                remote = p.selectNode(5, remote, new Exception());
            } catch (final Exception e) {
                exceptionThrown = true;
            }
            assertTrue(exceptionThrown);

            int retry = 0;
            try {
                for (retry = 0; retry < 10; ++retry) {
                    remote = p.selectNode(retry, remote, new Exception());
    //                p.nodeFailed(remote);
                }
            } catch (final Exception e) {
            }
            assertEquals(4, retry); // the 4th retry has not been performed
                                    // though
        } while (false);

        ///// 5 retries:
        do {
            p.setMaxRetries(5);

            try {
                remote = p.selectNode(0, remote, new Exception());
            } catch (final Exception e) {
                fail();
            }

            try {
                remote = p.selectNode(2, remote, new Exception());
            } catch (final Exception e) {
                fail();
            }

            try {
                remote = p.selectNode(5, remote, new Exception());
            } catch (final Exception e) {
                fail();
            }

            exceptionThrown = false;
            try {
                remote = p.selectNode(6, remote, new Exception());
            } catch (final Exception e) {
                exceptionThrown = true;
            }
            assertTrue(exceptionThrown);

            exceptionThrown = false;
            try {
                remote = p.selectNode(7, remote, new Exception());
            } catch (final Exception e) {
                exceptionThrown = true;
            }
            assertTrue(exceptionThrown);

            int retry = 0;
            try {
                for (retry = 0; retry < 10; ++retry) {
                    remote = p.selectNode(retry, remote, new Exception());
                    // p.nodeFailed(remote);
                }
            } catch (final Exception e) {
            }
            assertEquals(6, retry); // the 6th retry has not been performed
                                    // though
        } while (false);

        ///// 1 retry:
        do {
            p.setMaxRetries(1);

            try {
                remote = p.selectNode(0, remote, new Exception());
            } catch (final Exception e) {
                fail();
            }

            try {
                remote = p.selectNode(1, remote, new Exception());
            } catch (final Exception e) {
                fail();
            }

            exceptionThrown = false;
            try {
                remote = p.selectNode(2, remote, new Exception());
            } catch (final Exception e) {
                exceptionThrown = true;
            }
            assertTrue(exceptionThrown);

            exceptionThrown = false;
            try {
                remote = p.selectNode(3, remote, new Exception());
            } catch (final Exception e) {
                exceptionThrown = true;
            }
            assertTrue(exceptionThrown);

            int retry = 0;
            try {
                for (retry = 0; retry < 10; ++retry) {
                    remote = p.selectNode(retry, remote, new Exception());
                    // p.nodeFailed(remote);
                }
            } catch (final Exception e) {
            }
            assertEquals(2, retry); // the 2nd retry has not been performed
                                    // though
        } while (false);

        ///// 0 retries:
        do {
            p.setMaxRetries(0);

            try {
                remote = p.selectNode(0, remote, new Exception());
            } catch (final Exception e) {
                fail();
            }

            exceptionThrown = false;
            try {
                remote = p.selectNode(1, remote, new Exception());
            } catch (final Exception e) {
                exceptionThrown = true;
            }
            assertTrue(exceptionThrown);

            exceptionThrown = false;
            try {
                remote = p.selectNode(2, remote, new Exception());
            } catch (final Exception e) {
                exceptionThrown = true;
            }
            assertTrue(exceptionThrown);

            int retry = 0;
            try {
                for (retry = 0; retry < 10; ++retry) {
                    remote = p.selectNode(retry, remote, new Exception());
                    // p.nodeFailed(remote);
                }
            } catch (final Exception e) {
            }
            assertEquals(1, retry); // the 1st retry has not been performed
                                    // though
        } while (false);
    }

}
