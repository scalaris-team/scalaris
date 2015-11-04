/**
 *  Copyright 2011 Zuse Institute Berlin
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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Ignore;
import org.junit.Test;

import de.zib.scalaris.ScalarisVM.AddNodesResult;
import de.zib.scalaris.ScalarisVM.DeleteNodesByNameResult;
import de.zib.scalaris.ScalarisVM.GetInfoResult;

/**
 * Test class for {@link ScalarisVM}.
 *
 * @author Nico Kruber, kruber@zib.de
 * @version 3.6
 * @since 3.6
 */
public class ScalarisVMTest {

    protected enum DeleteAction {
        SHUTDOWN, KILL
    }

    final static String scalarisNode;

    static {
        // determine good/bad nodes:
        final ConnectionFactory cf = ConnectionFactory.getInstance();
        cf.testAllNodes();
        // set not to automatically try reconnects (auto-retries prevent ConnectionException tests from working):
        final DefaultConnectionPolicy cp = ((DefaultConnectionPolicy) cf.getConnectionPolicy());
        cp.setMaxRetries(0);
        scalarisNode = cp.selectNode().toString();
    }

    /**
     * Test method for {@link ScalarisVM#ScalarisVM(String)} .
     *
     * @throws ConnectionException
     */
    @Test
    public final void testScalarisVM() throws ConnectionException {
        final ScalarisVM conn = new ScalarisVM(scalarisNode);
        conn.closeConnection();
    }

    /**
     * Test method for {@link ScalarisVM#closeConnection()} trying to close the
     * connection twice.
     *
     * @throws ConnectionException
     */
    @Test
    public void testDoubleClose() throws ConnectionException {
        final ScalarisVM conn = new ScalarisVM(scalarisNode);
        conn.closeConnection();
        conn.closeConnection();
    }

    /**
     * Test method for {@link ScalarisVM#getVersion()} with a closed connection.
     *
     * @throws ConnectionException
     */
    @Test(expected=ConnectionException.class)
    public final void testGetVersion_NotConnected() throws ConnectionException {
        final ScalarisVM conn = new ScalarisVM(scalarisNode);
        conn.closeConnection();
        conn.getVersion();
    }

    /**
     * Test method for {@link ScalarisVM#getVersion()}.
     *
     * @throws ConnectionException
     */
    @Test
    public final void testGetVersion1() throws ConnectionException {
        final ScalarisVM conn = new ScalarisVM(scalarisNode);
        try {
            final String version = conn.getVersion();
            assertTrue(!version.isEmpty());
        } finally {
            conn.closeConnection();
        }
    }

    /**
     * Test method for {@link ScalarisVM#getInfo()} with a closed connection.
     *
     * @throws ConnectionException
     */
    @Test(expected=ConnectionException.class)
    public final void testGetInfo_NotConnected() throws ConnectionException {
        final ScalarisVM conn = new ScalarisVM(scalarisNode);
        conn.closeConnection();
        conn.getInfo();
    }

    /**
     * Test method for {@link ScalarisVM#getInfo()}.
     *
     * @throws ConnectionException
     */
    @Test
    public final void testGetInfo1() throws ConnectionException {
        final ScalarisVM conn = new ScalarisVM(scalarisNode);
        try {
            final GetInfoResult info = conn.getInfo();
            //        System.out.println(info.scalarisVersion);
            assertTrue("scalaris_version (" + info.scalarisVersion + ") != \"\"", !info.scalarisVersion.isEmpty());
            assertTrue("erlang_version (" + info.erlangVersion + ") != \"\"", !info.erlangVersion.isEmpty());
            assertTrue("mem_total (" + info.memTotal + ") >= 0", info.memTotal >= 0);
            assertTrue("uptime (" + info.uptime + ") >= 0", info.uptime >= 0);
            assertTrue("erlang_node (" + info.erlangNode + ") != \"\"", !info.erlangNode.isEmpty());
            assertTrue("0 <= port (" + info.port + ") <= 65535", (info.port >= 0) && (info.port <= 65535));
            assertTrue("0 <= yaws_port (" + info.yawsPort + ") <= 65535", (info.yawsPort >= 0) && (info.yawsPort <= 65535));
        } finally {
            conn.closeConnection();
        }
    }

    /**
     * Test method for {@link ScalarisVM#getNumberOfNodes()} with a closed
     * connection.
     *
     * @throws ConnectionException
     */
    @Test(expected=ConnectionException.class)
    public final void testGetNumberOfNodes_NotConnected() throws ConnectionException {
        final ScalarisVM conn = new ScalarisVM(scalarisNode);
        conn.closeConnection();
        conn.getNumberOfNodes();
    }

    /**
     * Test method for {@link ScalarisVM#getNumberOfNodes()}.
     *
     * @throws ConnectionException
     */
    @Test
    public final void testGetNumberOfNodes1() throws ConnectionException {
        final ScalarisVM conn = new ScalarisVM(scalarisNode);
        try {
            final int numberOfNodes = conn.getNumberOfNodes();
            assertTrue(numberOfNodes >= 0);
        } finally {
            conn.closeConnection();
        }
    }

    /**
     * Test method for {@link ScalarisVM#getNodes()} with a closed connection.
     *
     * @throws ConnectionException
     */
    @Test(expected=ConnectionException.class)
    public final void testGetNodes_NotConnected() throws ConnectionException {
        final ScalarisVM conn = new ScalarisVM(scalarisNode);
        conn.closeConnection();
        conn.getNodes();
    }

    /**
     * Test method for {@link ScalarisVM#getNodes()}.
     *
     * @throws ConnectionException
     */
    @Test
    public final void testGetNodes1() throws ConnectionException {
        final ScalarisVM conn = new ScalarisVM(scalarisNode);
        try {
            final List<ErlangValue> nodes = conn.getNodes();
            assertEquals(conn.getNumberOfNodes(), nodes.size());
        } finally {
            conn.closeConnection();
        }
    }

    /**
     * Test method for {@link ScalarisVM#addNodes(int)} with a closed
     * connection.
     *
     * @throws ConnectionException
     * @throws InterruptedException
     */
    @Test(expected=ConnectionException.class)
    public final void testAddNodes_NotConnected() throws ConnectionException, InterruptedException {
        final ScalarisVM conn = new ScalarisVM(scalarisNode);
        conn.closeConnection();
        conn.addNodes(1);
    }

    /**
     * Test method for {@link ScalarisVM#addNodes(int)}.
     *
     * @throws ConnectionException
     * @throws InterruptedException
     */
    @Test
    public final void testAddNodes0() throws ConnectionException, InterruptedException {
        testAddNodesX(0);
    }

    /**
     * Test method for {@link ScalarisVM#addNodes(int)}.
     *
     * @throws ConnectionException
     * @throws InterruptedException
     */
    @Test
    public final void testAddNodes1() throws ConnectionException, InterruptedException {
        testAddNodesX(1);
    }

    /**
     * Test method for {@link ScalarisVM#addNodes(int)}.
     *
     * @throws ConnectionException
     * @throws InterruptedException
     */
    @Test
    public final void testAddNodes3() throws ConnectionException, InterruptedException {
        testAddNodesX(3);
    }

    /**
     * Test method for {@link ScalarisVM#shutdownNodes(int)} and
     * {@link ScalarisVM#killNodes(int)}.
     *
     * @throws ConnectionException
     * @throws InterruptedException
     */
    private final void testAddNodesX(final int nodesToAdd) throws ConnectionException, InterruptedException {
        final ScalarisVM conn = new ScalarisVM(scalarisNode);
        try {
            int size = conn.getNumberOfNodes();
            final AddNodesResult addedNodes = conn.addNodes(nodesToAdd);
            size += nodesToAdd;
            assertEquals(nodesToAdd, addedNodes.successful.size());
            assertEquals(addedNodes.errors.length(), 0);
            assertEquals(size, conn.getNumberOfNodes());
            final List<ErlangValue> nodes = conn.getNodes();
            for (final ErlangValue name : addedNodes.successful) {
                assertTrue(nodes.toString() + " should contain " + name, nodes.contains(name));
            }
            for (final ErlangValue name : addedNodes.successful) {
                conn.killNode(name);
            }
            size -= nodesToAdd;
            assertEquals(size, conn.getNumberOfNodes());
        } finally {
            conn.closeConnection();
        }
    }

    /**
     * Test method for {@link ScalarisVM#shutdownNode(ErlangValue)} with a closed
     * connection.
     *
     * @throws ConnectionException
     * @throws InterruptedException
     */
    @Test(expected=ConnectionException.class)
    public final void testShutdownNode_NotConnected() throws ConnectionException, InterruptedException {
        final ScalarisVM conn = new ScalarisVM(scalarisNode);
        conn.closeConnection();
        conn.shutdownNode(new ErlangValue("test"));
    }

    /**
     * Test method for {@link ScalarisVM#shutdownNode(ErlangValue)}.
     *
     * @throws ConnectionException
     * @throws InterruptedException
     */
    @Test
    public final void testShutdownNode1() throws ConnectionException, InterruptedException {
        testDeleteNode(DeleteAction.SHUTDOWN);
    }

    /**
     * Test method for {@link ScalarisVM#killNode(ErlangValue)} with a closed
     * connection.
     *
     * @throws ConnectionException
     * @throws InterruptedException
     */
    @Test(expected=ConnectionException.class)
    public final void testKillNode_NotConnected() throws ConnectionException, InterruptedException {
        final ScalarisVM conn = new ScalarisVM(scalarisNode);
        conn.closeConnection();
        conn.killNode(new ErlangValue("test"));
    }

    /**
     * Test method for {@link ScalarisVM#killNode(ErlangValue)}.
     *
     * @throws ConnectionException
     * @throws InterruptedException
     */
    @Test
    public final void testKillNode1() throws ConnectionException, InterruptedException {
        testDeleteNode(DeleteAction.KILL);
    }

    /**
     * Test method for {@link ScalarisVM#shutdownNode(ErlangValue)} and
     * {@link ScalarisVM#killNode(ErlangValue)}.
     *
     * @throws ConnectionException
     * @throws InterruptedException
     */
    private final void testDeleteNode(final DeleteAction action) throws ConnectionException, InterruptedException {
        final ScalarisVM conn = new ScalarisVM(scalarisNode);
        try {
            final int size = conn.getNumberOfNodes();
            final ErlangValue name = conn.addNodes(1).successful.get(0);
            assertEquals(size + 1, conn.getNumberOfNodes());
            boolean result = false;
            switch (action) {
                case SHUTDOWN:
                    result = conn.shutdownNode(name);
                    break;
                case KILL:
                    result = conn.killNode(name);
                    break;
            }
            assertTrue(result);
            assertEquals(size, conn.getNumberOfNodes());
            final List<ErlangValue> nodes = conn.getNodes();
            assertTrue(nodes.toString() + " should not contain " + name, !nodes.contains(name));
        } finally {
            conn.closeConnection();
        }
    }

    /**
     * Test method for {@link ScalarisVM#shutdownNodes(int)} with a closed
     * connection.
     *
     * @throws ConnectionException
     * @throws InterruptedException
     */
    @Test(expected=ConnectionException.class)
    public final void testShutdownNodes_NotConnected() throws ConnectionException, InterruptedException {
        final ScalarisVM conn = new ScalarisVM(scalarisNode);
        conn.closeConnection();
        conn.shutdownNodes(1);
    }

    /**
     * Test method for {@link ScalarisVM#shutdownNodes(int)}.
     *
     * @throws ConnectionException
     * @throws InterruptedException
     */
    @Test
    public final void testShutdownNodes0() throws ConnectionException, InterruptedException {
        testDeleteNodesX(0, DeleteAction.SHUTDOWN);
    }

    /**
     * Test method for {@link ScalarisVM#shutdownNodes(int)}.
     *
     * @throws ConnectionException
     * @throws InterruptedException
     */
    @Test
    public final void testShutdownNodes1() throws ConnectionException, InterruptedException {
        testDeleteNodesX(1, DeleteAction.SHUTDOWN);
    }

    /**
     * Test method for {@link ScalarisVM#shutdownNodes(int)}.
     *
     * @throws ConnectionException
     * @throws InterruptedException
     */
    @Test
    public final void testShutdownNodes3() throws ConnectionException, InterruptedException {
        testDeleteNodesX(3, DeleteAction.SHUTDOWN);
    }

    /**
     * Test method for {@link ScalarisVM#killNodes(int)} with a closed
     * connection.
     *
     * @throws ConnectionException
     * @throws InterruptedException
     */
    @Test(expected=ConnectionException.class)
    public final void testKillNodes_NotConnected() throws ConnectionException, InterruptedException {
        final ScalarisVM conn = new ScalarisVM(scalarisNode);
        conn.closeConnection();
        conn.killNodes(1);
    }

    /**
     * Test method for {@link ScalarisVM#killNodes(int)}.
     *
     * @throws ConnectionException
     * @throws InterruptedException
     */
    @Test
    public final void testKillNodes0() throws ConnectionException, InterruptedException {
        testDeleteNodesX(0, DeleteAction.KILL);
    }

    /**
     * Test method for {@link ScalarisVM#killNodes(int)}.
     *
     * @throws ConnectionException
     * @throws InterruptedException
     */
    @Test
    public final void testKillNodes1() throws ConnectionException, InterruptedException {
        testDeleteNodesX(1, DeleteAction.KILL);
    }

    /**
     * Test method for {@link ScalarisVM#killNodes(int)}.
     *
     * @throws ConnectionException
     * @throws InterruptedException
     */
    @Test
    public final void testKillNodes3() throws ConnectionException, InterruptedException {
        testDeleteNodesX(3, DeleteAction.KILL);
    }

    /**
     * Test method for {@link ScalarisVM#shutdownNodes(int)} and
     * {@link ScalarisVM#killNodes(int)}.
     *
     * @throws ConnectionException
     * @throws InterruptedException
     */
    private final void testDeleteNodesX(final int nodesToRemove, final DeleteAction action) throws ConnectionException, InterruptedException {
        final ScalarisVM conn = new ScalarisVM(scalarisNode);
        try {
            final int size = conn.getNumberOfNodes();
            if (nodesToRemove >= 1) {
                conn.addNodes(nodesToRemove);
                assertEquals(size + nodesToRemove, conn.getNumberOfNodes());
            }
            List<ErlangValue> result = null;
            switch (action) {
                case SHUTDOWN:
                    result = conn.shutdownNodes(nodesToRemove);
                    break;
                case KILL:
                    result = conn.killNodes(nodesToRemove);
                    break;
                default:
                    throw new RuntimeException();
            }
            assertEquals(nodesToRemove, result.size());
            assertEquals(size, conn.getNumberOfNodes());
            final List<ErlangValue> nodes = conn.getNodes();
            for (final ErlangValue name : result) {
                assertTrue(nodes.toString() + " should not contain " + name, !nodes.contains(name));
            }
        } finally {
            conn.closeConnection();
        }
    }

    /**
     * Test method for {@link ScalarisVM#shutdownNodesByName(List)} with a
     * closed connection.
     *
     * @throws ConnectionException
     * @throws InterruptedException
     */
    @Test(expected=ConnectionException.class)
    public final void testShutdownByName_NotConnected() throws ConnectionException, InterruptedException {
        final ScalarisVM conn = new ScalarisVM(scalarisNode);
        conn.closeConnection();
        conn.shutdownNodesByName(Arrays.asList(new ErlangValue("test")));
    }

    /**
     * Test method for {@link ScalarisVM#shutdownNodesByName(List)}.
     *
     * @throws ConnectionException
     * @throws InterruptedException
     */
    @Test
    public final void testShutdownByName0() throws ConnectionException, InterruptedException {
        testDeleteNodesByNameX(0, DeleteAction.SHUTDOWN);
    }

    /**
     * Test method for
     * {@link ScalarisVM#shutdownNodesByName(List)}.
     *
     * @throws ConnectionException
     * @throws InterruptedException
     */
    @Test
    public final void testShutdownByName1() throws ConnectionException, InterruptedException {
        testDeleteNodesByNameX(1, DeleteAction.SHUTDOWN);
    }

    /**
     * Test method for
     * {@link ScalarisVM#shutdownNodesByName(List)}.
     *
     * @throws ConnectionException
     * @throws InterruptedException
     */
    @Test
    public final void testShutdownByName3() throws ConnectionException, InterruptedException {
        testDeleteNodesByNameX(3, DeleteAction.SHUTDOWN);
    }

    /**
     * Test method for {@link ScalarisVM#killNodes(List)} with a closed
     * connection.
     *
     * @throws ConnectionException
     * @throws InterruptedException
     */
    @Test(expected=ConnectionException.class)
    public final void testKillByName_NotConnected() throws ConnectionException, InterruptedException {
        final ScalarisVM conn = new ScalarisVM(scalarisNode);
        conn.closeConnection();
        conn.killNodes(Arrays.asList(new ErlangValue("test")));
    }

    /**
     * Test method for {@link ScalarisVM#killNodes(List)}.
     *
     * @throws ConnectionException
     * @throws InterruptedException
     */
    @Test
    public final void testKillByName0() throws ConnectionException, InterruptedException {
        testDeleteNodesByNameX(0, DeleteAction.KILL);
    }

    /**
     * Test method for {@link ScalarisVM#killNodes(List)}.
     *
     * @throws ConnectionException
     * @throws InterruptedException
     */
    @Test
    public final void testKillByName1() throws ConnectionException, InterruptedException {
        testDeleteNodesByNameX(1, DeleteAction.KILL);
    }

    /**
     * Test method for {@link ScalarisVM#killNodes(List)}.
     *
     * @throws ConnectionException
     * @throws InterruptedException
     */
    @Test
    public final void testKillByName3() throws ConnectionException, InterruptedException {
        testDeleteNodesByNameX(3, DeleteAction.KILL);
    }

    /**
     * Test method for {@link ScalarisVM#shutdownNodesByName(List)} and
     * {@link ScalarisVM#killNodes(List)}.
     *
     * @throws ConnectionException
     * @throws InterruptedException
     */
    private final void testDeleteNodesByNameX(final int nodesToRemove, final DeleteAction action) throws ConnectionException, InterruptedException {
        final ScalarisVM conn = new ScalarisVM(scalarisNode);
        try {
            final int size = conn.getNumberOfNodes();
            if (nodesToRemove >= 1) {
                conn.addNodes(nodesToRemove);
                assertEquals(size + nodesToRemove, conn.getNumberOfNodes());
            }
            List<ErlangValue> nodes = conn.getNodes();
            Collections.shuffle(nodes);
            final List<ErlangValue> removedNodes = nodes.subList(nodes.size() - nodesToRemove, nodes.size());
            DeleteNodesByNameResult result = null;
            switch (action) {
                case SHUTDOWN:
                    result = conn.shutdownNodesByName(removedNodes);
                    break;
                case KILL:
                    result = conn.killNodes(removedNodes);
                    break;
                default:
                    throw new RuntimeException();
            }
            assertEquals(nodesToRemove, result.successful.size());
            assertEquals(0, result.notFound.size());
            Collections.sort(removedNodes);
            Collections.sort(result.successful);
            assertEquals(removedNodes, result.successful);
            assertEquals(size, conn.getNumberOfNodes());
            nodes = conn.getNodes();
            for (final ErlangValue name : result.successful) {
                assertTrue(nodes.toString() + " should not contain " + name, !nodes.contains(name));
            }
        } finally {
            conn.closeConnection();
        }
    }

    /**
     * Test method for {@link ScalarisVM#getOtherVMs(int)} with a closed
     * connection.
     *
     * @throws ConnectionException
     * @throws InterruptedException
     */
    @Test(expected=ConnectionException.class)
    public final void testGetOtherVMs_NotConnected() throws ConnectionException, InterruptedException {
        final ScalarisVM conn = new ScalarisVM(scalarisNode);
        conn.closeConnection();
        conn.getOtherVMs(1);
    }

    /**
     * Test method for {@link ScalarisVM#getOtherVMs(int)}.
     *
     * @throws ConnectionException
     * @throws InterruptedException
     */
    @Test
    public final void testGetOtherVMs1() throws ConnectionException, InterruptedException {
        testGetOtherVMsX(1);
    }

    /**
     * Test method for {@link ScalarisVM#getOtherVMs(int)}.
     *
     * @throws ConnectionException
     * @throws InterruptedException
     */
    @Test
    public final void testGetOtherVMs2() throws ConnectionException, InterruptedException {
        testGetOtherVMsX(2);
    }

    /**
     * Test method for {@link ScalarisVM#getOtherVMs(int)}.
     *
     * @throws ConnectionException
     * @throws InterruptedException
     */
    @Test
    public final void testGetOtherVMs3() throws ConnectionException, InterruptedException {
        testGetOtherVMsX(3);
    }

    private final void testGetOtherVMsX(final int max) throws ConnectionException, InterruptedException {
        final ScalarisVM conn = new ScalarisVM(scalarisNode);
        try {
            final List<String> result = conn.getOtherVMs(max);
            assertTrue("list too long: " + result.toString(), result.size() <= max);
            for (final String node : result) {
                final ScalarisVM conn2 = new ScalarisVM(node);
                try {
                    conn2.getInfo();
                } finally {
                    conn2.closeConnection();
                }
            }
        } finally {
            conn.closeConnection();
        }
    }

    /**
     * Test method for {@link ScalarisVM#shutdownVM()} with a closed connection.
     *
     * @throws ConnectionException
     */
    @Test(expected=ConnectionException.class)
    public final void testShutdownVM_NotConnected() throws ConnectionException {
        final ScalarisVM conn = new ScalarisVM(scalarisNode);
        conn.closeConnection();
        conn.shutdownVM();
    }

    /**
     * Test method for {@link ScalarisVM#shutdownVM()}.
     *
     * @throws ConnectionException
     */
    @Ignore("we still need the Scalaris Erlang VM")
    @Test
    public final void testShutdownVM1() throws ConnectionException {
        final ScalarisVM conn = new ScalarisVM(scalarisNode);
        conn.shutdownVM();
    }

    /**
     * Test method for {@link ScalarisVM#killVM()} with a closed connection.
     *
     * @throws ConnectionException
     */
    @Test(expected=ConnectionException.class)
    public final void testKillVM_NotConnected() throws ConnectionException {
        final ScalarisVM conn = new ScalarisVM(scalarisNode);
        conn.closeConnection();
        conn.killVM();
    }

    /**
     * Test method for {@link ScalarisVM#killVM()}.
     *
     * @throws ConnectionException
     */
    @Ignore("we still need the Scalaris Erlang VM")
    @Test
    public final void testKillVM1() throws ConnectionException {
        final ScalarisVM conn = new ScalarisVM(scalarisNode);
        conn.killVM();
    }
}
