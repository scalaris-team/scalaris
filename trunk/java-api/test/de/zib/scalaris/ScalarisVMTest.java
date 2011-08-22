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
import java.util.List;
import java.util.Map;

import org.junit.Ignore;
import org.junit.Test;

import de.zib.scalaris.ScalarisVM.AddNodesResult;
import de.zib.scalaris.ScalarisVM.DeleteNodesResult;

/**
 * Test class for {@link ScalarisVM}.
 *
 * @author Nico Kruber, kruber@zib.de
 * @version 3.6
 * @since 3.6
 */
@Ignore
public class ScalarisVMTest {
    private static final int MAX_WAIT_FOR_VM_SIZE = 30000;

    protected enum DeleteAction {
        SHUTDOWN, KILL
    }

    static {
        // set not to automatically try reconnects (auto-retries prevent ConnectionException tests from working):
        ((DefaultConnectionPolicy) ConnectionFactory.getInstance().getConnectionPolicy()).setMaxRetries(0);
    }

    /**
     * Test method for {@link de.zib.scalaris.ScalarisVM#ScalarisVM()}.
     *
     * @throws ConnectionException
     */
    @Test
    public final void testScalarisVM1() throws ConnectionException {
        final ScalarisVM conn = new ScalarisVM();
        conn.closeConnection();
    }

    /**
     * Test method for
     * {@link de.zib.scalaris.ScalarisVM#ScalarisVM(de.zib.scalaris.Connection)}
     * .
     *
     * @throws ConnectionException
     */
    @Test
    public final void testScalarisVM2() throws ConnectionException {
        final ScalarisVM conn = new ScalarisVM(ConnectionFactory.getInstance().createConnection("test"));
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
        final ScalarisVM conn = new ScalarisVM();
        conn.closeConnection();
        conn.closeConnection();
    }

    /**
     * Test method for {@link de.zib.scalaris.ScalarisVM#getVersion()} with a
     * closed connection.
     *
     * @throws ConnectionException
     */
    @Test(expected=ConnectionException.class)
    public final void testGetVersion_NotConnected() throws ConnectionException {
        final ScalarisVM conn = new ScalarisVM();
        conn.closeConnection();
        conn.getVersion();
    }

    /**
     * Test method for {@link de.zib.scalaris.ScalarisVM#getVersion()}.
     *
     * @throws ConnectionException
     */
    @Test
    public final void testGetVersion1() throws ConnectionException {
        final ScalarisVM conn = new ScalarisVM();
        final String version = conn.getVersion();
        assertTrue(!version.isEmpty());
    }

    /**
     * Test method for {@link de.zib.scalaris.ScalarisVM#getInfo()} with a
     * closed connection.
     *
     * @throws ConnectionException
     */
    @Test(expected=ConnectionException.class)
    public final void testGetInfo_NotConnected() throws ConnectionException {
        final ScalarisVM conn = new ScalarisVM();
        conn.closeConnection();
        conn.getInfo();
    }

    /**
     * Test method for {@link de.zib.scalaris.ScalarisVM#getInfo()}.
     *
     * @throws ConnectionException
     */
    @Test
    public final void testGetInfo1() throws ConnectionException {
        final ScalarisVM conn = new ScalarisVM();
        final Map<String, Object> info = conn.getInfo();
        assertTrue(!info.isEmpty());
        assertEquals(String.class, info.get("scalaris_version").getClass());
        assertEquals(String.class, info.get("erlang_version").getClass());
        assertEquals(Integer.class, info.get("mem_total").getClass());
        assertEquals(Integer.class, info.get("uptime").getClass());
        assertTrue("mem_total >= 0", (Integer) info.get("mem_total") >= 0);
        assertTrue("uptime >= 0", (Integer) info.get("uptime") >= 0);
    }

    /**
     * Test method for {@link de.zib.scalaris.ScalarisVM#getNumberOfNodes()}
     * with a closed connection.
     *
     * @throws ConnectionException
     */
    @Test(expected=ConnectionException.class)
    public final void testGetNumberOfNodes_NotConnected() throws ConnectionException {
        final ScalarisVM conn = new ScalarisVM();
        conn.closeConnection();
        conn.getNumberOfNodes();
    }

    /**
     * Test method for {@link de.zib.scalaris.ScalarisVM#getNumberOfNodes()}.
     * @throws ConnectionException
     */
    @Test
    public final void testGetNumberOfNodes1() throws ConnectionException {
        final ScalarisVM conn = new ScalarisVM();
        final int numberOfNodes = conn.getNumberOfNodes();
        assertTrue(numberOfNodes >= 0);
    }

    /**
     * Test method for {@link de.zib.scalaris.ScalarisVM#getNodes()}
     * with a closed connection.
     *
     * @throws ConnectionException
     */
    @Test(expected=ConnectionException.class)
    public final void testGetNodes_NotConnected() throws ConnectionException {
        final ScalarisVM conn = new ScalarisVM();
        conn.closeConnection();
        conn.getNodes();
    }

    /**
     * Test method for {@link de.zib.scalaris.ScalarisVM#getNodes()}.
     *
     * @throws ConnectionException
     */
    @Test
    public final void testGetNodes1() throws ConnectionException {
        final ScalarisVM conn = new ScalarisVM();
        final List<String> nodes = conn.getNodes();
        assertTrue(nodes.size() >= 0);
        assertEquals(conn.getNumberOfNodes(), nodes.size());
    }

    private final void waitForVMSize(ScalarisVM conn, int expSize)
            throws InterruptedException, ConnectionException, UnknownException {
        int size;
        int waitedMs = 0;
        do {
            Thread.sleep(100);
            waitedMs += 100;
            size = conn.getNumberOfNodes();
        } while (size != expSize && waitedMs <= MAX_WAIT_FOR_VM_SIZE);
        if (waitedMs >= MAX_WAIT_FOR_VM_SIZE) {
            fail("waited for size " + expSize + " but only reached size "
                    + size + " in " + MAX_WAIT_FOR_VM_SIZE + "ms");
        }
    }

    /**
     * Test method for {@link de.zib.scalaris.ScalarisVM#addNodes(int)}
     * with a closed connection.
     *
     * @throws ConnectionException
     * @throws InterruptedException
     */
    @Test(expected=ConnectionException.class)
    public final void testAddNodes_NotConnected() throws ConnectionException, InterruptedException {
        final ScalarisVM conn = new ScalarisVM();
        int size = conn.getNumberOfNodes();
        conn.closeConnection();
        conn.addNodes(1);
        // should not get here...but if, then wait for the node to fully join
        waitForVMSize(conn, size + 1);
    }

    /**
     * Test method for {@link de.zib.scalaris.ScalarisVM#addNodes(int)}.
     *
     * @throws ConnectionException
     * @throws InterruptedException
     */
    @Test
    public final void testAddNodes0() throws ConnectionException, InterruptedException {
        testAddNodesX(0);
    }

    /**
     * Test method for {@link de.zib.scalaris.ScalarisVM#addNodes(int)}.
     *
     * @throws ConnectionException
     * @throws InterruptedException
     */
    @Test
    public final void testAddNodes1() throws ConnectionException, InterruptedException {
        testAddNodesX(1);
    }

    /**
     * Test method for {@link de.zib.scalaris.ScalarisVM#addNodes(int)}.
     *
     * @throws ConnectionException
     * @throws InterruptedException
     */
    @Test
    public final void testAddNodes3() throws ConnectionException, InterruptedException {
        testAddNodesX(3);
    }

    /**
     * Test method for
     * {@link de.zib.scalaris.ScalarisVM#shutdownNodes(int)} and
     * {@link de.zib.scalaris.ScalarisVM#killNodes(int)}.
     *
     * @throws ConnectionException
     * @throws InterruptedException
     */
    private final void testAddNodesX(int nodesToAdd) throws ConnectionException, InterruptedException {
        final ScalarisVM conn = new ScalarisVM();
        int size = conn.getNumberOfNodes();
        final AddNodesResult addedNodes = conn.addNodes(nodesToAdd);
        size += nodesToAdd;
        assertEquals(nodesToAdd, addedNodes.successful.size());
        assertTrue(addedNodes.errors.isEmpty());
        waitForVMSize(conn, size);
        final List<String> nodes = conn.getNodes();
        for (final String name : addedNodes.successful) {
            assertTrue(nodes.toString() + " should contain " + name, nodes.contains(name));
        }
        for (final String name : addedNodes.successful) {
            conn.killNode(name);
        }
        size -= nodesToAdd;
        waitForVMSize(conn, size);
    }

    /**
     * Test method for
     * {@link de.zib.scalaris.ScalarisVM#shutdownNode(java.lang.String)} with a
     * closed connection.
     *
     * @throws ConnectionException
     * @throws InterruptedException
     */
    @Test(expected=ConnectionException.class)
    public final void testShutdownNode_NotConnected() throws ConnectionException, InterruptedException {
        final ScalarisVM conn = new ScalarisVM();
        int size = conn.getNumberOfNodes();
        conn.closeConnection();
        conn.shutdownNode("test");
        // should not get here...but if, then wait for the correct ring size
        // note: there should not be a node named "test"
        waitForVMSize(conn, size);
    }

    /**
     * Test method for
     * {@link de.zib.scalaris.ScalarisVM#shutdownNode(java.lang.String)}.
     *
     * @throws ConnectionException
     * @throws InterruptedException
     */
    @Test
    public final void testShutdownNode1() throws ConnectionException, InterruptedException {
        testDeleteNode(DeleteAction.SHUTDOWN);
    }

    /**
     * Test method for
     * {@link de.zib.scalaris.ScalarisVM#killNode(java.lang.String)} with a
     * closed connection.
     *
     * @throws ConnectionException
     * @throws InterruptedException
     */
    @Test(expected=ConnectionException.class)
    public final void testKillNode_NotConnected() throws ConnectionException, InterruptedException {
        final ScalarisVM conn = new ScalarisVM();
        int size = conn.getNumberOfNodes();
        conn.closeConnection();
        conn.killNode("test");
        // should not get here...but if, then wait for the correct ring size
        // note: there should not be a node named "test"
        waitForVMSize(conn, size);
    }

    /**
     * Test method for
     * {@link de.zib.scalaris.ScalarisVM#killNode(java.lang.String)}.
     *
     * @throws ConnectionException
     * @throws InterruptedException
     */
    @Test
    public final void testKillNode1() throws ConnectionException, InterruptedException {
        testDeleteNode(DeleteAction.KILL);
    }

    /**
     * Test method for
     * {@link de.zib.scalaris.ScalarisVM#shutdownNode(String)} and
     * {@link de.zib.scalaris.ScalarisVM#killNode(String)}.
     *
     * @throws ConnectionException
     * @throws InterruptedException
     */
    private final void testDeleteNode(DeleteAction action) throws ConnectionException, InterruptedException {
        final ScalarisVM conn = new ScalarisVM();
        final int size = conn.getNumberOfNodes();
        final String name = conn.addNodes(1).successful.get(0);
        waitForVMSize(conn, size + 1);
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
        waitForVMSize(conn, size);
        final List<String> nodes = conn.getNodes();
        assertTrue(nodes.toString() + " should not contain " + name, !nodes.contains(name));
    }

    /**
     * Test method for
     * {@link de.zib.scalaris.ScalarisVM#shutdownNodes(int)} with a
     * closed connection.
     *
     * @throws ConnectionException
     * @throws InterruptedException
     */
    @Test(expected=ConnectionException.class)
    public final void testShutdownNodes_NotConnected() throws ConnectionException, InterruptedException {
        final ScalarisVM conn = new ScalarisVM();
        final int size = conn.getNumberOfNodes();
        conn.closeConnection();
        conn.shutdownNodes(1);
        // should not get here...but if, then wait for the correct ring size
        waitForVMSize(conn, size - 1);
    }

    /**
     * Test method for {@link de.zib.scalaris.ScalarisVM#shutdownNodes(int)}.
     *
     * @throws ConnectionException
     * @throws InterruptedException
     */
    @Test
    public final void testShutdownNodes0() throws ConnectionException, InterruptedException {
        testDeleteNodesX(0, DeleteAction.SHUTDOWN);
    }

    /**
     * Test method for {@link de.zib.scalaris.ScalarisVM#shutdownNodes(int)}.
     *
     * @throws ConnectionException
     * @throws InterruptedException
     */
    @Test
    public final void testShutdownNodes1() throws ConnectionException, InterruptedException {
        testDeleteNodesX(1, DeleteAction.SHUTDOWN);
    }

    /**
     * Test method for {@link de.zib.scalaris.ScalarisVM#shutdownNodes(int)}.
     *
     * @throws ConnectionException
     * @throws InterruptedException
     */
    @Test
    public final void testShutdownNodes3() throws ConnectionException, InterruptedException {
        testDeleteNodesX(3, DeleteAction.SHUTDOWN);
    }

    /**
     * Test method for
     * {@link de.zib.scalaris.ScalarisVM#killNodes(int)} with a
     * closed connection.
     *
     * @throws ConnectionException
     * @throws InterruptedException
     */
    @Test(expected=ConnectionException.class)
    public final void testKillNodes_NotConnected() throws ConnectionException, InterruptedException {
        final ScalarisVM conn = new ScalarisVM();
        final int size = conn.getNumberOfNodes();
        conn.closeConnection();
        conn.killNodes(1);
        // should not get here...but if, then wait for the correct ring size
        waitForVMSize(conn, size - 1);
    }

    /**
     * Test method for {@link de.zib.scalaris.ScalarisVM#killNodes(int)}.
     *
     * @throws ConnectionException
     * @throws InterruptedException
     */
    @Test
    public final void testKillNodes0() throws ConnectionException, InterruptedException {
        testDeleteNodesX(0, DeleteAction.KILL);
    }

    /**
     * Test method for {@link de.zib.scalaris.ScalarisVM#killNodes(int)}.
     *
     * @throws ConnectionException
     * @throws InterruptedException
     */
    @Test
    public final void testKillNodes1() throws ConnectionException, InterruptedException {
        testDeleteNodesX(1, DeleteAction.KILL);
    }

    /**
     * Test method for {@link de.zib.scalaris.ScalarisVM#killNodes(int)}.
     *
     * @throws ConnectionException
     * @throws InterruptedException
     */
    @Test
    public final void testKillNodes3() throws ConnectionException, InterruptedException {
        testDeleteNodesX(3, DeleteAction.KILL);
    }

    /**
     * Test method for
     * {@link de.zib.scalaris.ScalarisVM#shutdownNodes(int)} and
     * {@link de.zib.scalaris.ScalarisVM#killNodes(int)}.
     *
     * @throws ConnectionException
     * @throws InterruptedException
     */
    private final void testDeleteNodesX(final int nodesToRemove, DeleteAction action) throws ConnectionException, InterruptedException {
        final ScalarisVM conn = new ScalarisVM();
        final int size = conn.getNumberOfNodes();
        if (nodesToRemove >= 1) {
            conn.addNodes(nodesToRemove);
            waitForVMSize(conn, size + nodesToRemove);
        }
        DeleteNodesResult result = null;
        switch (action) {
            case SHUTDOWN:
                result = conn.shutdownNodes(nodesToRemove);
                break;
            case KILL:
                result = conn.killNodes(nodesToRemove);
                break;
        }
        assertEquals(nodesToRemove, result.successful.size());
        assertEquals(0, result.notFound.size());
        waitForVMSize(conn, size);
        final List<String> nodes = conn.getNodes();
        for (final String name : result.successful) {
            assertTrue(nodes.toString() + " should not contain " + name, !nodes.contains(name));
        }
    }

    /**
     * Test method for
     * {@link de.zib.scalaris.ScalarisVM#shutdownNodes(java.util.List)} with a
     * closed connection.
     *
     * @throws ConnectionException
     * @throws InterruptedException
     */
    @Test(expected=ConnectionException.class)
    public final void testShutdownNodesList_NotConnected() throws ConnectionException, InterruptedException {
        final ScalarisVM conn = new ScalarisVM();
        int size = conn.getNumberOfNodes();
        conn.closeConnection();
        conn.shutdownNodes(Arrays.asList("test"));
        // should not get here...but if, then wait for the correct ring size
        // note: there should not be a node named "test"
        waitForVMSize(conn, size);
    }

    /**
     * Test method for
     * {@link de.zib.scalaris.ScalarisVM#shutdownNodes(java.util.List)}.
     *
     * @throws ConnectionException
     * @throws InterruptedException
     */
    @Test
    public final void testShutdownNodesList0() throws ConnectionException, InterruptedException {
        testDeleteNodesListX(0, DeleteAction.SHUTDOWN);
    }

    /**
     * Test method for
     * {@link de.zib.scalaris.ScalarisVM#shutdownNodes(java.util.List)}.
     *
     * @throws ConnectionException
     * @throws InterruptedException
     */
    @Test
    public final void testShutdownNodesList1() throws ConnectionException, InterruptedException {
        testDeleteNodesListX(1, DeleteAction.SHUTDOWN);
    }

    /**
     * Test method for
     * {@link de.zib.scalaris.ScalarisVM#shutdownNodes(java.util.List)}.
     *
     * @throws ConnectionException
     * @throws InterruptedException
     */
    @Test
    public final void testShutdownNodesList3() throws ConnectionException, InterruptedException {
        testDeleteNodesListX(3, DeleteAction.SHUTDOWN);
    }

    /**
     * Test method for
     * {@link de.zib.scalaris.ScalarisVM#killNodes(java.util.List)} with a
     * closed connection.
     *
     * @throws ConnectionException
     * @throws InterruptedException
     */
    @Test(expected=ConnectionException.class)
    public final void testKillNodesList_NotConnected() throws ConnectionException, InterruptedException {
        final ScalarisVM conn = new ScalarisVM();
        int size = conn.getNumberOfNodes();
        conn.closeConnection();
        conn.killNodes(Arrays.asList("test"));
        // should not get here...but if, then wait for the correct ring size
        // note: there should not be a node named "test"
        waitForVMSize(conn, size);
    }

    /**
     * Test method for
     * {@link de.zib.scalaris.ScalarisVM#killNodes(java.util.List)}.
     *
     * @throws ConnectionException
     * @throws InterruptedException
     */
    @Test
    public final void testKillNodesList0() throws ConnectionException, InterruptedException {
        testDeleteNodesListX(0, DeleteAction.KILL);
    }

    /**
     * Test method for
     * {@link de.zib.scalaris.ScalarisVM#killNodes(java.util.List)}.
     *
     * @throws ConnectionException
     * @throws InterruptedException
     */
    @Test
    public final void testKillNodesList1() throws ConnectionException, InterruptedException {
        testDeleteNodesListX(1, DeleteAction.KILL);
    }

    /**
     * Test method for
     * {@link de.zib.scalaris.ScalarisVM#killNodes(java.util.List)}.
     *
     * @throws ConnectionException
     * @throws InterruptedException
     */
    @Test
    public final void testKillNodesList3() throws ConnectionException, InterruptedException {
        testDeleteNodesListX(3, DeleteAction.KILL);
    }

    /**
     * Test method for
     * {@link de.zib.scalaris.ScalarisVM#shutdownNodes(java.util.List)} and
     * {@link de.zib.scalaris.ScalarisVM#killNodes(java.util.List)}.
     *
     * @throws ConnectionException
     * @throws InterruptedException
     */
    private final void testDeleteNodesListX(final int nodesToRemove, DeleteAction action) throws ConnectionException, InterruptedException {
        final ScalarisVM conn = new ScalarisVM();
        final int size = conn.getNumberOfNodes();
        if (nodesToRemove >= 1) {
            conn.addNodes(nodesToRemove);
            waitForVMSize(conn, size + nodesToRemove);
        }
        List<String> nodes = conn.getNodes();
        final List<String> removedNodes = nodes.subList(nodes.size() - nodesToRemove, nodes.size());
        DeleteNodesResult result = null;
        switch (action) {
            case SHUTDOWN:
                result = conn.shutdownNodes(removedNodes);
                break;
            case KILL:
                result = conn.killNodes(removedNodes);
                break;
        }
        assertEquals(nodesToRemove, result.successful.size());
        assertEquals(0, result.notFound.size());
        assertEquals(removedNodes, result.successful);
        waitForVMSize(conn, size);
        nodes = conn.getNodes();
        for (final String name : result.successful) {
            assertTrue(nodes.toString() + " should not contain " + name, !nodes.contains(name));
        }
    }

    /**
     * Test method for {@link de.zib.scalaris.ScalarisVM#getOtherVMs(int)} with
     * a closed connection.
     *
     * @throws ConnectionException
     * @throws InterruptedException
     */
    @Test(expected=ConnectionException.class)
    public final void testGetOtherVMs_NotConnected() throws ConnectionException, InterruptedException {
        ScalarisVM conn = new ScalarisVM();
        conn.closeConnection();
        conn.getOtherVMs(1);
    }

    /**
     * Test method for {@link de.zib.scalaris.ScalarisVM#getOtherVMs(int)}.
     *
     * @throws ConnectionException
     * @throws InterruptedException
     */
    @Test
    public final void testGetOtherVMs1() throws ConnectionException, InterruptedException {
        testGetOtherVMsX(1);
    }

    /**
     * Test method for {@link de.zib.scalaris.ScalarisVM#getOtherVMs(int)}.
     *
     * @throws ConnectionException
     * @throws InterruptedException
     */
    @Test
    public final void testGetOtherVMs2() throws ConnectionException, InterruptedException {
        testGetOtherVMsX(2);
    }

    /**
     * Test method for {@link de.zib.scalaris.ScalarisVM#getOtherVMs(int)}.
     *
     * @throws ConnectionException
     * @throws InterruptedException
     */
    @Test
    public final void testGetOtherVMs3() throws ConnectionException, InterruptedException {
        testGetOtherVMsX(3);
    }

    private final void testGetOtherVMsX(int max) throws ConnectionException, InterruptedException {
        ScalarisVM conn = new ScalarisVM();
        List<String> result = conn.getOtherVMs(max);
        assertTrue("list too long:" + result.toString(), result.size() <= max);
        ConnectionFactory cf = new ConnectionFactory();
        for (String node : result) {
            cf.setNode(node);
            ScalarisVM conn2 = new ScalarisVM(cf.createConnection());
            conn2.getInfo();
        }
        conn.closeConnection();
    }

    /**
     * Test method for {@link de.zib.scalaris.ScalarisVM#shutdownVM()} with a
     * closed connection.
     *
     * @throws ConnectionException
     */
    @Test(expected=ConnectionException.class)
    public final void testShutdownVM_NotConnected() throws ConnectionException {
        final ScalarisVM conn = new ScalarisVM();
        conn.closeConnection();
        conn.shutdownVM();
    }

    /**
     * Test method for {@link de.zib.scalaris.ScalarisVM#shutdownVM()}.
     *
     * @throws ConnectionException
     */
    @Ignore("we still need the Scalaris Erlang VM")
    @Test
    public final void testShutdownVM1() throws ConnectionException {
        final ScalarisVM conn = new ScalarisVM();
        conn.shutdownVM();
    }

    /**
     * Test method for {@link de.zib.scalaris.ScalarisVM#killVM()} with a
     * closed connection.
     *
     * @throws ConnectionException
     */
    @Test(expected=ConnectionException.class)
    public final void testKillVM_NotConnected() throws ConnectionException {
        final ScalarisVM conn = new ScalarisVM();
        conn.closeConnection();
        conn.killVM();
    }

    /**
     * Test method for {@link de.zib.scalaris.ScalarisVM#killVM()}.
     *
     * @throws ConnectionException
     */
    @Ignore("we still need the Scalaris Erlang VM")
    @Test
    public final void testKillVM1() throws ConnectionException {
        final ScalarisVM conn = new ScalarisVM();
        conn.killVM();
    }
}
