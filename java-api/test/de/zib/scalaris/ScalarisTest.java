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

import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Test;

/**
 * Test class for {@link Scalaris}.
 *
 * @author Nico Kruber, kruber@zib.de
 * @version 3.10
 * @since 3.10
 */
public class ScalarisTest {

    static {
        // determine good/bad nodes:
        final ConnectionFactory cf = ConnectionFactory.getInstance();
        cf.testAllNodes();
        // set not to automatically try reconnects (auto-retries prevent ConnectionException tests from working):
        ((DefaultConnectionPolicy) cf.getConnectionPolicy()).setMaxRetries(0);
    }

    /**
     * Test method for {@link de.zib.scalaris.Scalaris#Scalaris()}.
     *
     * @throws ConnectionException
     */
    @Test
    public final void testScalaris1() throws ConnectionException {
        final Scalaris conn = new Scalaris();
        conn.closeConnection();
    }

    /**
     * Test method for
     * {@link de.zib.scalaris.Scalaris#Scalaris(de.zib.scalaris.Connection)}
     * .
     *
     * @throws ConnectionException
     */
    @Test
    public final void testScalaris2() throws ConnectionException {
        final Scalaris conn = new Scalaris(ConnectionFactory.getInstance().createConnection("test"));
        conn.closeConnection();
    }

    /**
     * Test method for {@link Scalaris#closeConnection()} trying to close the
     * connection twice.
     *
     * @throws ConnectionException
     */
    @Test
    public void testDoubleClose() throws ConnectionException {
        final Scalaris conn = new Scalaris();
        conn.closeConnection();
        conn.closeConnection();
    }

    /**
     * Test method for {@link de.zib.scalaris.Scalaris#getRandomNodes(int)} with
     * a closed connection.
     *
     * @throws ConnectionException
     * @throws InterruptedException
     */
    @Test(expected=ConnectionException.class)
    public final void testGetRandomNodes_NotConnected() throws ConnectionException, InterruptedException {
        final Scalaris conn = new Scalaris();
        conn.closeConnection();
        conn.getRandomNodes(1);
    }

    /**
     * Test method for {@link de.zib.scalaris.Scalaris#getRandomNodes(int)}.
     *
     * @throws ConnectionException
     * @throws InterruptedException
     */
    @Test
    public final void testGetRandomNodes1() throws ConnectionException, InterruptedException {
        testGetRandomNodesX(1);
    }

    /**
     * Test method for {@link de.zib.scalaris.Scalaris#getRandomNodes(int)}.
     *
     * @throws ConnectionException
     * @throws InterruptedException
     */
    @Test
    public final void testGetRandomNodes2() throws ConnectionException, InterruptedException {
        testGetRandomNodesX(2);
    }

    /**
     * Test method for {@link de.zib.scalaris.Scalaris#getRandomNodes(int)}.
     *
     * @throws ConnectionException
     * @throws InterruptedException
     */
    @Test
    public final void testGetRandomNodes3() throws ConnectionException, InterruptedException {
        testGetRandomNodesX(3);
    }

    private final void testGetRandomNodesX(final int max) throws ConnectionException, InterruptedException {
        final Scalaris conn = new Scalaris();
        try {
            final List<String> result = conn.getRandomNodes(max);
            assertTrue("list empty:" + result.toString(), !result.isEmpty());
            assertTrue("list too long:" + result.toString(), result.size() <= max);
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
}
