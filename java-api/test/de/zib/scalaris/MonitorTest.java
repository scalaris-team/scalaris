/**
 *  Copyright 2012 Zuse Institute Berlin
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

import java.util.Map.Entry;

import org.junit.Test;

import de.zib.scalaris.Monitor.GetNodeInfoResult;
import de.zib.scalaris.Monitor.GetNodePerformanceResult;
import de.zib.scalaris.Monitor.GetServiceInfoResult;
import de.zib.scalaris.Monitor.GetServicePerformanceResult;

/**
 * Unit tests for the {@link Monitor} class.
 *
 * @author Nico Kruber, kruber@zib.de
 * @since 3.11
 */
public class MonitorTest {

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
     * Test method for {@link Monitor#Monitor(String)}.
     *
     * @throws ConnectionException
     */
    @Test
    public final void testMonitor() throws ConnectionException {
        final Monitor conn = new Monitor(scalarisNode);
        conn.closeConnection();
    }

    /**
     * Test method for {@link Monitor#closeConnection()} trying to close the
     * connection twice.
     *
     * @throws ConnectionException
     */
    @Test
    public void testDoubleClose() throws ConnectionException {
        final Monitor conn = new Monitor(scalarisNode);
        conn.closeConnection();
        conn.closeConnection();
    }

    /**
     * Test method for {@link Monitor#getNodeInfo()} with a closed connection.
     *
     * @throws ConnectionException
     */
    @Test(expected = ConnectionException.class)
    public final void testGetNodeInfo_NotConnected() throws ConnectionException {
        final Monitor conn = new Monitor(scalarisNode);
        conn.closeConnection();
        conn.getNodeInfo();
    }

    /**
     * Test method for {@link Monitor#getNodeInfo()}.
     *
     * @throws ConnectionException
     */
    @Test
    public final void testGetNodeInfo1() throws ConnectionException {
        final Monitor conn = new Monitor(scalarisNode);
        try {
            final GetNodeInfoResult nodeInfo = conn.getNodeInfo();
            assertTrue(nodeInfo.dhtNodes >= 0);
            assertTrue(!nodeInfo.scalarisVersion.isEmpty());
            assertTrue(!nodeInfo.erlangVersion.isEmpty());
        } finally {
            conn.closeConnection();
        }
    }

    /**
     * Test method for {@link Monitor#getNodePerformance()} with a closed
     * connection.
     *
     * @throws ConnectionException
     */
    @Test(expected = ConnectionException.class)
    public final void testGetNodePerformance_NotConnected()
            throws ConnectionException {
        final Monitor conn = new Monitor(scalarisNode);
        conn.closeConnection();
        conn.getNodePerformance();
    }

    /**
     * Test method for {@link Monitor#getNodePerformance()}.
     *
     * @throws ConnectionException
     */
    @Test
    public final void testGetNodePerformance1() throws ConnectionException {
        final Monitor conn = new Monitor(scalarisNode);
        try {
            final GetNodePerformanceResult nodePerformance = conn.getNodePerformance();
            for (final Entry<Long, Double> latencyAvg : nodePerformance.latencyAvg.entrySet()) {
                assertTrue(latencyAvg.toString(), latencyAvg.getKey() >= 0);
                assertTrue(latencyAvg.toString(), latencyAvg.getValue() >= 0);
            }
            for (final Entry<Long, Double> latencyStddev : nodePerformance.latencyStddev.entrySet()) {
                assertTrue(latencyStddev.toString(), latencyStddev.getKey() >= 0);
                assertTrue(latencyStddev.toString(), latencyStddev.getValue() >= 0);
            }
        } finally {
            conn.closeConnection();
        }
    }

    /**
     * Test method for {@link Monitor#getServiceInfo()} with a closed
     * connection.
     *
     * @throws ConnectionException
     */
    @Test(expected = ConnectionException.class)
    public final void testGetServiceInfo_NotConnected()
            throws ConnectionException {
        final Monitor conn = new Monitor(scalarisNode);
        conn.closeConnection();
        conn.getServiceInfo();
    }

    /**
     * Test method for {@link Monitor#getServiceInfo()}.
     *
     * @throws ConnectionException
     */
    @Test
    public final void testGetServiceInfo1() throws ConnectionException {
        final Monitor conn = new Monitor(scalarisNode);
        try {
            final GetServiceInfoResult serviceInfo = conn.getServiceInfo();
            assertTrue(serviceInfo.nodes >= 0);
            assertTrue(serviceInfo.totalLoad >= 0);
        } finally {
            conn.closeConnection();
        }
    }

    /**
     * Test method for {@link Monitor#getServicePerformance()} with a closed
     * connection.
     *
     * @throws ConnectionException
     */
    @Test(expected = ConnectionException.class)
    public final void testGetServicePerformance_NotConnected()
            throws ConnectionException {
        final Monitor conn = new Monitor(scalarisNode);
        conn.closeConnection();
        conn.getServicePerformance();
    }

    /**
     * Test method for {@link Monitor#getServicePerformance()}.
     *
     * @throws ConnectionException
     */
    @Test
    public final void testGetServicePerformance1() throws ConnectionException {
        final Monitor conn = new Monitor(scalarisNode);
        try {
            final GetServicePerformanceResult nodePerformance = conn.getServicePerformance();
            for (final Entry<Long, Double> latencyAvg : nodePerformance.latencyAvg.entrySet()) {
                assertTrue(latencyAvg.toString(), latencyAvg.getKey() >= 0);
                assertTrue(latencyAvg.toString(), latencyAvg.getValue() >= 0);
            }
            for (final Entry<Long, Double> latencyStddev : nodePerformance.latencyStddev.entrySet()) {
                assertTrue(latencyStddev.toString(), latencyStddev.getKey() >= 0);
                assertTrue(latencyStddev.toString(), latencyStddev.getValue() >= 0);
            }
        } finally {
            conn.closeConnection();
        }
    }
}
