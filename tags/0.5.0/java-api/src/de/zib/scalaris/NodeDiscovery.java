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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Provides a node discovery service.
 *
 * When started with {@link #startWithFixedDelay(long)} or
 * {@link #startWithFixedDelay(long, long, TimeUnit)}, periodically connects to
 * a Scalaris node and gets information about other Scalaris nodes. These will
 * then be added to the given {@link ConnectionFactory} where old nodes with
 * connection failures will be removed in favour of newly discovered nodes.
 *
 * @author Nico Kruber, kruber@zib.de
 * @version 3.16
 * @since 3.16
 */
public class NodeDiscovery implements Runnable {
    /**
     * Handler that is invoked whenever a new node is found by the
     * {@link NodeDiscovery} thread.
     *
     * @author Nico Kruber, kruber@zib.de
     */
    public static interface NewNodeHandler {
        /**
         * Called if a new node is found.
         *
         * @param node
         *            the new node's name
         */
        public void newNodeFound(String node);
    }

    /**
     * {@link ConnectionFactory} to work with.
     */
    protected final ConnectionFactory cf;

    /**
     * If not null, connections currently in the pool to removed nodes will be
     * closed.
     */
    protected final ConnectionPool cPool;

    /**
     * Maximum number of nodes that should remain in the
     * {@link ConnectionFactory} {@link #cf}.
     */
    protected int maxNodes = 10;

    /**
     * Minimum time in seconds since the last successful connection for a node
     * to be removed in favour of newly discovered nodes.
     */
    protected int minAgeToRemove = 60;

    /**
     * Constructor
     *
     * @param cf
     *            the {@link ConnectionFactory} to add nodes to / remove nodes
     *            from
     */
    public NodeDiscovery(final ConnectionFactory cf) {
        this.cf = cf;
        this.cPool = null;
    }

    /**
     * Constructor
     *
     * @param cPool
     *            the {@link ConnectionPool} to interact with
     */
    public NodeDiscovery(final ConnectionPool cPool) {
        this.cf = cPool.getConnectionFactory();
        this.cPool = cPool;
    }

    /**
     * Starts the node discovery service at the given fixed delay.
     *
     * @param delay
     *            the delay between the termination of one execution and the
     *            commencement of the next
     */
    public void startWithFixedDelay(final long delay) {
        startWithFixedDelay(0, delay, TimeUnit.SECONDS);
    }

    /**
     * Starts the node discovery service at the given fixed delay.
     *
     * @param initialDelay
     *            the time to delay first execution
     * @param delay
     *            the delay between the termination of one execution and the
     *            commencement of the next
     * @param unit
     *            the time unit of the initialDelay and delay parameters
     */
    public void startWithFixedDelay(final long initialDelay,
            final long delay,
            final TimeUnit unit) {
        final ScheduledExecutorService ses = Executors.newScheduledThreadPool(1);
        ses.scheduleWithFixedDelay(this, initialDelay, delay, unit);
    }

    /**
     * Executed a single call to a known Scalaris node asking for other known
     * nodes. These nodes will then be added to the {@link ConnectionFactory}
     * {@link #cf}.
     */
    public void run() {
        try {
            final ScalarisVM vm = new ScalarisVM(cf.getConnectionPolicy().selectNode());
            final List<String> otherVms = vm.getOtherVMs(maxNodes);
            final List<PeerNode> existingNodes = cf.getNodes();

            // get a mapping of node names to PeerNode objects for faster access
            final HashMap<String, PeerNode> existingNodesMap = new HashMap<String, PeerNode>(existingNodes.size());
            for (final PeerNode node : existingNodes) {
                existingNodesMap.put(node.toString(), node);
            }
            // remove nodes already known
            for (final Iterator<String> iterator = otherVms.iterator(); iterator.hasNext();) {
                final String otherVm = iterator.next();
                if (existingNodesMap.containsKey(otherVm)) {
                    iterator.remove();
                }
            }

            if (otherVms.isEmpty()) {
                // no new nodes found...
                return;
            }

            final int remainingNodes = removeFailedNodes(existingNodes, otherVms);
            addNewNodes(existingNodes, otherVms, remainingNodes);
            if (cPool != null) {
                cPool.closeAllBut(new HashSet<PeerNode>(existingNodes));
            }
        } catch (final ConnectionException e) {
            e.printStackTrace();
        }
    }

    /**
     * Removes nodes with failed connection attempts, without any previous
     * connections, or with connections longer than minAgeToRemove seconds ago
     * from the {@link ConnectionFactory} {@link #cf} to make room for newly
     * discovered nodes.
     *
     * @param existingNodes
     *            existing Erlang VMs
     * @param otherVms
     *            newly discovered Erlang VMs not already in
     *            <tt>existingNodes</tt> (non-empty)
     *
     * @return number of remaining nodes in the {@link ConnectionFactory}
     *         {@link #cf}
     */
    protected int removeFailedNodes(final List<PeerNode> existingNodes,
            final List<String> otherVms) {
        // sort existing nodes, least failed connection attempts first
        Collections.sort(existingNodes, new LeastFailedNodesComparator());
        int failedNodesToRemove;
        final int existingNodesCount = existingNodes.size();
        if (otherVms.size() > maxNodes) {
            failedNodesToRemove = existingNodesCount;
        } else if ((existingNodesCount + otherVms.size()) <= maxNodes) {
            failedNodesToRemove = 0;
        } else {
            failedNodesToRemove = (existingNodesCount + otherVms.size()) - maxNodes;
        }
        assert failedNodesToRemove >= 0;
        assert failedNodesToRemove <= existingNodesCount;

        int lastNodeIdx = existingNodesCount - 1;
        for (; lastNodeIdx >= (existingNodesCount - failedNodesToRemove); --lastNodeIdx) {
            final PeerNode node = existingNodes.get(lastNodeIdx);
            if (  // failed connection?
                       (node.getFailureCount() > 0)
                  // never connected?
                    || (node.getLastConnectSuccess() == null)
                  // last connection longer than minAgeToRemove seconds ago?
                    || (node.getLastConnectSuccess().getTime() < (System
                            .currentTimeMillis() - (minAgeToRemove * 1000)))) {
                cf.removeNode(node);
                existingNodes.remove(lastNodeIdx);
            } else {
                break;
            }
        }
        return lastNodeIdx + 1;
    }

    /**
     * Adds newly discovered nodes to the {@link ConnectionFactory} {@link #cf}.
     *
     * @param existingNodes
     *            existing Erlang VMs
     * @param otherVms
     *            newly discovered Erlang VMs not already in
     *            <tt>existingNodes</tt> (non-empty)
     * @param remainingNodes
     *            number of remaining nodes in the {@link ConnectionFactory}
     *            {@link #cf}
     */
    protected void addNewNodes(final List<PeerNode> existingNodes,final List<String> otherVms,
            final int remainingNodes) {
        // then add new nodes (not more than maxNodes number of available nodes!)
        for (int i = 0; (i < (maxNodes - remainingNodes)) && (i < otherVms.size()); ++i) {
            final PeerNode p = new PeerNode(otherVms.get(i));
            cf.addNode(p);
            existingNodes.add(p);
        }
        // TODO: replace some more remaining nodes with newly discovered ones?
        // e.g. randomly select some to be replaced?
    }

    /**
     * Gets the maximum number of nodes that should remain in the
     * {@link ConnectionFactory} {@link #cf}.
     *
     * @return the maxNodes member
     */
    public final int getMaxNodes() {
        return maxNodes;
    }

    /**
     * Sets the maximum number of nodes that should remain in the
     * {@link ConnectionFactory} {@link #cf}.
     *
     * @param maxNodes
     *            the maxNodes to set
     */
    public final void setMaxNodes(final int maxNodes) {
        this.maxNodes = maxNodes;
    }

    /**
     * Gets the minimum time in seconds since the last successful connection for
     * a node to be removed in favour of newly discovered nodes.
     *
     * @return the minAgeToRemove member
     */
    public final int getMinAgeToRemove() {
        return minAgeToRemove;
    }

    /**
     * Sets the minimum time in seconds since the last successful connection for
     * a node to be removed in favour of newly discovered nodes.
     *
     * @param minAgeToRemove
     *            the minAgeToRemove to set
     */
    public final void setMinAgeToRemove(final int minAgeToRemove) {
        this.minAgeToRemove = minAgeToRemove;
    }

    /**
     * Gets the {@link ConnectionFactory} to work with.
     *
     * @return the connection factory
     */
    public final ConnectionFactory getCf() {
        return cf;
    }
}
