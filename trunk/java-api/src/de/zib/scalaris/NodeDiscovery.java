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
     * Connection Factory to work with.
     */
    protected final ConnectionFactory cf;
    /**
     * Maximum number of nodes that should remain in the
     * {@link ConnectionFactory} {@link #cf}.
     */
    protected final int maxNodes;

    /**
     * Constructor
     *
     * @param cf
     *            the {@link ConnectionFactory} to add nodes to / remove nodes
     *            from
     * @param maxNodes
     *            maximum number of nodes to keep track of
     */
    public NodeDiscovery(final ConnectionFactory cf, final int maxNodes) {
        this.cf = cf;
        this.maxNodes = maxNodes;
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

            // first, remove nodes with failed connection attempts to make room for the new nodes:
            // sort existing nodes, least failed connection attempts first
            Collections.sort(existingNodes, new LeastFailedNodesComparator());
            int failedNodesToRemove;
            if (otherVms.size() > maxNodes) {
                failedNodesToRemove = existingNodes.size();
            } else if ((existingNodes.size() + otherVms.size()) <= maxNodes) {
                failedNodesToRemove = 0;
            } else {
                failedNodesToRemove = (existingNodes.size() + otherVms.size()) - maxNodes;
            }
            assert failedNodesToRemove >= 0;
            assert failedNodesToRemove <= existingNodes.size();

            int lastNodeIdx = existingNodes.size() - 1;
            for (; lastNodeIdx >= (existingNodes.size() - failedNodesToRemove); --lastNodeIdx) {
                final PeerNode node = existingNodes.get(lastNodeIdx);
                if (node.getFailureCount() > 0) {
                    cf.removeNode(node);
                } else {
                    break;
                }
            }
            // note: existingNodes still contains the removed nodes here!

            // then add new nodes (not more than maxNodes number of available nodes!)
            final int remainingNodes = lastNodeIdx + 1;
            for (int i = 0; (i < (maxNodes - remainingNodes)) && (i < otherVms.size()); ++i) {
                cf.addNode(otherVms.get(i));
            }
        } catch (final ConnectionException e) {
            e.printStackTrace();
        }
    }
}
