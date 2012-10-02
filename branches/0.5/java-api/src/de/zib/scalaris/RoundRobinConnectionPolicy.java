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

import java.util.List;

/**
 * Implements a {@link ConnectionPolicy} by choosing nodes round-robin.
 *
 * This implementation is based on {@link DefaultConnectionPolicy} and thus
 * also differentiates good and bad nodes.
 *
 * @author Nico Kruber, kruber@zib.de
 *
 * @version 3.5
 * @since 3.5
 *
 * @see DefaultConnectionPolicy
 */
public class RoundRobinConnectionPolicy extends DefaultConnectionPolicy {
    int nextNode = 0;

    /**
     * Creates a new connection policy working with the given remote node.
     *
     * Provided for convenience.
     *
     * Attention: This method also synchronises on the node.
     *
     * @param remoteNode the (only) available remote node
     */
    public RoundRobinConnectionPolicy(final PeerNode remoteNode) {
        super(remoteNode);
    }

    /**
     * Creates a new connection policy with the given remote nodes.
     *
     * Attention: This method synchronises on {@code availableRemoteNodes}.
     *
     * Any time this list is changed, the according methods in this class should
     * be called, i.e. {@link #availableNodeAdded(PeerNode)},
     * {@link #availableNodeRemoved(PeerNode)}, {@link #availableNodesReset()}
     * to update the good and bad nodes lists.
     *
     * @param availableRemoteNodes
     *            the remote nodes available for connections
     */
    public RoundRobinConnectionPolicy(final List<PeerNode> availableRemoteNodes) {
        super(availableRemoteNodes);
    }

    /**
     * Selects a good node in a round-robin fashion.
     */
    @Override
    protected synchronized PeerNode getGoodNode() {
        if (goodNodes.size() == 1) {
            return goodNodes.get(0);
        } else {
            nextNode %= goodNodes.size();
            return goodNodes.get(nextNode++);
        }
    }
}
