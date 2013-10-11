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

import java.util.ArrayList;
import java.util.List;

/**
 * Defines a policy on how to select a node to connect to from a set of
 * possible nodes and whether to automatically re-connect.
 *
 * @author Nico Kruber, kruber@zib.de
 *
 * @see ConnectionFactory
 *
 * @version 2.3
 * @since 2.3
 */
public abstract class ConnectionPolicy {
    /**
     * A reference to the list of available nodes
     */
    protected List<PeerNode> availableRemoteNodes;

    /**
     * Creates a connection policy with one available node to connect to.
     *
     * Provided for convenience.
     *
     * @param remoteNode the node available for connections
     */
    public ConnectionPolicy(final PeerNode remoteNode) {
        availableRemoteNodes = new ArrayList<PeerNode>(1);
        availableRemoteNodes.add(remoteNode);
    }

    /**
     * Creates a connection policy with the given set of nodes available for
     * connections.
     *
     * @param availableRemoteNodes available nodes to connect to
     */
    public ConnectionPolicy(final List<PeerNode> availableRemoteNodes) {
        this.availableRemoteNodes = availableRemoteNodes;
    }

    /**
     * Signals the connection policy that the given node has been added to the
     * list of available nodes.
     *
     * @param newNode the new node
     */
    public void availableNodeAdded(final PeerNode newNode) {
    }

    /**
     * Signals the connection policy that the given node has been removed from
     * the list of available nodes.
     *
     * @param removedNode the removed node
     */
    public void availableNodeRemoved(final PeerNode removedNode) {
    }

    /**
     * Signals the connection policy that the list of available nodes has been
     * reset (cleared).
     */
    public void availableNodesReset() {
    }

    /**
     * Acts upon a failure of the given node.
     *
     * Sets the node's last failed connect time stamp.
     *
     * @param node the failed node
     */
    public void nodeFailed(final PeerNode node) {
        synchronized (node) {
            node.setLastFailedConnect();
        }
    }

    /**
     * Acts upon a failure reset of the given node.
     *
     * Resets the node's last failure state.
     *
     * @param node the node
     */
    public void nodeFailReset(final PeerNode node) {
        synchronized (node) {
            node.resetFailureCount();
        }
    }

    /**
     * Acts upon a successful connect attempt of the given node.
     *
     * Sets the node's last successful connect time stamp and resets its failure
     * statistics.
     *
     * @param node the node
     */
    public void nodeConnectSuccess(final PeerNode node) {
        synchronized (node) {
            node.resetFailureCount();
            node.setLastConnectSuccess();
        }
    }

    /**
     * Selects the node to connect with when establishing a connection (no
     * failed node, no exception that has already been thrown).
     *
     * Provided for convenience.
     *
     * @return the node to use for the connection
     *
     * @throws UnsupportedOperationException
     *             is thrown if the operation can not be performed, e.g. the
     *             list is empty
     *
     * @see ConnectionFactory
     */
    public PeerNode selectNode() throws UnsupportedOperationException {
        try {
            return selectNode(0, null, null);
        } catch (final UnsupportedOperationException e) {
            throw e;
        } catch (final Exception e) {
            // this should not happen - selectNode should be able to copy with
            // this situation without throwing another exception than
            // UnsupportedOperationException
            throw new InternalError();
        }
    }

    /**
     * Selects the node to (re-)connect with.
     *
     * If no re-connection is desired, throw an exception!
     *
     * @param <E>
     *            the type of the exception that came from the failed connection
     *            and may be re-thrown
     *
     * @param retry
     *            the n'th retry (initial connect = 0, 1st reconnect = 1,...)
     * @param failedNode
     *            the node from the previous connection attempt or {@code null}
     * @param e
     *            the exception that came back from the previous connection
     *            attempt or {@code null}
     *
     * @return the new node to connect with
     *
     * @throws E
     *             if thrown, automatic re-connection attempts will stop
     * @throws UnsupportedOperationException
     *             is thrown if the operation can not be performed, e.g. the
     *             list is empty
     *
     * @see Connection#connect()
     * @see Connection#doRPC(String, String,
     *      com.ericsson.otp.erlang.OtpErlangList)
     * @see Connection#doRPC(String, String,
     *      com.ericsson.otp.erlang.OtpErlangObject[])
     */
    public abstract <E extends Exception> PeerNode selectNode(int retry,
            PeerNode failedNode, E e) throws E, UnsupportedOperationException;
}
