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

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;

/**
 * Implements a simple (thread-safe) connection pool for Scalaris connections.
 *
 * @author Nico Kruber, kruber@zib.de
 * @version 3.7
 * @since 3.7
 */
public class ConnectionPool {
    /**
     * Connection factory used for creating connections.
     */
    protected ConnectionFactory cFactory;
    /**
     * Maximum number of connections in this pool (including checked out and
     * available connections).
     */
    protected int maxConnections;
    /**
     * All available connections not checked out yet.
     */
    protected LinkedList<Connection> availableConns;
    /**
     * Number of checked out connections.
     */
    protected int checkedOut = 0;

    /**
     * Creates a new connection pool.
     *
     * @param cFactory
     *            the connection factory to use to create new connections
     * @param maxConnections
     *            the maximum number of connections (<tt>0</tt> for no limit)
     */
    public ConnectionPool(final ConnectionFactory cFactory,
            final int maxConnections) {
        this.cFactory = cFactory;
        this.maxConnections = maxConnections;
        availableConns = new LinkedList<Connection>();
    }

    /**
     * Gets a connection from the pool. Creates a new connection if necessary.
     * Returns <tt>null</tt> if the maximum number of connections has already
     * been hit.
     *
     * @return a connection to Scalaris or <tt>null</tt> if the maximum number
     *         of connections has been hit
     *
     * @throws ConnectionException
     *             if creating the connection fails
     */
    public synchronized Connection getConnection() throws ConnectionException {
        Connection conn = null;
        // use first available connection (if any):
        if (!availableConns.isEmpty()) {
            conn = availableConns.remove();
            ++checkedOut;
        } else if ((maxConnections == 0) || (checkedOut < maxConnections)) {
            conn = cFactory.createConnection();
            ++checkedOut;
        }
        return conn;
    }

    /**
     * Tries to get a valid connection from the pool waiting at most
     * <tt>timeout</tt> milliseconds. Creates a new connection if necessary and
     * the maximum number of connections has not been hit yet. If the timeout is
     * hit and no connection is available, <tt>null</tt> is returned.
     *
     * @param timeout
     *            number of milliseconds to wait at most for a valid connection
     *            to appear (<tt>0</tt> to wait forever)
     *
     * @return a connection to Scalaris or <tt>null</tt> if the timeout has been
     *         hit
     *
     * @throws ConnectionException
     *             if creating the connection fails
     */
    public Connection getConnection(final long timeout) throws ConnectionException {
        final long timeAtStart = System.currentTimeMillis();
        Connection conn;
        while ((conn = getConnection()) == null) {
            try {
                synchronized (this) {
                    wait(timeout);
                }
            } catch (final InterruptedException e) {
            }
            final long timeAtEnd = System.currentTimeMillis();
            if ((timeAtEnd - timeAtStart) >= timeout) {
                return null; // timeout
            }
        }
        return conn;
    }

    /**
     * Puts the given connection back into the pool.
     *
     * @param conn
     *            the connection to release
     */
    public synchronized void releaseConnection(final Connection conn) {
        availableConns.add(conn);
        --checkedOut;
        // need to notify all waiting threads so they do not exceed their timeouts
        notifyAll();
    }

    /**
     * Closes all available pooled connections.
     *
     * NOTE: This does not include any checked out connections!
     */
    public synchronized void closeAll() {
        for (final Connection conn : availableConns) {
            conn.close();
        }
        availableConns.clear();
    }

    /**
     * Closes all available pooled connections to any node not in the given
     * collection.
     *
     * NOTE: This does not include any checked out connections!
     *
     * @param remainingNodes
     *            a set of nodes to which connections should remain (fast access
     *            to {@link Collection#contains(Object)} is preferable, e.g. use
     *            {@link Set})
     */
    public synchronized void closeAllBut(
            final Collection<PeerNode> remainingNodes) {
        for (final Iterator<Connection> iterator = availableConns.iterator();
                iterator.hasNext();) {
            final Connection conn = iterator.next();
            if (!remainingNodes.contains(conn.getRemote())) {
                conn.close();
                iterator.remove();
            }

        }
    }

    /* (non-Javadoc)
     * @see java.lang.Object#finalize()
     */
    @Override
    protected void finalize() throws Throwable {
        closeAll();
        super.finalize();
    }

    /**
     * Gets the connection factory used by the pool.
     *
     * @return the connection factory
     */
    public ConnectionFactory getConnectionFactory() {
        return cFactory;
    }

}
