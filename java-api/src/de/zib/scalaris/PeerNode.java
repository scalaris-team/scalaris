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

import java.util.Date;
import java.util.List;

import com.ericsson.otp.erlang.OtpPeer;

/**
 * Wrapper class to the {@link OtpPeer} class, adding some additional
 * information.
 *
 * @author Nico Kruber, kruber@zib.de
 */
public class PeerNode {
    /**
     * The node this object wraps.
     */
    private final OtpPeer node;
    /**
     * Date of the last failed connection.
     */
    private Date lastFailedConnection = null;

    /**
     * Total number of connection failures.
     */
    private int failureCount = 0;

    /**
     * Date of the last successful connection attempt.
     */
    private Date lastConnectSuccess = null;

    /**
     * Creates a new object using the given node.
     *
     * @param node
     *            the node to wrap
     */
    public PeerNode(final OtpPeer node) {
        super();
        this.node = node;
    }

    /**
     * Creates a new object using the given node.
     *
     * Provided for convenience.
     *
     * @param node
     *            the name of the node to wrap
     */
    public PeerNode(final String node) {
        super();
        this.node = new OtpPeer(node);
    }

    /**
     * Gets the OTP node that is being wrapped.
     *
     * @return the node
     */
    public OtpPeer getNode() {
        return node;
    }

    /**
     * Sets the last failed connection (attempt or broken connection) with the
     * current date and time.
     *
     * Note: Only call this from a connection policy since it might set up
     * additional data structures based on this time.
     */
    synchronized void setLastFailedConnect() {
        lastFailedConnection = new Date();
        ++failureCount;
    }

    /**
     * Gets the date of the last failed connection.
     *
     * @return the date of the last connection failure (or {@code null})
     */
    synchronized public Date getLastFailedConnect() {
        return lastFailedConnection;
    }

    /**
     * Gets the number of failed connections.
     *
     * This is faster than getting a list of failed connections and calling
     * {@link List#size()} since this does not require a conversion.
     *
     * @return the number of failed connections (dates and times)
     */
    synchronized public int getFailureCount() {
        return failureCount;
    }

    /**
     * Resets the failed connections statistics.
     *
     * Note: Only call this from a connection policy since it might set up
     * additional data structures based on this time.
     */
    synchronized void resetFailureCount() {
        failureCount = 0;
        lastFailedConnection = null;
    }

    /**
     * Gets the date of the last successful connection.
     *
     * @return the last connection success
     */
    synchronized public Date getLastConnectSuccess() {
        return lastConnectSuccess;
    }

    /**
     * Adds a connection success with the current date.
     *
     * Note: Only call this from a connection policy since it might set up
     * additional data structures based on this time.
     */
    synchronized void setLastConnectSuccess() {
        this.lastConnectSuccess = new Date();
    }

    /**
     * Returns a string representation of this node.
     *
     * @return the name of the node
     */
    @Override
    public String toString() {
        return node.toString();
    }
}
