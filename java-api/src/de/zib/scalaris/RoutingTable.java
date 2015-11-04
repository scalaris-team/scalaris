/*
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

import com.ericsson.otp.erlang.OtpErlangLong;
import com.ericsson.otp.erlang.OtpErlangObject;
import com.ericsson.otp.erlang.OtpErlangRangeException;

/**
 * Scalaris interface to basic routing table information.
 *
 * @author Thorsten Schuett, schuett@zib.de
 * @version 3.20
 * @since 3.20
 */
public class RoutingTable {
    /**
     * Connection to a Scalaris node.
     */
    private final Connection connection;

    /**
     * Constructor, uses the default connection returned by
     * {@link ConnectionFactory#createConnection()}.
     *
     * @throws ConnectionException
     *             if the connection fails
     */
    public RoutingTable() throws ConnectionException {
        connection = ConnectionFactory.getInstance().createConnection();
    }

    /**
     * Constructor, uses the given connection to an erlang node.
     *
     * @param conn
     *            connection to use for the Scalaris access
     */
    public RoutingTable(final Connection conn) {
        connection = conn;
    }

    /**
     * Returns the replication factor used by the current routing table
     * implementation.
     *
     * @return the current replication factor
     *
     * @throws ConnectionException
     *             if the connection is not active or a communication error
     *             occurs or an exit signal was received or the remote node
     *             sends a message containing an invalid cookie
     * @throws UnknownException
     *             if any other error occurs
     */
    public int getReplicationFactor() throws ConnectionException, UnknownException {
        final OtpErlangObject received_raw = connection.doRPC("api_rt", "get_replication_factor",
                new OtpErlangObject[] { });
        try {
            final OtpErlangLong received = (OtpErlangLong) received_raw;
            return received.intValue();
        } catch (final ClassCastException e) {
            // e.printStackTrace();
            throw new UnknownException(e, received_raw);
        } catch (final OtpErlangRangeException e) {
            // there should not this many replicates that do not fit into an integer!
            // e.printStackTrace();
            throw new UnknownException(e, received_raw);
        }
    }
}
