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

import java.util.ArrayList;
import java.util.List;

import com.ericsson.otp.erlang.OtpErlangAtom;
import com.ericsson.otp.erlang.OtpErlangList;
import com.ericsson.otp.erlang.OtpErlangObject;
import com.ericsson.otp.erlang.OtpErlangTuple;

/**
 * Provides methods to interact with some Scalaris (Erlang) VM.
 *
 * <p>
 * Instances of this class can be generated using a given connection to a
 * scalaris node ({@link #Scalaris(Connection)}) or without a
 * connection ({@link #Scalaris()}) in which case a new connection
 * is created using {@link ConnectionFactory#createConnection()}.
 * </p>
 *
 * @author Nico Kruber, kruber@zib.de
 * @version 3.10
 * @since 3.10
 */
public class Scalaris {
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
    public Scalaris() throws ConnectionException {
        connection = ConnectionFactory.getInstance().createConnection();
    }

    /**
     * Constructor, uses the given connection to an erlang node.
     *
     * @param conn
     *            connection to use for the transaction
     */
    public Scalaris(final Connection conn) {
        connection = conn;
    }

    /**
     * Retrieves random nodes from Scalaris for use by
     * {@link ConnectionFactory#addNode(String)} or {@link ScalarisVM}.
     * 
     * @param max
     *            maximum number of nodes to return (> 0)
     * 
     * @return a list of nodes (if an empty node list was returned from
     *         Scalaris, the connection's node will be returned here)
     * 
     * @throws ConnectionException
     *             if the connection is not active or a communication error
     *             occurs or an exit signal was received or the remote node
     *             sends a message containing an invalid cookie
     * @throws UnknownException
     *             if any other error occurs
     */
    public List<String> getRandomNodes(int max)
            throws ConnectionException, UnknownException {
        if (max <= 0) {
            throw new IllegalArgumentException("max must be an integer > 0");
        }
        final OtpErlangObject received_raw = connection.doRPC("api_vm", "get_other_vms",
                    new OtpErlangObject[] { ErlangValue.convertToErlang(max) });
        try {
            final OtpErlangList list = ErlangValue.otpObjectToOtpList(received_raw);
            final ArrayList<String> result = new ArrayList<String>(list.arity());
            for (int i = 0; i < list.arity(); ++i) {
                OtpErlangTuple connTuple = ((OtpErlangTuple) list.elementAt(i));
                if (connTuple.arity() != 4) {
                    throw new UnknownException(received_raw);
                }
                OtpErlangAtom name_otp = (OtpErlangAtom) connTuple.elementAt(i);
                result.add(name_otp.atomValue());
            }
            if (result.isEmpty()) {
                result.add(connection.getRemote().getNode().toString());
            }
            return result;
        } catch (final ClassCastException e) {
            throw new UnknownException(e, received_raw);
        }
    }

    /**
     * Closes the transaction's connection to a scalaris node.
     *
     * Note: Subsequent calls to the other methods will throw
     * {@link ConnectionException}s!
     */
    public void closeConnection() {
        connection.close();
    }
}
