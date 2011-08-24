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
import java.util.Map;

import com.ericsson.otp.erlang.OtpErlangAtom;
import com.ericsson.otp.erlang.OtpErlangInt;
import com.ericsson.otp.erlang.OtpErlangList;
import com.ericsson.otp.erlang.OtpErlangObject;
import com.ericsson.otp.erlang.OtpErlangString;
import com.ericsson.otp.erlang.OtpErlangTuple;

/**
 * Provides methods to interact with a specific Scalaris (Erlang) VM.
 *
 * <p>
 * Instances of this class can be generated using a given connection to a
 * scalaris node ({@link #ScalarisVM(Connection)}) or without a
 * connection ({@link #ScalarisVM()}) in which case a new connection
 * is created using {@link ConnectionFactory#createConnection()}.
 * </p>
 *
 * @author Nico Kruber, kruber@zib.de
 * @version 3.6
 * @since 3.6
 */
public class ScalarisVM {
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
    public ScalarisVM() throws ConnectionException {
        connection = ConnectionFactory.getInstance().createConnection();
    }

    /**
     * Constructor, uses the given connection to an erlang node.
     *
     * @param conn
     *            connection to use for the transaction
     */
    public ScalarisVM(final Connection conn) {
        connection = conn;
    }

    /**
     * Gets the version of the Scalaris VM of the current connection.
     *
     * @return Scalaris version string
     *
     * @throws ConnectionException
     *             if the connection is not active or a communication error
     *             occurs or an exit signal was received or the remote node
     *             sends a message containing an invalid cookie
     * @throws UnknownException
     *             if any other error occurs
     */
    public String getVersion()
            throws ConnectionException, UnknownException {
        final OtpErlangObject received_raw = connection.doRPC("api_vm", "get_version",
                    new OtpErlangObject[] {});
        try {
            return new ErlangValue(received_raw).stringValue();
        } catch (final ClassCastException e) {
            throw new UnknownException(e, received_raw);
        }
    }

    /**
     * Gets some information about the VM and Scalaris.
     *
     * Available keys in the returned map are:
     * <ul>
     *  <li><tt>scalaris_version</tt> - contains a {@link String}</li>
     *  <li><tt>erlang_version</tt> - contains a {@link String}</li>
     *  <li><tt>mem_total</tt> - contains an {@link Integer}</li>
     *  <li><tt>uptime</tt> - contains an {@link Integer}</li>
     * </ul>
     *
     * @return VM information
     *
     * @throws ConnectionException
     *             if the connection is not active or a communication error
     *             occurs or an exit signal was received or the remote node
     *             sends a message containing an invalid cookie
     * @throws UnknownException
     *             if any other error occurs
     */
    public Map<String, Object> getInfo()
            throws ConnectionException, UnknownException {
        final OtpErlangObject received_raw = connection.doRPC("api_vm", "get_info",
                    new OtpErlangObject[] {});
        try {
            // note: the type ist a K/V list which is the same as in JSON object
            final ErlangValueJSONToMap json_converter = new ErlangValueJSONToMap();
            return json_converter.toJava((OtpErlangList) received_raw);
        } catch (final ClassCastException e) {
            throw new UnknownException(e, received_raw);
        }
    }

    /**
     * Gets the number of nodes in the Scalaris VM of the current connection.
     *
     * @return number of nodes
     *
     * @throws ConnectionException
     *             if the connection is not active or a communication error
     *             occurs or an exit signal was received or the remote node
     *             sends a message containing an invalid cookie
     * @throws UnknownException
     *             if any other error occurs
     */
    public int getNumberOfNodes()
            throws ConnectionException, UnknownException {
        final OtpErlangObject received_raw = connection.doRPC("api_vm", "number_of_nodes",
                    new OtpErlangObject[] {});
        try {
            return new ErlangValue(received_raw).intValue();
        } catch (final ClassCastException e) {
            throw new UnknownException(e, received_raw);
        }
    }

    /**
     * Gets the names of the nodes in the Scalaris VM of the current connection.
     *
     * @return the names of the nodes
     *
     * @throws ConnectionException
     *             if the connection is not active or a communication error
     *             occurs or an exit signal was received or the remote node
     *             sends a message containing an invalid cookie
     * @throws UnknownException
     *             if any other error occurs
     */
    public List<String> getNodes()
            throws ConnectionException, UnknownException {
        final OtpErlangObject received_raw = connection.doRPC("api_vm", "get_nodes",
                    new OtpErlangObject[] {});
        try {
            return new ErlangValue(received_raw).stringListValue();
        } catch (final ClassCastException e) {
            throw new UnknownException(e, received_raw);
        }
    }

    /**
     * Plain old data object for results of {@link ScalarisVM#addNodes(int)}.
     *
     * @author Nico Kruber, kruber@zib.de
     * @version 3.6
     * @since 3.6
     */
    public static class AddNodesResult {
        /**
         * Names of successfully added nodes.
         */
        public final List<String> successful;
        /**
         * Error string for nodes that could not be started (empty if all nodes
         * have been started successfully).
         */
        public final String errors;

        protected AddNodesResult(List<String> successful, String errors) {
            this.successful = successful;
            this.errors = errors;
        }
    }

    /**
     * Adds the given number of nodes to the Scalaris VM of the current connection.
     *
     * @param number
     *            number of nodes to add
     *
     * @return result of the operation
     *
     * @throws ConnectionException
     *             if the connection is not active or a communication error
     *             occurs or an exit signal was received or the remote node
     *             sends a message containing an invalid cookie
     * @throws UnknownException
     *             if any other error occurs
     */
    public AddNodesResult addNodes(final int number)
            throws ConnectionException, UnknownException {
        final OtpErlangObject received_raw = connection.doRPC("api_vm", "add_nodes",
                    new OtpErlangObject[] { new OtpErlangInt(number) });
        try {
            final OtpErlangTuple received = (OtpErlangTuple) received_raw;
            final List<String> successful = new ErlangValue(received.elementAt(0)).stringListValue();
            OtpErlangList errors = ErlangValue.otpObjectToOtpList(received.elementAt(1));
            String error_str;
            if (errors.arity() == 0) {
                error_str = "";
            } else {
                error_str = errors.toString();
            }
            return new AddNodesResult(successful, error_str);
        } catch (final ClassCastException e) {
            throw new UnknownException(e, received_raw);
        }
    }

    /**
     * Shuts down the given node (graceful leave) inside the Scalaris VM of the
     * current connection.
     *
     * @param name
     *            the name of a node
     *
     * @return <tt>true</tt> if the node was shut down,
     *         <tt>false</tt> if the node was not found
     *
     * @throws ConnectionException
     *             if the connection is not active or a communication error
     *             occurs or an exit signal was received or the remote node
     *             sends a message containing an invalid cookie
     * @throws UnknownException
     *             if any other error occurs
     */
    public boolean shutdownNode(String name)
            throws ConnectionException, UnknownException {
        final OtpErlangObject received_raw = connection.doRPC("api_vm", "shutdown_node",
                    new OtpErlangObject[] { new OtpErlangString(name) });
        if (received_raw.equals(CommonErlangObjects.okAtom)) {
            return true;
        } else if (received_raw.equals(CommonErlangObjects.notFoundAtom)) {
            return false;
        }
        throw new UnknownException(received_raw);
    }

    /**
     * Kills the given node inside the Scalaris VM of the current connection.
     *
     * @param name
     *            the name of a node
     *
     * @return <tt>true</tt> if the node was shut down,
     *         <tt>false</tt> if the node was not found
     *
     * @throws ConnectionException
     *             if the connection is not active or a communication error
     *             occurs or an exit signal was received or the remote node
     *             sends a message containing an invalid cookie
     * @throws UnknownException
     *             if any other error occurs
     */
    public boolean killNode(String name)
            throws ConnectionException, UnknownException {
        final OtpErlangObject received_raw = connection.doRPC("api_vm", "kill_node",
                    new OtpErlangObject[] { new OtpErlangString(name) });
        if (received_raw.equals(CommonErlangObjects.okAtom)) {
            return true;
        } else if (received_raw.equals(CommonErlangObjects.notFoundAtom)) {
            return false;
        }
        throw new UnknownException(received_raw);
    }

    /**
     * Plain old data object for results of
     * {@link ScalarisVM#shutdownNodes(int)}, {@link ScalarisVM#shutdownNodesByName(List)},
     * {@link ScalarisVM#killNodes(int)} and {@link ScalarisVM#killNodes(List)}.
     *
     * @author Nico Kruber, kruber@zib.de
     * @version 3.6
     * @since 3.6
     */
    public static class DeleteNodesByNameResult {
        /**
         * Names of successfully deleted nodes.
         */
        public final List<String> successful;
        /**
         * Nodes which do not exist (anymore) in the VM.
         */
        public final List<String> notFound;

        protected DeleteNodesByNameResult(List<String> successful, List<String> notFound) {
            this.successful = successful;
            this.notFound = notFound;
        }
    }

    /**
     * Shuts down the given number of nodes (graceful leave) inside the
     * Scalaris VM of the current connection.
     *
     * @param number
     *            number of nodes to shut down
     *
     * @return result of the operation
     *
     * @throws ConnectionException
     *             if the connection is not active or a communication error
     *             occurs or an exit signal was received or the remote node
     *             sends a message containing an invalid cookie
     * @throws UnknownException
     *             if any other error occurs
     */
    public List<String> shutdownNodes(final int number)
            throws ConnectionException, UnknownException {
        final OtpErlangObject received_raw = connection.doRPC("api_vm", "shutdown_nodes",
                    new OtpErlangObject[] { new OtpErlangInt(number) });
        return makeDeleteResult(received_raw);
    }

    /**
     * Shuts down the given nodes (graceful leave) inside the Scalaris VM of the
     * current connection.
     *
     * @param names
     *            names of the nodes to shut down
     *
     * @return result of the operation
     *
     * @throws ConnectionException
     *             if the connection is not active or a communication error
     *             occurs or an exit signal was received or the remote node
     *             sends a message containing an invalid cookie
     * @throws UnknownException
     *             if any other error occurs
     */
    public DeleteNodesByNameResult shutdownNodesByName(final List<String> names)
            throws ConnectionException, UnknownException {
        final OtpErlangObject received_raw = connection.doRPC("api_vm", "shutdown_nodes_by_name",
                    new OtpErlangObject[] { ErlangValue.convertToErlang(names) });
        return makeDeleteByNameResult(received_raw);
    }

    /**
     * Kills the given number of nodes inside the Scalaris VM of the current
     * connection.
     *
     * @param number
     *            number of nodes to kill
     *
     * @return result of the operation
     *
     * @throws ConnectionException
     *             if the connection is not active or a communication error
     *             occurs or an exit signal was received or the remote node
     *             sends a message containing an invalid cookie
     * @throws UnknownException
     *             if any other error occurs
     */
    public List<String> killNodes(final int number)
            throws ConnectionException, UnknownException {
        final OtpErlangObject received_raw = connection.doRPC("api_vm", "kill_nodes",
                    new OtpErlangObject[] { new OtpErlangInt(number) });
        return makeDeleteResult(received_raw);
    }

    /**
     * Kills the given nodes inside the Scalaris VM of the current connection.
     *
     * @param names
     *            names of the nodes to kill
     *
     * @return result of the operation
     *
     * @throws ConnectionException
     *             if the connection is not active or a communication error
     *             occurs or an exit signal was received or the remote node
     *             sends a message containing an invalid cookie
     * @throws UnknownException
     *             if any other error occurs
     */
    public DeleteNodesByNameResult killNodes(final List<String> names)
            throws ConnectionException, UnknownException {
        final OtpErlangObject received_raw = connection.doRPC("api_vm", "kill_nodes_by_name",
                    new OtpErlangObject[] { ErlangValue.convertToErlang(names) });
        return makeDeleteByNameResult(received_raw);
    }

    /**
     * Transforms the given result from a "delete nodes"-operation into a
     * {@link DeleteResult}.
     *
     * @param received_raw
     *            raw erlang result
     *
     * @return {@link DeleteResult} object
     *
     * @throws UnknownException
     *             if an error occurs during transformation
     */
    private final List<String> makeDeleteResult(
            final OtpErlangObject received_raw) throws UnknownException {
        try {
            return new ErlangValue(received_raw).stringListValue();
        } catch (final ClassCastException e) {
            throw new UnknownException(e, received_raw);
        }
    }

    /**
     * Transforms the given result from a "delete nodes by name"-operation into
     * a {@link DeleteNodesByNameResult}.
     *
     * @param received_raw
     *            raw erlang result
     *
     * @return {@link DeleteResult} object
     *
     * @throws UnknownException
     *             if an error occurs during transformation
     */
    private final DeleteNodesByNameResult makeDeleteByNameResult(
            final OtpErlangObject received_raw) throws UnknownException {
        try {
            final OtpErlangTuple received = (OtpErlangTuple) received_raw;
            final List<String> successful = new ErlangValue(received.elementAt(0)).stringListValue();
            final List<String> not_found = new ErlangValue(received.elementAt(1)).stringListValue();
            return new DeleteNodesByNameResult(successful, not_found);
        } catch (final ClassCastException e) {
            throw new UnknownException(e, received_raw);
        }
    }

    /**
     * Retrieves additional nodes from the Scalaris VM of the current
     * connection for use by {@link ConnectionFactory#addNode(String)}.
     *
     * @param max
     *            maximum number of nodes to return
     *
     * @return a list of nodes (may be empty!)
     *
     * @throws ConnectionException
     *             if the connection is not active or a communication error
     *             occurs or an exit signal was received or the remote node
     *             sends a message containing an invalid cookie
     * @throws UnknownException
     *             if any other error occurs
     */
    public List<String> getOtherVMs(int max)
            throws ConnectionException, UnknownException {
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
            return result;
        } catch (final ClassCastException e) {
            throw new UnknownException(e, received_raw);
        }
    }

    /**
     * Tells the Scalaris VM of the current connection to shut down gracefully.
     *
     * @throws ConnectionException
     *             if the connection is not active or a communication error
     *             occurs or an exit signal was received or the remote node
     *             sends a message containing an invalid cookie
     * @throws UnknownException
     *             if any other error occurs
     */
    public void shutdownVM() throws ConnectionException {
        connection.sendRPC("api_vm", "shutdown_vm", new OtpErlangObject[] {});
    }

    /**
     * Kills the Scalaris VM of the current connection.
     *
     * @throws ConnectionException
     *             if the connection is not active or a communication error
     *             occurs or an exit signal was received or the remote node
     *             sends a message containing an invalid cookie
     * @throws UnknownException
     *             if any other error occurs
     */
    public void killVM() throws ConnectionException {
        connection.sendRPC("api_vm", "kill_vm", new OtpErlangObject[] {});
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
