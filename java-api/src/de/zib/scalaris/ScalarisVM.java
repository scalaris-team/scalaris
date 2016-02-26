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

import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.ericsson.otp.erlang.OtpErlangAtom;
import com.ericsson.otp.erlang.OtpErlangInt;
import com.ericsson.otp.erlang.OtpErlangList;
import com.ericsson.otp.erlang.OtpErlangObject;
import com.ericsson.otp.erlang.OtpErlangTuple;

/**
 * Provides methods to interact with a specific Scalaris (Erlang) VM.
 *
 * @author Nico Kruber, kruber@zib.de
 * @version 3.10
 * @since 3.6
 */
public class ScalarisVM {
    /**
     * Connection to a Scalaris node.
     */
    private final Connection connection;

    /**
     * Creates a connection to the erlang VM of the given Scalaris node. Uses
     * the connection policy of the global connection factory.
     *
     * @param node
     *            Scalaris node to connect with
     * @throws ConnectionException
     *             if the connection fails or the connection policy is not
     *             cloneable
     */
    public ScalarisVM(final PeerNode node) throws ConnectionException {
        final ConnectionFactory cf = ConnectionFactory.getInstance();
        connection = cf.createConnection(new FixedNodeConnectionPolicy(node));
    }

    /**
     * Creates a connection to the erlang VM of the given Scalaris node. Uses
     * the connection policy of the global connection factory.
     *
     * @param node
     *            Scalaris node to connect with
     * @throws ConnectionException
     *             if the connection fails or the connection policy is not
     *             cloneable
     */
    public ScalarisVM(final String node) throws ConnectionException {
        final ConnectionFactory cf = ConnectionFactory.getInstance();
        connection = cf.createConnection(new FixedNodeConnectionPolicy(node));
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
     * Plain old data object for results of {@link ScalarisVM#getInfo()}.
     *
     * @author Nico Kruber, kruber@zib.de
     * @version 3.6
     * @since 3.6
     */
    public static class GetInfoResult {
        /**
         * Scalaris version string.
         */
        public final String scalarisVersion;
        /**
         * Erlang version string.
         */
        public final String erlangVersion;
        /**
         * Total amount of memory currently allocated.
         */
        public final int memTotal;
        /**
         * Uptime of the Erlang VM.
         */
        public final int uptime;
        /**
         * Erlang node name.
         */
        public final String erlangNode;
        /**
         * IP address to reach the Scalaris node inside the Erlang VM.
         */
        public final Inet4Address ip;
        /**
         * Port to reach the Scalaris node inside the Erlang VM.
         */
        public final int port;
        /**
         * Yaws port to reach the JSON API and web debug interface.
         */
        public final int yawsPort;

        protected GetInfoResult(final String scalarisVersion, final String erlangVersion,
                final int memTotal, final int uptime, final String erlangNode, final Inet4Address ip,
                final int port, final int yawsPort) {
            super();
            this.scalarisVersion = scalarisVersion;
            this.erlangVersion = erlangVersion;
            this.memTotal = memTotal;
            this.uptime = uptime;
            this.erlangNode = erlangNode;
            this.ip = ip;
            this.port = port;
            this.yawsPort = yawsPort;
        }
    }

    /**
     * Gets some information about the VM and Scalaris.
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
    public GetInfoResult getInfo()
            throws ConnectionException, UnknownException {
        final OtpErlangObject received_raw = connection.doRPC("api_vm", "get_info",
                    new OtpErlangObject[] {});
        try {
            final OtpErlangList received = (OtpErlangList) received_raw;
            final Map<String, OtpErlangObject> result = new LinkedHashMap<String, OtpErlangObject>(
                    received.arity());
            for (final OtpErlangObject iter : received) {
                final OtpErlangTuple iter_tpl = (OtpErlangTuple) iter;
                if (iter_tpl.arity() == 2) {
                    final String key = ((OtpErlangAtom) (iter_tpl.elementAt(0))).atomValue();
                    result.put(key, iter_tpl.elementAt(1));
                } else {
                    throw new UnknownException(received_raw);
                }
            }
            final String scalarisVersion = new ErlangValue(result.get("scalaris_version")).stringValue();
            final String erlangVersion = new ErlangValue(result.get("erlang_version")).stringValue();
            final int memTotal = new ErlangValue(result.get("mem_total")).intValue();
            final int uptime = new ErlangValue(result.get("uptime")).intValue();
            final String erlangNode = new ErlangValue(result.get("erlang_node")).stringValue();
            final OtpErlangTuple erlIP = (OtpErlangTuple) result.get("ip");
            final Inet4Address ip = (Inet4Address) Inet4Address.getByName(
                    erlIP.elementAt(0) + "." + erlIP.elementAt(1) + "." + erlIP.elementAt(2) + "." + erlIP.elementAt(3));
            final int port = new ErlangValue(result.get("port")).intValue();
            final int yawsPort = new ErlangValue(result.get("yaws_port")).intValue();
            return new GetInfoResult(scalarisVersion, erlangVersion, memTotal, uptime, erlangNode, ip, port, yawsPort);
        } catch (final ClassCastException e) {
            throw new UnknownException(e, received_raw);
        } catch (final NullPointerException e) {
            throw new UnknownException(e, received_raw);
        } catch (final UnknownHostException e) {
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
     * @return the names of the nodes (arbitrary type!)
     *
     * @throws ConnectionException
     *             if the connection is not active or a communication error
     *             occurs or an exit signal was received or the remote node
     *             sends a message containing an invalid cookie
     * @throws UnknownException
     *             if any other error occurs
     */
    public List<ErlangValue> getNodes()
            throws ConnectionException, UnknownException {
        final OtpErlangObject received_raw = connection.doRPC("api_vm", "get_nodes",
                    new OtpErlangObject[] {});
        try {
            return new ErlangValue(received_raw).listValue();
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
        public final List<ErlangValue> successful;
        /**
         * Error string for nodes that could not be started (empty if all nodes
         * have been started successfully).
         */
        public final String errors;

        protected AddNodesResult(final List<ErlangValue> successful, final String errors) {
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
            final List<ErlangValue> successful = new ErlangValue(received.elementAt(0)).listValue();
            final OtpErlangList errors = ErlangValue.otpObjectToOtpList(received.elementAt(1));
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
    public boolean shutdownNode(final ErlangValue name)
            throws ConnectionException, UnknownException {
        final OtpErlangObject received_raw = connection.doRPC("api_vm", "shutdown_node",
                    new OtpErlangObject[] { name.value() });
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
    public boolean killNode(final ErlangValue name)
            throws ConnectionException, UnknownException {
        final OtpErlangObject received_raw = connection.doRPC("api_vm", "kill_node",
                    new OtpErlangObject[] { name.value() });
        if (received_raw.equals(CommonErlangObjects.okAtom)) {
            return true;
        } else if (received_raw.equals(CommonErlangObjects.notFoundAtom)) {
            return false;
        }
        throw new UnknownException(received_raw);
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
    public List<ErlangValue> shutdownNodes(final int number)
            throws ConnectionException, UnknownException {
        final OtpErlangObject received_raw = connection.doRPC("api_vm", "shutdown_nodes",
                    new OtpErlangObject[] { new OtpErlangInt(number) });
        return makeDeleteResult(received_raw);
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
    public List<ErlangValue> killNodes(final int number)
            throws ConnectionException, UnknownException {
        final OtpErlangObject received_raw = connection.doRPC("api_vm", "kill_nodes",
                    new OtpErlangObject[] { new OtpErlangInt(number) });
        return makeDeleteResult(received_raw);
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
    private final List<ErlangValue> makeDeleteResult(
            final OtpErlangObject received_raw) throws UnknownException {
        try {
            return new ErlangValue(received_raw).listValue();
        } catch (final ClassCastException e) {
            throw new UnknownException(e, received_raw);
        }
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
        public final List<ErlangValue> successful;
        /**
         * Nodes which do not exist (anymore) in the VM.
         */
        public final List<ErlangValue> notFound;

        protected DeleteNodesByNameResult(final List<ErlangValue> successful,
                final List<ErlangValue> notFound) {
            this.successful = successful;
            this.notFound = notFound;
        }
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
    public DeleteNodesByNameResult shutdownNodesByName(final List<ErlangValue> names)
            throws ConnectionException, UnknownException {
        final OtpErlangObject received_raw = connection.doRPC("api_vm", "shutdown_nodes_by_name",
                    new OtpErlangObject[] { ErlangValue.convertToErlang(names) });
        return makeDeleteByNameResult(received_raw);
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
    public DeleteNodesByNameResult killNodes(final List<ErlangValue> names)
            throws ConnectionException, UnknownException {
        final OtpErlangObject received_raw = connection.doRPC("api_vm", "kill_nodes_by_name",
                    new OtpErlangObject[] { ErlangValue.convertToErlang(names) });
        return makeDeleteByNameResult(received_raw);
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
            final List<ErlangValue> successful = new ErlangValue(received.elementAt(0)).listValue();
            final List<ErlangValue> not_found = new ErlangValue(received.elementAt(1)).listValue();
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
     *            maximum number of nodes to return (> 0)
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
    public List<String> getOtherVMs(final int max)
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
                final OtpErlangTuple connTuple = ((OtpErlangTuple) list.elementAt(i));
                if (connTuple.arity() != 4) {
                    throw new UnknownException(received_raw);
                }
                final OtpErlangAtom name_otp = (OtpErlangAtom) connTuple.elementAt(0);
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
