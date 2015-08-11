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

import java.io.PrintStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import com.ericsson.otp.erlang.OtpSelf;

import de.zib.tools.PropertyLoader;

/**
 * Provides means to create connections to scalaris nodes.
 *
 * This class uses a singleton-alike pattern providing a global (static)
 * instance through its {@link #getInstance()} method but also allowing for
 * object construction which might be useful when using multiple threads each
 * creating its own connections.
 *
 * The location of the default configuration file used by
 * {@link #ConnectionFactory()} can be overridden by specifying the <tt>
 * scalaris.java.config</tt> system property - otherwise the class tries to load
 * <tt>scalaris.properties</tt>.
 *
 * A specific property can also be overridden specifying a (non-empty) system
 * property with its name.
 *
 * A user-defined {@link Properties} object can also be used by creating objects
 * with {@link #ConnectionFactory(Properties)} or setting the new values with
 * {@link #setProperties(Properties)} but must provide the following values
 * (default values as shown)
 * <ul>
 * <li><tt>scalaris.node = "node1@localhost"</tt></li>
 * <li><tt>scalaris.cookie = "chocolate chip cookie"</tt></li>
 * <li><tt>scalaris.client.name = "java_client"</tt></li>
 * <li><tt>scalaris.client.appendUUID = "true"</tt></li>
 * </ul>
 *
 * Note: {@code scalaris.node} can be a whitespace, ',' or ';' separated list of
 * available nodes. See {@link DefaultConnectionPolicy} about how this list is
 * used when connections are setup or when existing connections fail.
 * Since Java and Erlang both need to known the same node name in order to
 * connect, any "@localhost" in such a name is translated using
 * {@link #fixLocalhostName(String)} to a real node name. The best method of
 * changing this name is to use the name Erlang uses - it can be provided by
 * setting the <tt>scalaris.erlang.nodename</tt> system property. If this is
 * not set or empty, we will try to look-up the name ourselves (see
 * {@link #getLocalhostName()}.
 *
 * The {@link #connectionPolicy} set will be passed on to created connections.
 * Changing it via {@link #setConnectionPolicy(ConnectionPolicy)} will not
 * change previously created connections - they keep the old policy. By
 * default, {@link DefaultConnectionPolicy} is used.
 *
 * @author Nico Kruber, kruber@zib.de
 * @version 3.20
 * @since 2.0
 */
public class ConnectionFactory {
    /**
     * Default name of the configuration file.
     */
    private static final String defaultConfigFile = "scalaris.properties";

    private static final String hostname = findLocalhostName();

    /**
     * The name of the node to connect to.
     */
    private final List<PeerNode> nodes = Collections.synchronizedList(new ArrayList<PeerNode>());
    /**
     * The cookie name to use for connections.
     */
    private String cookie;
    /**
     * The name of the (Java) client to use when establishing a connection with
     * erlang.
     */
    private String clientName;
    /**
     * Specifies whether to append a pseudo UUID to client names or not.
     */
    private boolean clientNameAppendUUID;
    /**
     * Pseudo UUID - the number of this counter is added to client names when
     * creating a connection if clientNameAppendUUID is set.
     *
     * <p>
     * TODO: In some cases, an atomic value is not necessary, e.g. when the
     * factory is used by only one thread, then a normal long value will
     * suffice. Possible optimisation: use {@link AtomicLong} for singleton
     * instance and {@link Long} for the other instances!
     * </p>
     */
    private final AtomicLong clientNameUUID = new AtomicLong(0);

    /**
     * Stores which config file was actually read into the object's properties.
     */
    private String configFileUsed;

    /**
     * Static instance of a connection factory.
     */
    private static ConnectionFactory instance = new ConnectionFactory();

    /**
     * Contains the default node selection policy a copy of which will be set
     * for each created connection.
     *
     * @since 2.3
     */
    private ConnectionPolicy connectionPolicy = new DefaultConnectionPolicy(nodes);

    /**
     * Returns the static instance of a connection factory.
     *
     * @return a connection factory
     */
    public static ConnectionFactory getInstance() {
        return instance;
    }

    /**
     * Constructor, sets the parameters to use for connections according to
     * values given in a <tt>scalaris.properties</tt> file and falls back to
     * default values if values don't exist.
     *
     * By default the config file is assumed to be in the same directory as
     * the classes. Specify the <tt>scalaris.java.config</tt> system property
     * to set a different location.
     *
     * Default values are:
     * <ul>
     * <li><tt>scalaris.node = "node1@localhost"</tt></li>
     * <li><tt>scalaris.cookie = "chocolate chip cookie"</tt></li>
     * <li><tt>scalaris.client.name = "java_client"</tt></li>
     * <li><tt>scalaris.client.appendUUID = "true"</tt></li>
     * </ul>
     *
     * These properties can be overridden by specifying (non-empty) system
     * properties with their names.
     */
    public ConnectionFactory() {
        final Properties properties = new Properties();
        String configFile = System.getProperty("scalaris.java.config");
        if ((configFile == null) || (configFile.length() == 0)) {
            configFile = defaultConfigFile;
        }
//        System.out.println("loading config file: " + configFile);
        PropertyLoader.loadProperties(properties, configFile, true, false,
                new String[] {"scalaris.node", "scalaris.cookie", "scalaris.client.name", "scalaris.client.appendUUID"});
        setProperties(properties);
    }

    /**
     * Constructor, sets the parameters to use for connections according to
     * values given in the given {@link Properties} object and falls back to
     * default values if values don't exist.
     *
     * The {@link Properties} object should provide the following values
     * (default values as shown):
     * <ul>
     * <li><tt>scalaris.node = "node1@localhost"</tt></li>
     * <li><tt>scalaris.cookie = "chocolate chip cookie"</tt></li>
     * <li><tt>scalaris.client.name = "java_client"</tt></li>
     * <li><tt>scalaris.client.appendUUID = "true"</tt></li>
     * </ul>
     *
     * @param properties
     */
    public ConnectionFactory(final Properties properties) {
        setProperties(properties);
    }

    /**
     * Sets the object's members used for creating connections to erlang to
     * values provided by the given {@link Properties} object.
     *
     * The {@link Properties} object should provide the following values
     * (default values as shown):
     * <ul>
     * <li><tt>scalaris.node = "node1@localhost"</tt></li>
     * <li><tt>scalaris.cookie = "chocolate chip cookie"</tt></li>
     * <li><tt>scalaris.client.name = "java_client"</tt></li>
     * <li><tt>scalaris.client.appendUUID = "true"</tt></li>
     * </ul>
     *
     * NOTE: Existing connections are not changed!
     *
     * @param properties
     *            the object to get the connection parameters from
     */
    public void setProperties(final Properties properties) {
        final String[] nodesTemp = properties.getProperty("scalaris.node", "node1@localhost").split("[\\s,;]");
        nodes.clear();

        for (final String element : nodesTemp) {
            addNode(fixLocalhostName(element));
        }
        cookie = properties.getProperty("scalaris.cookie", "chocolate chip cookie");
        clientName = properties.getProperty("scalaris.client.name", "java_client");
        if (properties.getProperty("scalaris.client.appendUUID", "true").equals("true")) {
            clientNameAppendUUID = true;
        } else {
            clientNameAppendUUID = false;
        }
        configFileUsed = properties.getProperty("PropertyLoader.loadedfile", "");

        //System.out.println("node: " + node);
    }

    /**
     * Creates a connection to a scalaris erlang node specified by the given
     * parameters. Uses the given client name.
     *
     * If <tt>clientNameAppendUUID</tt> is specified a pseudo UUID is appended
     * to the given name. BEWARE that scalaris nodes accept only one connection
     * per client name!
     *
     * @param clientName
     *            the name that identifies the java client
     * @param clientNameAppendUUID
     *            override the object's setting for
     *            {@link #clientNameAppendUUID}
     * @param connectionPolicy
     *            override the connection policy that will be used for the new
     *            connection
     *
     * @return the created connection
     *
     * @throws ConnectionException
     *             if the connection fails
     *
     * @since 2.3
     */
    public Connection createConnection(String clientName,
            final boolean clientNameAppendUUID, final ConnectionPolicy connectionPolicy)
            throws ConnectionException {
        if (clientNameAppendUUID) {
            clientName = clientName + "_" + clientNameUUID.getAndIncrement();
        }
        try {
            final OtpSelf self = new OtpSelf(clientName + "@" + getLocalhostName(), cookie);
            return new Connection(self, connectionPolicy);
        } catch (final Exception e) {
//                 e.printStackTrace();
            throw new ConnectionException(e);
        }
    }

    /**
     * Creates a connection to a scalaris erlang node specified by the given
     * parameters. Uses the given client name.
     *
     * If <tt>clientNameAppendUUID</tt> is specified a pseudo UUID is appended to
     * the given name. BEWARE that scalaris nodes accept only one connection per
     * client name!
     *
     * @param clientName
     *            the name that identifies the java client
     * @param clientNameAppendUUID
     *            override the object's setting for
     *            {@link #clientNameAppendUUID}
     *
     * @return the created connection
     *
     * @throws ConnectionException
     *             if the connection fails
     */
    public Connection createConnection(final String clientName, final boolean clientNameAppendUUID)
            throws ConnectionException {
        return createConnection(clientName, clientNameAppendUUID, connectionPolicy);
    }

    /**
     * Creates a connection to a scalaris erlang node specified by the given
     * parameters. Uses the given client name.
     *
     * If {@link #clientNameAppendUUID} has been set, a pseudo UUID is appended
     * to the given name. BEWARE that scalaris nodes accept only one connection
     * per client name!
     *
     * @param clientName
     *            the name that identifies the java client
     *
     * @return the created connection
     *
     * @throws ConnectionException
     *             if the connection fails
     */
    public Connection createConnection(final String clientName)
            throws ConnectionException {
        return createConnection(clientName, clientNameAppendUUID);
    }

    /**
     * Creates a connection to a scalaris erlang node specified by the given
     * parameters. Uses the given connection policy.
     *
     * If {@link #clientNameAppendUUID} has been set, a pseudo UUID is appended
     * to the given name. BEWARE that scalaris nodes accept only one connection
     * per client name!
     *
     * @param connectionPolicy
     *            override the connection policy that will be used for the new
     *            connection
     *
     * @return the created connection
     *
     * @throws ConnectionException
     *             if the connection fails
     *
     * @since 3.10
     */
    public Connection createConnection(final ConnectionPolicy connectionPolicy)
            throws ConnectionException {
        return createConnection(clientName, clientNameAppendUUID, connectionPolicy);
    }

    /**
     * Creates a connection to a scalaris erlang node specified by the given
     * parameters.
     *
     * @return the created connection
     *
     * @throws ConnectionException
     *             if the connection fails
     */
    public Connection createConnection() throws ConnectionException {
        return createConnection(clientName);
    }

    /**
     * Replaces <tt>localhost</tt> in the node's name to the machine's real host
     * name.
     *
     * Due to a "feature" of OtpErlang >= 1.4.1 erlang nodes are now only
     * reachable by their official host name - so <tt>...@localhost</tt> does
     * not work anymore if there is a real host name.
     *
     * @param node
     *            the name of the node to use
     *
     * @return the node's official host name as returned by
     *         {@link InetAddress#getHostName()}
     */
    public static String fixLocalhostName(final String node) {
        if (node.endsWith("@localhost")) {
            return node.replaceAll("@localhost$", "@" + getLocalhostName());
        } else if (node.indexOf('@') == -1) {
            return node + "@" + getLocalhostName();
        }
        return node;
    }

    /**
     * Returns the name of the local host as determined by
     * {@link #findLocalhostName()}.
     *
     * @return the local host' name or <tt>localhost</tt> if no IP address was
     *         found
     */
    public static final String getLocalhostName() {
        return hostname;
    }

    /**
     * Returns the name of the local host (or at least what Java thinks it is).
     * If the system property <tt>scalaris.erlang.nodename</tt> is set, this
     * will be returned, otherwise we look-up our hostname with
     * {@link InetAddress#getHostName()}.
     *
     * @return the local host' name or <tt>localhost</tt> if no IP address was
     *         found
     */
    private static final String findLocalhostName() {
        String hostname = "localhost";

        final String erlangNodeName = System.getProperty("scalaris.erlang.nodename");
        if ((erlangNodeName == null) || (erlangNodeName.length() == 0)) {
            try {
                hostname = InetAddress.getLocalHost().getCanonicalHostName();
            } catch (final UnknownHostException e) {
            }
        } else {
            hostname = erlangNodeName;
        }

        return hostname;
    }

    /**
     * Prints the object's properties to System.out.
     */
    public void printProperties() {
        printProperties(System.out);
    }

    /**
     * Prints the object's properties to the given output stream.
     *
     * @param out  the output stream to write to
     */
    public void printProperties(final PrintStream out) {
        out.println("ConnectionFactory properties:");
        out.println("  config file                = " + configFileUsed);
        out.println("  scalaris.node              = " + nodes.toString());
        out.println("  scalaris.cookie            = " + cookie);
        out.println("  scalaris.client.name       = " + clientName);
        out.println("  scalaris.client.appendUUID = " + clientNameAppendUUID);
    }

    /**
     * Gets a copy of the list of nodes available for connections.
     *
     * The {@link PeerNode} elements of the list will not be copied!
     *
     * @return a set of nodes
     *
     * @since 2.3
     */
    public List<PeerNode> getNodes() {
        return new ArrayList<PeerNode>(nodes);
    }

    /**
     * Returns the name of the first node available for connection.
     *
     * WARNING: If {@link #nodes} contains multiple nodes, only the first of
     * them will be returned. This is not necessarily the one the class will set
     * up connections with!
     *
     * @return the name of the first node or an empty string if the list is
     *         empty
     *
     * @deprecated This method returns the names of all nodes available for
     *             connection as a String (as previously when only one node was
     *             used), use the new {@link #getNodes()} method instead.
     */
    @Deprecated
    public String getNode() {
        if (nodes.size() > 0) {
            return nodes.get(0).getNode().node();
        } else {
            return "";
        }
    }

    /**
     * Sets the name of the node to connect to.
     *
     * The list of nodes available for connection will be cleared at first, the
     * node will then be added.
     *
     * @param node
     *            the node to set (will not be mangled by
     *            {@link #fixLocalhostName(String)}!)
     *
     * @see ConnectionFactory#addNode(String)
     */
    public void setNode(final String node) {
        this.nodes.clear();
        connectionPolicy.availableNodesReset();
        addNode(node);
    }

    /**
     * Sets the name of the node to connect to.
     *
     * The list of nodes available for connection will be cleared at first, the
     * node will then be added.
     *
     * @param node
     *            the node to set (will not be mangled by
     *            {@link #fixLocalhostName(String)}!)
     *
     * @see ConnectionFactory#addNode(PeerNode)
     *
     * @since 3.16
     */
    public void setNode(final PeerNode node) {
        this.nodes.clear();
        connectionPolicy.availableNodesReset();
        addNode(node);
    }

    /**
     * Adds a node to the set of nodes available for connections.
     *
     * @param node
     *            the node to add (will not be mangled by
     *            {@link #fixLocalhostName(String)}!)
     *
     * @see ConnectionFactory#setNode(String)
     *
     * @since 2.3
     */
    public void addNode(final String node) {
        final PeerNode p = new PeerNode(node);
        addNode(p);
    }

    /**
     * Adds a node to the set of nodes available for connections.
     *
     * @param node
     *            the node to add
     *
     * @see ConnectionFactory#setNode(String)
     *
     * @since 3.16
     */
    public void addNode(final PeerNode node) {
        this.nodes.add(node);
        connectionPolicy.availableNodeAdded(node);
    }

    /**
     * Removes a node from the set of nodes available for connections.
     *
     * @param node
     *            the node to remove
     *
     * @see ConnectionFactory#getNodes()
     *
     * @since 2.3
     */
    public void removeNode(final PeerNode node) {
        this.nodes.remove(node);
        connectionPolicy.availableNodeRemoved(node);
    }

    /**
     * (Re-)evaluates all currently set nodes by trying to establish a
     * connection to each of them.
     *
     * Note: This resets all {@link PeerNode} statistics collected before.
     *
     * @since 3.20
     */
    public void testAllNodes() {
        connectionPolicy.availableNodesReset();
        for (final PeerNode node: nodes) {
            node.resetFailureCount();
            try {
                createConnection(new FixedNodeConnectionPolicy(node)).close();
            } catch (final ConnectionException e) {
            }
            connectionPolicy.availableNodeAdded(node);
        }
    }

    /**
     * Returns the cookie name to use for connections.
     *
     * @return the cookie
     */
    public String getCookie() {
        return cookie;
    }

    /**
     * Sets the cookie name to use for connections.
     *
     * @param cookie
     *            the cookie to set
     */
    public void setCookie(final String cookie) {
        this.cookie = cookie;
    }

    /**
     * Returns the name of the (Java) client to use when establishing a
     * connection with erlang.
     *
     * @return the clientName
     */
    public String getClientName() {
        return clientName;
    }

    /**
     * Sets the name of the (Java) client to use when establishing a connection
     * with erlang.
     *
     * @param clientName
     *            the clientName to set
     */
    public void setClientName(final String clientName) {
        this.clientName = clientName;
    }

    /**
     * Returns whether an UUID is appended to client names or not.
     *
     * @return <tt>true</tt> if an UUID is appended, <tt>false</tt> otherwise
     */
    public boolean isClientNameAppendUUID() {
        return clientNameAppendUUID;
    }

    /**
     * Sets whether to append an UUID to client names or not.
     *
     * @param clientNameAppendUUID
     *            <tt>true</tt> if an UUID is appended, <tt>false</tt> otherwise
     */
    public void setClientNameAppendUUID(final boolean clientNameAppendUUID) {
        this.clientNameAppendUUID = clientNameAppendUUID;
    }

    /**
     * Sets the connection policy to use for new connections.
     *
     * Warning: this will effectively disconnect the previous connection policy
     * from any updates to the list of available nodes (they will still use the
     * same list but will not be able to react on changes). Prefer not to change
     * the default policy after connections have been created.
     *
     * @param connectionPolicy
     *            the connection policy to set
     *
     * @since 2.3
     */
    public void setConnectionPolicy(final ConnectionPolicy connectionPolicy) {
        this.connectionPolicy = connectionPolicy;
    }

    /**
     * Gets the current connection policy.
     *
     * @return the currently used connection policy
     *
     * @since 2.3
     */
    public ConnectionPolicy getConnectionPolicy() {
        return connectionPolicy;
    }
}
