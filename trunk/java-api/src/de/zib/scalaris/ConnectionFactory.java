/**
 *  Copyright 2007-2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
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

import de.zib.scalaris.ConnectionException;
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
 * A user-defined {@link Properties} object can also be used by creating objects
 * with {@link #ConnectionFactory(Properties)} or setting the new values with
 * {@link #setProperties(Properties)} but must provide the following values
 * (default values as shown)
 * <ul>
 * <li><tt>scalaris.node = "boot@localhost"</tt></li>
 * <li><tt>scalaris.cookie = "chocolate chip cookie"</tt></li>
 * <li><tt>scalaris.client.name = "java_client"</tt></li>
 * <li><tt>scalaris.client.appendUUID = "true"</tt></li>
 * </ul>
 * 
 * @author Nico Kruber, kruber@zib.de
 * @version 2.1
 * @since 2.0
 */
public class ConnectionFactory {
	/**
	 * Default name of the configuration file.
	 */
	private static final String defaultConfigFile = "scalaris.properties";
	
	/**
	 * The name of the node to connect to.
	 */
	private List<PeerNode> nodes = Collections.synchronizedList(new ArrayList<PeerNode>());
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
	private AtomicLong clientNameUUID = new AtomicLong(0);
	
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
	 * <li><tt>scalaris.node = "boot@localhost"</tt></li>
	 * <li><tt>scalaris.cookie = "chocolate chip cookie"</tt></li>
	 * <li><tt>scalaris.client.name = "java_client"</tt></li>
	 * <li><tt>scalaris.client.appendUUID = "true"</tt></li>
	 * </ul>
	 */
	public ConnectionFactory() {
		Properties properties = new Properties();
		String configFile = System.getProperty("scalaris.java.config");
		if (configFile == null || configFile.length() == 0) {
			configFile = defaultConfigFile;
		}
//		System.out.println("loading config file: " + configFile);
		PropertyLoader.loadProperties(properties, configFile);
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
	 * <li><tt>scalaris.node = "boot@localhost"</tt></li>
	 * <li><tt>scalaris.cookie = "chocolate chip cookie"</tt></li>
	 * <li><tt>scalaris.client.name = "java_client"</tt></li>
	 * <li><tt>scalaris.client.appendUUID = "true"</tt></li>
	 * </ul>
	 * 
	 * @param properties
	 */
	public ConnectionFactory(Properties properties) {
		setProperties(properties);
	}

	/**
	 * Sets the object's members used for creating connections to erlang to
	 * values provided by the given {@link Properties} object.
	 * 
	 * The {@link Properties} object should provide the following values
	 * (default values as shown):
	 * <ul>
	 * <li><tt>scalaris.node = "boot@localhost"</tt></li>
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
	public void setProperties(Properties properties) {
		String[] nodesTemp = properties.getProperty("scalaris.node", "boot@localhost").split("[\\s,;]");
		nodes.clear();
		
		for (int i = 0; i < nodesTemp.length; ++i) {
			nodes.add(new PeerNode(fixLocalhostName(nodesTemp[i])));
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
	public Connection createConnection(String clientName, boolean clientNameAppendUUID)
			throws ConnectionException {
		try {
			if (clientNameAppendUUID) {
				clientName = clientName + "_" + clientNameUUID.getAndIncrement();
			}
			OtpSelf self = new OtpSelf(clientName, cookie);
			return new Connection(self, connectionPolicy);
		} catch (Exception e) {
//		         e.printStackTrace();
			throw new ConnectionException(e);
		}
	}

	/**
	 * Creates a connection to a scalaris erlang node specified by the given
	 * parameters. Uses the given client name.
	 * 
	 * If {@link #clientNameAppendUUID} is specified a pseudo UUID is appended
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
	public Connection createConnection(String clientName)
			throws ConnectionException {
		return createConnection(clientName, clientNameAppendUUID);
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
	 *         {@link InetAddress#getCanonicalHostName()}
	 */
	public static String fixLocalhostName(String node) {
		if (node.endsWith("@localhost")) {
			String hostname = "localhost";
			try {
				InetAddress addr = InetAddress.getLocalHost();
				hostname = addr.getCanonicalHostName();
			} catch (UnknownHostException e) {
			}
			return node.replaceAll("@localhost$", "@" + hostname);
		}
		return node;
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
	public void printProperties(PrintStream out) {
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
	public void setNode(String node) {
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
	public void addNode(String node) {
		PeerNode p = new PeerNode(node);
		this.nodes.add(p);
		connectionPolicy.availableNodeAdded(p);
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
	public void removeNode(PeerNode node) {
		this.nodes.remove(node);
		connectionPolicy.availableNodeRemoved(node);
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
	public void setCookie(String cookie) {
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
	public void setClientName(String clientName) {
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
	public void setClientNameAppendUUID(boolean clientNameAppendUUID) {
		this.clientNameAppendUUID = clientNameAppendUUID;
	}

	/**
	 * Sets the connection policy to use for new connections.
	 * 
	 * @param connectionPolicy the connection policy to set
	 * 
	 * @since 2.3
	 */
	public void setConnectionPolicy(ConnectionPolicy connectionPolicy) {
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
