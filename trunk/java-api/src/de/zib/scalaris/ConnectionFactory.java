/**
 * 
 */
package de.zib.scalaris;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import com.ericsson.otp.erlang.OtpConnection;
import com.ericsson.otp.erlang.OtpPeer;
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
 * @author Nico Kruber, kruber@zib.de
 * @version 2.0
 * @since 2.0
 */
public class ConnectionFactory {
	/**
	 * The name of the node to connect to.
	 */
	private String node;
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
	 * @todo In some cases, an atomic value is not necessary, e.g. when the
	 *       factory is used by only one thread, then a normal long value will
	 *       suffice. Possible optimisation: use {@link AtomicLong} for
	 *       singleton instance and {@link Long} for the other instances!
	 */
	private AtomicLong clientNameUUID = new AtomicLong(0);

	/**
	 * Static instance of a connection factory.
	 */
	private static ConnectionFactory instance = new ConnectionFactory();

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
	 * values given in a {@code scalaris.properties} file and falls back to
	 * default values if values don't exist.
	 * 
	 * Default values are:
	 * <ul>
	 * <li>{@code scalaris.node = "boot@localhost"}</li>
	 * <li>{@code scalaris.cookie = "chocolate chip cookie"}</li>
	 * <li>{@code scalaris.client.name = "java_client"}</li>
	 * <li>{@code scalaris.client.appendUUID = "true"}</li>
	 * </ul>
	 */
	public ConnectionFactory() {
		Properties properties = new Properties();
		PropertyLoader.loadProperties(properties, "scalaris.properties");

		node = properties.getProperty("scalaris.node", "boot@localhost");
		cookie = properties.getProperty("scalaris.cookie", "chocolate chip cookie");
		clientName = properties.getProperty("scalaris.client.name", "java_client");
		if (properties.getProperty("scalaris.client.appendUUID", "true")
				.equals("true")) {
			clientNameAppendUUID = true;
		} else {
			clientNameAppendUUID = false;
		}
	}

	/**
	 * Creates a connection to a scalaris erlang node specified by the given
	 * parameters. Uses the given client name.
	 * 
	 * If {@code clientNameAppendUUID} is specified a pseudo UUID is appended to
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
	public OtpConnection createConnection(String clientName, boolean clientNameAppendUUID)
			throws ConnectionException {
		try {
			if (clientNameAppendUUID) {
				clientName = clientName + "_" + clientNameUUID.getAndIncrement();
			}
			OtpSelf self = new OtpSelf(clientName, cookie);
			OtpPeer other = new OtpPeer(node);
			return self.connect(other);
		} catch (Exception e) {
			// e.printStackTrace();
			throw new ConnectionException(e.getMessage());
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
	public OtpConnection createConnection(String clientName)
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
	public OtpConnection createConnection() throws ConnectionException {
		return createConnection(clientName);
	}

	/**
	 * Returns the name of the node to connect to.
	 * 
	 * @return the name of the node
	 */
	public String getNode() {
		return node;
	}

	/**
	 * Sets the name of the node to connect to.
	 * 
	 * @param node
	 *            the node to set
	 */
	public void setNode(String node) {
		this.node = node;
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
	 * @return {@code true} if an UUID is appended, {@code false} otherwise
	 */
	public boolean isClientNameAppendUUID() {
		return clientNameAppendUUID;
	}

	/**
	 * Sets whether to append an UUID to client names or not.
	 * 
	 * @param clientNameAppendUUID
	 *            {@code true} if an UUID is appended, {@code false} otherwise
	 */
	public void setClientNameAppendUUID(boolean clientNameAppendUUID) {
		this.clientNameAppendUUID = clientNameAppendUUID;
	}
}
