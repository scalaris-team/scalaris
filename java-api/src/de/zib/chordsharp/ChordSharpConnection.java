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
package de.zib.chordsharp;

import java.util.Properties;
import java.util.Vector;

import com.ericsson.otp.erlang.OtpConnection;
import com.ericsson.otp.erlang.OtpErlangList;
import com.ericsson.otp.erlang.OtpErlangObject;
import com.ericsson.otp.erlang.OtpErlangString;

import de.zib.scalaris.ConnectionFactory;
import de.zib.scalaris.Scalaris;
import de.zib.tools.PropertyLoader;

/**
 * Provides methods to read and write key/value pairs to a chordsharp ring.
 * 
 * Each operation is a single transaction. If you are looking for more
 * transactions, use the {@link Transaction} class instead.
 * 
 * <p>
 * It reads the connection parameters from a file called
 * {@code ChordSharpConnection.properties} or uses default properties defined in
 * {@link ChordSharpConnection#defaultProperties}.
 * </p>
 * 
 * <p>
 * Each method is provided in a static manner ({@code read}, {@code write},
 * {@code publish}, {@code subscribe}, {@code getSubscribers}) which uses a
 * static connection to the chordsharp ring generated at program start which all
 * calls in a jvm share.<br />
 * Instantiated objects generate their own connection and non-static methods ({@code singleRead},
 * {@code singleWrite}, {@code singlePublish}, {@code singleSubscribe},
 * {@code singleGetSubscribers}) use the object's connection to communicate
 * with chordsharp.
 * </p>
 * 
 * <h3>Reading values</h3>
 * <code style="white-space:pre;">
 *   OtpErlangString otpKey;
 *   OtpErlangString otpValue;
 *   
 *   String key;
 *   String value;
 *   
 *   // static:
 *   otpValue = ChordSharpConnection.readString(otpKey); // {@link #readString(OtpErlangString)}
 *   value    = ChordSharpConnection.readString(key);    // {@link #readString(String)}
 *   
 *   // non-static:
 *   ChordSharpConnection cs = new ChordSharpConnection();
 *   otpValue = cs.singleReadString(otpKey); // {@link #singleReadString(OtpErlangString)}
 *   value    = cs.singleReadString(key);    // {@link #singleReadString(String)}
 * </code>
 * 
 * <p>For the full example, see {@link de.zib.chordsharp.examples.ChordSharpConnectionReadExample}</p>
 * 
 * <h3>Writing values</h3>
 * <code style="white-space:pre;">
 *   OtpErlangString otpKey;
 *   OtpErlangString otpValue;
 *   
 *   String key;
 *   String value;
 *   
 *   // static:
 *   ChordSharpConnection.write(otpKey, otpValue); // {@link #write(OtpErlangString, OtpErlangObject)}
 *   ChordSharpConnection.write(key, value);       // {@link #write(String, String)}
 *   
 *   // non-static:
 *   ChordSharpConnection cs = new ChordSharpConnection();
 *   cs.singleWrite(otpKey, otpValue); // {@link #singleWrite(OtpErlangString, OtpErlangObject)}
 *   cs.singleWrite(key, value);       // {@link #singleWrite(String, String)}
 * </code>
 * 
 * <p>For the full example, see {@link de.zib.chordsharp.examples.ChordSharpConnectionWriteExample}</p>
 * 
 * <h3>Publishing topics</h3>
 * <code style="white-space:pre;">
 *   OtpErlangString otpTopic;
 *   OtpErlangString otpContent;
 *   
 *   String topic;
 *   String content;
 *   
 *   // static:
 *   ChordSharpConnection.publish(otpTopic, otpContent); // {@link #publish(OtpErlangString, OtpErlangString)}
 *   ChordSharpConnection.publish(topic, content);       // {@link #publish(String, String)}
 *   
 *   // non-static:
 *   ChordSharpConnection cs = new ChordSharpConnection();
 *   cs.singlePublish(otpTopic, otpContent); // {@link #singlePublish(OtpErlangString, OtpErlangString)}
 *   cs.singlePublish(topic, content);       // {@link #singlePublish(String, String)}
 * </code>
 * 
 * <p>For the full example, see {@link de.zib.chordsharp.examples.ChordSharpConnectionPublishExample}</p>
 * 
 * <h3>Subscribing to topics</h3>
 * <code style="white-space:pre;">
 *   OtpErlangString otpTopic;
 *   OtpErlangString otpURL;
 *   
 *   String topic;
 *   String URL;
 *   
 *   // static:
 *   ChordSharpConnection.subscribe(otpTopic, otpURL); // {@link #subscribe(OtpErlangString, OtpErlangString)}
 *   ChordSharpConnection.subscribe(topic, URL);       // {@link #subscribe(String, String)}
 *   
 *   // non-static:
 *   ChordSharpConnection cs = new ChordSharpConnection();
 *   cs.singleSubscribe(otpTopic, otpURL); // {@link #singleSubscribe(OtpErlangString, OtpErlangString)}
 *   cs.singleSubscribe(topic, URL);       // {@link #singleSubscribe(String, String)}
 * </code>
 * 
 * <p>For the full example, see {@link de.zib.chordsharp.examples.ChordSharpConnectionSubscribeExample}</p>
 * 
 * <h3>Unsubscribing from topics</h3>
 * 
 * Unsubscribing from topics works like subscribing to topics with the exception
 * of a {@link NotFoundException} being thrown if either the topic does not
 * exist or the URL is not subscribed to the topic.
 * 
 * <code style="white-space:pre;">
 *   OtpErlangString otpTopic;
 *   OtpErlangString otpURL;
 *   
 *   String topic;
 *   String URL;
 *   
 *   // static:
 *   ChordSharpConnection.unsubscribe(otpTopic, otpURL); // {@link #unsubscribe(OtpErlangString, OtpErlangString)}
 *   ChordSharpConnection.unsubscribe(topic, URL);       // {@link #unsubscribe(String, String)}
 *   
 *   // non-static:
 *   ChordSharpConnection cs = new ChordSharpConnection();
 *   cs.singleUnsubscribe(otpTopic, otpURL); // {@link #singleUnsubscribe(OtpErlangString, OtpErlangString)}
 *   cs.singleUnsubscribe(topic, URL);       // {@link #singleUnsubscribe(String, String)}
 * </code>
 * 
 * <h3>Getting a list of subscribers to a topic</h3>
 * <code style="white-space:pre;">
 *   OtpErlangString otpTopic;
 *   String topic;
 *   
 *   OtpErlangList otpSubscribers;
 *   List&lt;String&gt; subscribers;
 *   
 *   // static:
 *   otpSubscribers = ChordSharpConnection.getSubscribers(otpTopic); // {@link #getSubscribers(OtpErlangString)}
 *   subscribers    = ChordSharpConnection.getSubscribers(topic);    // {@link #getSubscribers(String)}
 *   
 *   // non-static:
 *   ChordSharpConnection cs = new ChordSharpConnection();
 *   otpSubscribers = cs.singleGetSubscribers(otpTopic); // {@link #singleGetSubscribers(OtpErlangString)}
 *   subscribers    = cs.singleGetSubscribers(topic);    // {@link #singleGetSubscribers(String)}
 * </code>
 * 
 * <p>For the full example, see {@link de.zib.chordsharp.examples.ChordSharpConnectionGetSubscribersExample}</p>
 * 
 * @author Nico Kruber, kruber@zib.de
 * @version 2.0
 * 
 * @deprecated use {@link Scalaris} class instead
 */
@Deprecated
public class ChordSharpConnection {
	/**
	 * the connection to a chorsharp node
	 */
	private Scalaris connection = null;
	
	/**
	 * the static connection to a chordsharp node to be used by the static
	 * methods
	 */
	static private Scalaris staticConnection = null;

	/**
	 * The default settings to use (set in static initialiser). The settings
	 * will get overridden by the contents of the {@code
	 * ChordSharpConnection.properties} file.
	 * 
	 * <ul>
	 * <li>{@code cs.node = "boot@localhost"}</li>
	 * <li>{@code cs.cookie = "chocolate chip cookie"}</li>
	 * </ul>
	 */
	static Properties defaultProperties = new Properties();

	/**
	 * static initialiser: sets default values for the chordsharp connection and
	 * initialises the static connection
	 */
	static {
		ChordSharpConnection.defaultProperties.setProperty("cs.node", "boot@localhost");
		ChordSharpConnection.defaultProperties.setProperty("cs.cookie",
				"chocolate chip cookie");
		
		// load properties from file:
		PropertyLoader.loadProperties(defaultProperties, "ChordSharpConnection.properties");
		try {
			staticConnection = new Scalaris(ChordSharpConnection.createConnection(defaultProperties));
		} catch (de.zib.scalaris.ConnectionException e) {
			 e.printStackTrace();
		} catch (ConnectionException e) {
			 e.printStackTrace();
		}
	}

	/**
	 * Creates the object's connection to the chordsharp node specified in the
	 * {@code "ChordSharpConnection.properties"}.
	 * 
	 * @throws ConnectionException
	 *             if the connection fails
	 */
	public ChordSharpConnection() throws ConnectionException {
		Properties properties = new Properties(defaultProperties);
		PropertyLoader.loadProperties(properties, "ChordSharpConnection.properties");
		try {
			connection = new Scalaris(ChordSharpConnection.createConnection(properties));
		} catch (de.zib.scalaris.ConnectionException e) {
			throw new ConnectionException(e);
		}
	}

	/**
	 * Sets up the connection to the chordsharp erlang node specified by the
	 * given parameters. Uses a UUID to make the client's name unique to the
	 * chordsharp node which only accepts one connection per client name.
	 * 
	 * @param node
	 *            the chordsharp node to connect to
	 * @param cookie
	 *            the cookie the chordsharp node uses for connections
	 * 
	 * @return the created connection
	 * @throws ConnectionException
	 *             if the connection fails
	 */
	static OtpConnection createConnection(String node, String cookie)
			throws ConnectionException {
		try {
			/*
			 * only one connection per client name is allowed, so the name is
			 * made unique with UUIDs.
			 */
			return ConnectionFactory.getInstance().createConnection("java_client", true);
		} catch (Exception e) {
//			e.printStackTrace();
			throw new ConnectionException(e);
		}
	}

	/**
	 * Convenience method to create the connection to the erlang node with a Properties object.
	 * 
	 * @param properties
	 *            a Properties object that contains the boot server's name and
	 *            cookie
	 * @return the created connection
	 * @throws ConnectionException
	 *             if the connection fails
	 */
	static OtpConnection createConnection(Properties properties)
			throws ConnectionException {
		String node = properties.getProperty("cs.node");
		String cookie = properties.getProperty("cs.cookie");
		return ChordSharpConnection.createConnection(node, cookie);
	}
	
	// /////////////////////////////
	// read methods
	// /////////////////////////////
	
	/**
	 * Gets the value stored under the given {@code key}. Uses the given
	 * {@code connection}.
	 * 
	 * @param connection
	 *            the connection to perform the operation on
	 * @param key
	 *            the key to look up
	 * @return the value stored under the given {@code key}
	 * @throws ConnectionException
	 *             if the connection is not active or a communication error
	 *             occurs or an exit signal was received or the remote node
	 *             sends a message containing an invalid cookie
	 * @throws TimeoutException
	 *             if a timeout occurred while trying to fetch the value
	 * @throws NotFoundException
	 *             if the requested key does not exist
	 * @throws UnknownException
	 *             if any other error occurs
	 */
	private static OtpErlangObject read(Scalaris connection,
			OtpErlangString key) throws ConnectionException, TimeoutException,
			UnknownException, NotFoundException {
		if (connection == null) {
			throw new ConnectionException("No connection.");
		}
		try {
			return connection.readObject(key);
		} catch (de.zib.scalaris.ConnectionException e) {
			// e.printStackTrace();
			throw new ConnectionException(e);
		} catch (de.zib.scalaris.TimeoutException e) {
			// e.printStackTrace();
			throw new TimeoutException(e);
		} catch (de.zib.scalaris.UnknownException e) {
			// e.printStackTrace();
			throw new UnknownException(e);
		} catch (de.zib.scalaris.NotFoundException e) {
			// e.printStackTrace();
			throw new NotFoundException(e);
		}
	}
	
	/**
	 * Gets the value stored under the given {@code key}. Uses the given
	 * {@code connection}.
	 * 
	 * @param connection
	 *            the connection to perform the operation on
	 * @param key
	 *            the key to look up
	 * @return the value stored under the given {@code key}
	 * @throws ConnectionException
	 *             if the connection is not active or a communication error
	 *             occurs or an exit signal was received or the remote node
	 *             sends a message containing an invalid cookie
	 * @throws TimeoutException
	 *             if a timeout occurred while trying to fetch the value
	 * @throws NotFoundException
	 *             if the requested key does not exist
	 * @throws UnknownException
	 *             if any other error occurs
	 * 
	 * @since 2.0
	 */
	private static String read(Scalaris connection, String key)
			throws ConnectionException, TimeoutException, UnknownException,
			NotFoundException {
		if (connection == null) {
			throw new ConnectionException("No connection.");
		}
		try {
			return connection.read(key);
		} catch (de.zib.scalaris.ConnectionException e) {
			// e.printStackTrace();
			throw new ConnectionException(e);
		} catch (de.zib.scalaris.TimeoutException e) {
			// e.printStackTrace();
			throw new TimeoutException(e);
		} catch (de.zib.scalaris.UnknownException e) {
			// e.printStackTrace();
			throw new UnknownException(e);
		} catch (de.zib.scalaris.NotFoundException e) {
			// e.printStackTrace();
			throw new NotFoundException(e);
		}
	}

	/**
	 * Gets the value stored under the given {@code key}. Uses the object's
	 * {@link #connection}.
	 * 
	 * @param key
	 *            the key to look up
	 * @return the (string) value stored under the given {@code key}
	 * @throws ConnectionException
	 *             if the connection is not active or a communication error
	 *             occurs or an exit signal was received or the remote node
	 *             sends a message containing an invalid cookie
	 * @throws TimeoutException
	 *             if a timeout occurred while trying to fetch the value
	 * @throws NotFoundException
	 *             if the requested key does not exist
	 * @throws UnknownException
	 *             if any other error occurs
	 * @see #read(Scalaris, OtpErlangString)
	 */
	public OtpErlangString singleReadString(OtpErlangString key)
			throws ConnectionException, TimeoutException, UnknownException,
			NotFoundException {
		try {
			return (OtpErlangString) read(connection, key);
		} catch (ClassCastException e) {
			// e.printStackTrace();
			throw new UnknownException(e.getMessage());
		}
	}

	/**
	 * Gets the value stored under the given {@code key}. Uses the object's
	 * {@link #connection}.
	 * 
	 * @param key
	 *            the key to look up
	 * @return the (string) value stored under the given {@code key}
	 * @throws ConnectionException
	 *             if the connection is not active or a communication error
	 *             occurs or an exit signal was received or the remote node
	 *             sends a message containing an invalid cookie
	 * @throws TimeoutException
	 *             if a timeout occurred while trying to fetch the value
	 * @throws NotFoundException
	 *             if the requested key does not exist
	 * @throws UnknownException
	 *             if any other error occurs
	 * @see #read(Scalaris, String)
	 */
	public String singleReadString(String key) throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException {
		return read(connection, key);
	}
	
	/**
	 * Gets the value stored under the given {@code key}. Uses the object's
	 * {@link #connection}.
	 * 
	 * @param key
	 *            the key to look up
	 * @return the value stored under the given {@code key} as a raw erlang type
	 * @throws ConnectionException
	 *             if the connection is not active or a communication error
	 *             occurs or an exit signal was received or the remote node
	 *             sends a message containing an invalid cookie
	 * @throws TimeoutException
	 *             if a timeout occurred while trying to fetch the value
	 * @throws NotFoundException
	 *             if the requested key does not exist
	 * @throws UnknownException
	 *             if any other error occurs
	 * @see #read(Scalaris, OtpErlangString)
	 */
	public OtpErlangObject singleReadObject(OtpErlangString key)
			throws ConnectionException, TimeoutException, UnknownException,
			NotFoundException {
		return read(connection, key);
	}

	/**
	 * Gets the value stored under the given {@code key}. Uses the static
	 * {@link #staticConnection}.
	 * 
	 * @param key
	 *            the key to look up
	 * @return the (string) value stored under the given {@code key}
	 * @throws ConnectionException
	 *             if the connection is not active or a communication error
	 *             occurs or an exit signal was received or the remote node
	 *             sends a message containing an invalid cookie
	 * @throws TimeoutException
	 *             if a timeout occurred while trying to fetch the value
	 * @throws NotFoundException
	 *             if the requested key does not exist
	 * @throws UnknownException
	 *             if any other error occurs
	 * @see #read(Scalaris, OtpErlangString)
	 */
	public static OtpErlangString readString(OtpErlangString key)
			throws ConnectionException, TimeoutException, UnknownException,
			NotFoundException {
		try {
			return (OtpErlangString) read(staticConnection, key);
		} catch (ClassCastException e) {
			// e.printStackTrace();
			throw new UnknownException(e.getMessage());
		}
	}

	/**
	 * Gets the value stored under the given {@code key}. Uses the static
	 * {@link #staticConnection}.
	 * 
	 * @param key
	 *            the key to look up
	 * @return the (string) value stored under the given {@code key}
	 * @throws ConnectionException
	 *             if the connection is not active or a communication error
	 *             occurs or an exit signal was received or the remote node
	 *             sends a message containing an invalid cookie
	 * @throws TimeoutException
	 *             if a timeout occurred while trying to fetch the value
	 * @throws NotFoundException
	 *             if the requested key does not exist
	 * @throws UnknownException
	 *             if any other error occurs
	 * @see #read(Scalaris, String)
	 */
	public static String readString(String key) throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException {
		return read(staticConnection, key);
	}
	
	/**
	 * Gets the value stored under the given {@code key}. Uses the static
	 * {@link #staticConnection}.
	 * 
	 * @param key
	 *            the key to look up
	 * @return the value stored under the given {@code key} as a raw erlang type
	 * @throws ConnectionException
	 *             if the connection is not active or a communication error
	 *             occurs or an exit signal was received or the remote node
	 *             sends a message containing an invalid cookie
	 * @throws TimeoutException
	 *             if a timeout occurred while trying to fetch the value
	 * @throws NotFoundException
	 *             if the requested key does not exist
	 * @throws UnknownException
	 *             if any other error occurs
	 * @see #read(Scalaris, OtpErlangString)
	 */
	public static OtpErlangObject readObject(OtpErlangString key)
			throws ConnectionException, TimeoutException, UnknownException,
			NotFoundException {
		return read(staticConnection, key);
	}
	
	// /////////////////////////////
	// write methods
	// /////////////////////////////

	/**
	 * Stores the given {@code key}/{@code value} pair. Uses the given
	 * {@code connection}.
	 * 
	 * @param connection
	 *            the connection to perform the operation on
	 * @param key
	 *            the key to store the value for
	 * @param value
	 *            the value to store
	 * @throws ConnectionException
	 *             if the connection is not active or a communication error
	 *             occurs or an exit signal was received or the remote node
	 *             sends a message containing an invalid cookie
	 * @throws TimeoutException
	 *             if a timeout occurred while trying to write the value
	 * @throws UnknownException
	 *             if any other error occurs
	 */
	private static void write(Scalaris connection, OtpErlangString key,
			OtpErlangObject value) throws ConnectionException,
			TimeoutException, UnknownException {
		if (connection == null) {
			throw new ConnectionException("No connection.");
		}
		try {
			connection.writeObject(key, value);
		} catch (de.zib.scalaris.ConnectionException e) {
			// e.printStackTrace();
			throw new ConnectionException(e);
		} catch (de.zib.scalaris.TimeoutException e) {
			// e.printStackTrace();
			throw new TimeoutException(e);
		} catch (de.zib.scalaris.UnknownException e) {
			// e.printStackTrace();
			throw new UnknownException(e);
		}
	}
	
	/**
	 * Stores the given {@code key}/{@code value} pair. Uses the given
	 * {@code connection}.
	 * 
	 * @param connection
	 *            the connection to perform the operation on
	 * @param key
	 *            the key to store the value for
	 * @param value
	 *            the value to store
	 * @throws ConnectionException
	 *             if the connection is not active or a communication error
	 *             occurs or an exit signal was received or the remote node
	 *             sends a message containing an invalid cookie
	 * @throws TimeoutException
	 *             if a timeout occurred while trying to write the value
	 * @throws UnknownException
	 *             if any other error occurs
	 * 
	 * @since 2.0
	 */
	private static void write(Scalaris connection, String key, String value)
			throws ConnectionException, TimeoutException, UnknownException {
		if (connection == null) {
			throw new ConnectionException("No connection.");
		}
		try {
			connection.write(key, value);
		} catch (de.zib.scalaris.ConnectionException e) {
			// e.printStackTrace();
			throw new ConnectionException(e);
		} catch (de.zib.scalaris.TimeoutException e) {
			// e.printStackTrace();
			throw new TimeoutException(e);
		} catch (de.zib.scalaris.UnknownException e) {
			// e.printStackTrace();
			throw new UnknownException(e);
		}
	}

	/**
	 * Stores the given {@code key}/{@code value} pair. Uses the object's
	 * {@link #connection}.
	 * 
	 * @param key
	 *            the key to store the value for
	 * @param value
	 *            the value to store
	 * @throws ConnectionException
	 *             if the connection is not active or a communication error
	 *             occurs or an exit signal was received or the remote node
	 *             sends a message containing an invalid cookie
	 * @throws TimeoutException
	 *             if a timeout occurred while trying to write the value
	 * @throws UnknownException
	 *             if any other error occurs
	 * @see #write(Scalaris, OtpErlangString, OtpErlangObject)
	 */
	public void singleWrite(OtpErlangString key, OtpErlangObject value)
			throws ConnectionException, TimeoutException, UnknownException {
		write(connection, key, value);
	}

	/**
	 * Stores the given {@code key}/{@code value} pair. Uses the object's
	 * {@link #connection}.
	 * 
	 * @param key
	 *            the key to store the value for
	 * @param value
	 *            the value to store
	 * @throws ConnectionException
	 *             if the connection is not active or a communication error
	 *             occurs or an exit signal was received or the remote node
	 *             sends a message containing an invalid cookie
	 * @throws TimeoutException
	 *             if a timeout occurred while trying to write the value
	 * @throws UnknownException
	 *             if any other error occurs
	 * @see #write(Scalaris, String, String)
	 */
	public void singleWrite(String key, String value)
			throws ConnectionException, TimeoutException, UnknownException {
		write(connection, key, value);
	}

	/**
	 * Stores the given {@code key}/{@code value} pair. Uses the static
	 * {@link #staticConnection}.
	 * 
	 * @param key
	 *            the key to store the value for
	 * @param value
	 *            the value to store
	 * @throws ConnectionException
	 *             if the connection is not active or a communication error
	 *             occurs or an exit signal was received or the remote node
	 *             sends a message containing an invalid cookie
	 * @throws TimeoutException
	 *             if a timeout occurred while trying to write the value
	 * @throws UnknownException
	 *             if any other error occurs
	 * @see #write(Scalaris, OtpErlangString, OtpErlangObject)
	 */
	public static void write(OtpErlangString key, OtpErlangObject value)
			throws ConnectionException, TimeoutException, UnknownException {
		write(staticConnection, key, value);
	}

	/**
	 * Stores the given {@code key}/{@code value} pair. Uses the static
	 * {@link #staticConnection}.
	 * 
	 * @param key
	 *            the key to store the value for
	 * @param value
	 *            the value to store
	 * @throws ConnectionException
	 *             if the connection is not active or a communication error
	 *             occurs or an exit signal was received or the remote node
	 *             sends a message containing an invalid cookie
	 * @throws TimeoutException
	 *             if a timeout occurred while trying to write the value
	 * @throws UnknownException
	 *             if any other error occurs
	 * @see #write(Scalaris, String, String)
	 */
	public static void write(String key, String value)
			throws ConnectionException, TimeoutException, UnknownException {
		write(staticConnection, key, value);
	}

	// /////////////////////////////
	// publish methods
	// /////////////////////////////
	
	/**
	 * Publishes an event under a given {@code topic}. Uses the given
	 * {@code connection}.
	 * 
	 * Note: The specification of {@code pubsub.pubsub_api:publish/2} states
	 * that the only returned value is {@code ok}, so no further evaluation is
	 * necessary.
	 * 
	 * @param connection
	 *            the connection to perform the operation on
	 * @param topic
	 *            the topic to publish the content under
	 * @param content
	 *            the content to publish
	 * @throws ConnectionException
	 *             if the connection is not active or a communication error
	 *             occurs or an exit signal was received or the remote node
	 *             sends a message containing an invalid cookie
	 */
	private static void publish(Scalaris connection, OtpErlangString topic,
			OtpErlangString content) throws ConnectionException {
		if (connection == null) {
			throw new ConnectionException("No connection.");
		}
		try {
			connection.publish(topic, content);
		} catch (de.zib.scalaris.ConnectionException e) {
			// e.printStackTrace();
			throw new ConnectionException(e);
		}
	}
	
	/**
	 * Publishes an event under a given {@code topic}. Uses the given
	 * {@code connection}.
	 * 
	 * Note: The specification of {@code pubsub.pubsub_api:publish/2} states
	 * that the only returned value is {@code ok}, so no further evaluation is
	 * necessary.
	 * 
	 * @param connection
	 *            the connection to perform the operation on
	 * @param topic
	 *            the topic to publish the content under
	 * @param content
	 *            the content to publish
	 * @throws ConnectionException
	 *             if the connection is not active or a communication error
	 *             occurs or an exit signal was received or the remote node
	 *             sends a message containing an invalid cookie
	 * 
	 * @since 2.0
	 */
	private static void publish(Scalaris connection, String topic,
			String content) throws ConnectionException {
		if (connection == null) {
			throw new ConnectionException("No connection.");
		}
		try {
			connection.publish(topic, content);
		} catch (de.zib.scalaris.ConnectionException e) {
			// e.printStackTrace();
			throw new ConnectionException(e);
		}
	}

	/**
	 * Publishes an event under a given {@code topic}. Uses the object's
	 * {@link #connection}.
	 * 
	 * @param topic
	 *            the topic to publish the content under
	 * @param content
	 *            the content to publish
	 * @throws ConnectionException
	 *             if the connection is not active or a communication error
	 *             occurs or an exit signal was received or the remote node
	 *             sends a message containing an invalid cookie
	 * @see #publish(Scalaris, OtpErlangString, OtpErlangString)
	 */
	public void singlePublish(OtpErlangString topic, OtpErlangString content)
			throws ConnectionException {
		publish(connection, topic, content);
	}

	/**
	 * Publishes an event under a given {@code topic}. Uses the object's
	 * {@link #connection}.
	 * 
	 * @param topic
	 *            the topic to publish the content under
	 * @param content
	 *            the content to publish
	 * @throws ConnectionException
	 *             if the connection is not active or a communication error
	 *             occurs or an exit signal was received or the remote node
	 *             sends a message containing an invalid cookie
	 * @see #publish(Scalaris, String, String)
	 */
	public void singlePublish(String topic, String content)
			throws ConnectionException {
		publish(connection, topic, content);
	}

	/**
	 * Publishes an event under a given {@code topic}. Uses the static
	 * {@link #staticConnection}.
	 * 
	 * @param topic
	 *            the topic to publish the content under
	 * @param content
	 *            the content to publish
	 * @throws ConnectionException
	 *             if the connection is not active or a communication error
	 *             occurs or an exit signal was received or the remote node
	 *             sends a message containing an invalid cookie
	 * @see #publish(Scalaris, OtpErlangString, OtpErlangString)
	 */
	public static void publish(OtpErlangString topic, OtpErlangString content)
			throws ConnectionException {
		publish(staticConnection, topic, content);
	}

	/**
	 * Publishes an event under a given {@code topic}. Uses the static
	 * {@link #staticConnection}.
	 * 
	 * @param topic
	 *            the topic to publish the content under
	 * @param content
	 *            the content to publish
	 * @throws ConnectionException
	 *             if the connection is not active or a communication error
	 *             occurs or an exit signal was received or the remote node
	 *             sends a message containing an invalid cookie
	 * @see #publish(Scalaris, String, String)
	 */
	public static void publish(String topic, String content)
			throws ConnectionException {
		publish(staticConnection, topic, content);
	}
	
	// /////////////////////////////
	// subscribe methods
	// /////////////////////////////
	
	/**
	 * Subscribes a url to a {@code topic}. Uses the given {@code connection}.
	 * 
	 * Note: Since version 1.4 erlang's return type is evaluated and additional
	 * exceptions were added to comply with that change.
	 * 
	 * @param connection
	 *            the connection to perform the operation on
	 * @param topic
	 *            the topic to subscribe the url to
	 * @param url
	 *            the url of the subscriber (this is where the events are send
	 *            to)
	 * @throws ConnectionException
	 *             if the connection is not active or a communication error
	 *             occurs or an exit signal was received or the remote node
	 *             sends a message containing an invalid cookie
	 * @throws TimeoutException
	 *             if a timeout occurred while trying to write the value
	 * @throws UnknownException
	 *             if any other error occurs
	 */
	private static void subscribe(Scalaris connection, OtpErlangString topic,
			OtpErlangString url) throws ConnectionException, TimeoutException,
			UnknownException {
		if (connection == null) {
			throw new ConnectionException("No connection.");
		}
		try {
			connection.subscribe(topic, url);
		} catch (de.zib.scalaris.ConnectionException e) {
			// e.printStackTrace();
			throw new ConnectionException(e);
		} catch (de.zib.scalaris.TimeoutException e) {
			// e.printStackTrace();
			throw new TimeoutException(e);
		} catch (de.zib.scalaris.UnknownException e) {
			// e.printStackTrace();
			throw new UnknownException(e);
		}
	}
	
	/**
	 * Subscribes a url to a {@code topic}. Uses the given {@code connection}.
	 * 
	 * Note: Since version 1.4 erlang's return type is evaluated and additional
	 * exceptions were added to comply with that change.
	 * 
	 * @param connection
	 *            the connection to perform the operation on
	 * @param topic
	 *            the topic to subscribe the url to
	 * @param url
	 *            the url of the subscriber (this is where the events are send
	 *            to)
	 * @throws ConnectionException
	 *             if the connection is not active or a communication error
	 *             occurs or an exit signal was received or the remote node
	 *             sends a message containing an invalid cookie
	 * @throws TimeoutException
	 *             if a timeout occurred while trying to write the value
	 * @throws UnknownException
	 *             if any other error occurs
	 * 
	 * @since 2.0
	 */
	private static void subscribe(Scalaris connection, String topic, String url)
			throws ConnectionException, TimeoutException, UnknownException {
		if (connection == null) {
			throw new ConnectionException("No connection.");
		}
		try {
			connection.subscribe(topic, url);
		} catch (de.zib.scalaris.ConnectionException e) {
			// e.printStackTrace();
			throw new ConnectionException(e);
		} catch (de.zib.scalaris.TimeoutException e) {
			// e.printStackTrace();
			throw new TimeoutException(e);
		} catch (de.zib.scalaris.UnknownException e) {
			// e.printStackTrace();
			throw new UnknownException(e);
		}
	}

	/**
	 * Subscribes a url to a {@code topic}. Uses the object's
	 * {@link #connection}.
	 * 
	 * Note: Since version 1.4 erlang's return type is evaluated and additional
	 * exceptions were added to comply with that change.
	 * 
	 * @param topic
	 *            the topic to subscribe the url to
	 * @param url
	 *            the url of the subscriber (this is where the events are send
	 *            to)
	 * @throws ConnectionException
	 *             if the connection is not active or a communication error
	 *             occurs or an exit signal was received or the remote node
	 *             sends a message containing an invalid cookie
	 * @throws TimeoutException
	 *             if a timeout occurred while trying to write the value
	 * @throws UnknownException
	 *             if any other error occurs
	 * @see #subscribe(Scalaris, OtpErlangString, OtpErlangString)
	 */
	public void singleSubscribe(OtpErlangString topic, OtpErlangString url)
			throws ConnectionException, TimeoutException, UnknownException {
		subscribe(connection, topic, url);
	}

	/**
	 * Subscribes a url to a {@code topic}. Uses the object's
	 * {@link #connection}.
	 * 
	 * Note: Since version 1.4 erlang's return type is evaluated and additional
	 * exceptions were added to comply with that change.
	 * 
	 * @param topic
	 *            the topic to subscribe the url to
	 * @param url
	 *            the url of the subscriber (this is where the events are send
	 *            to)
	 * @throws ConnectionException
	 *             if the connection is not active or a communication error
	 *             occurs or an exit signal was received or the remote node
	 *             sends a message containing an invalid cookie
	 * @throws TimeoutException
	 *             if a timeout occurred while trying to write the value
	 * @throws UnknownException
	 *             if any other error occurs
	 * @see #subscribe(Scalaris, String, String)
	 */
	public void singleSubscribe(String topic, String url)
			throws ConnectionException, TimeoutException, UnknownException {
		subscribe(connection, topic, url);
	}

	/**
	 * Subscribes a url to a {@code topic}. Uses the static
	 * {@link #staticConnection}.
	 * 
	 * Note: Since version 1.4 erlang's return type is evaluated and additional
	 * exceptions were added to comply with that change.
	 * 
	 * @param topic
	 *            the topic to subscribe the url to
	 * @param url
	 *            the url of the subscriber (this is where the events are send
	 *            to)
	 * @throws ConnectionException
	 *             if the connection is not active or a communication error
	 *             occurs or an exit signal was received or the remote node
	 *             sends a message containing an invalid cookie
	 * @throws TimeoutException
	 *             if a timeout occurred while trying to write the value
	 * @throws UnknownException
	 *             if any other error occurs
	 * @see #subscribe(Scalaris, OtpErlangString, OtpErlangString)
	 */
	public static void subscribe(OtpErlangString topic, OtpErlangString url)
			throws ConnectionException, TimeoutException, UnknownException {
		subscribe(staticConnection, topic, url);
	}

	/**
	 * Subscribes a url to a {@code topic}. Uses the static
	 * {@link #staticConnection}.
	 * 
	 * Note: Since version 1.4 erlang's return type is evaluated and additional
	 * exceptions were added to comply with that change.
	 * 
	 * @param topic
	 *            the topic to subscribe the url to
	 * @param url
	 *            the url of the subscriber (this is where the events are send
	 *            to)
	 * @throws ConnectionException
	 *             if the connection is not active or a communication error
	 *             occurs or an exit signal was received or the remote node
	 *             sends a message containing an invalid cookie
	 * @throws TimeoutException
	 *             if a timeout occurred while trying to write the value
	 * @throws UnknownException
	 *             if any other error occurs
	 * @see #subscribe(Scalaris, String, String)
	 */
	public static void subscribe(String topic, String url)
			throws ConnectionException, TimeoutException, UnknownException {
		subscribe(staticConnection, topic, url);
	}
	
	// /////////////////////////////
	// unsubscribe methods
	// /////////////////////////////

	/**
	 * Unsubscribes a url from a {@code topic}. Uses the given {@code
	 * connection}.
	 * 
	 * Note: Since version 1.4 erlang's return type is evaluated and additional
	 * exceptions were added to comply with that change.
	 * 
	 * @param connection
	 *            the connection to perform the operation on
	 * @param topic
	 *            the topic to unsubscribe the url from
	 * @param url
	 *            the url of the subscriber
	 * @throws ConnectionException
	 *             if the connection is not active or a communication error
	 *             occurs or an exit signal was received or the remote node
	 *             sends a message containing an invalid cookie
	 * @throws TimeoutException
	 *             if a timeout occurred while trying to write the value
	 * @throws NotFoundException
	 *             if the topic does not exist or the given subscriber is not
	 *             subscribed to the given topic
	 * @throws UnknownException
	 *             if any other error occurs
	 * 
	 * @since 1.3
	 */
	private static void unsubscribe(Scalaris connection, OtpErlangString topic,
			OtpErlangString url) throws ConnectionException, TimeoutException,
			NotFoundException, UnknownException {
		if (connection == null) {
			throw new ConnectionException("No connection.");
		}
		try {
			connection.unsubscribe(topic, url);
		} catch (de.zib.scalaris.ConnectionException e) {
			// e.printStackTrace();
			throw new ConnectionException(e);
		} catch (de.zib.scalaris.TimeoutException e) {
			// e.printStackTrace();
			throw new TimeoutException(e);
		} catch (de.zib.scalaris.UnknownException e) {
			// e.printStackTrace();
			throw new UnknownException(e);
		} catch (de.zib.scalaris.NotFoundException e) {
			// e.printStackTrace();
			throw new NotFoundException(e);
		}
	}

	/**
	 * Unsubscribes a url from a {@code topic}. Uses the given {@code
	 * connection}.
	 * 
	 * Note: Since version 1.4 erlang's return type is evaluated and additional
	 * exceptions were added to comply with that change.
	 * 
	 * @param connection
	 *            the connection to perform the operation on
	 * @param topic
	 *            the topic to unsubscribe the url from
	 * @param url
	 *            the url of the subscriber
	 * @throws ConnectionException
	 *             if the connection is not active or a communication error
	 *             occurs or an exit signal was received or the remote node
	 *             sends a message containing an invalid cookie
	 * @throws TimeoutException
	 *             if a timeout occurred while trying to write the value
	 * @throws NotFoundException
	 *             if the topic does not exist or the given subscriber is not
	 *             subscribed to the given topic
	 * @throws UnknownException
	 *             if any other error occurs
	 * 
	 * @since 2.0
	 */
	private static void unsubscribe(Scalaris connection, String topic,
			String url) throws ConnectionException, TimeoutException,
			NotFoundException, UnknownException {
		if (connection == null) {
			throw new ConnectionException("No connection.");
		}
		try {
			connection.unsubscribe(topic, url);
		} catch (de.zib.scalaris.ConnectionException e) {
			// e.printStackTrace();
			throw new ConnectionException(e);
		} catch (de.zib.scalaris.TimeoutException e) {
			// e.printStackTrace();
			throw new TimeoutException(e);
		} catch (de.zib.scalaris.UnknownException e) {
			// e.printStackTrace();
			throw new UnknownException(e);
		} catch (de.zib.scalaris.NotFoundException e) {
			// e.printStackTrace();
			throw new NotFoundException(e);
		}
	}

	/**
	 * Unsubscribes a url from a {@code topic}. Uses the object's
	 * {@link #connection}.
	 * 
	 * Note: Since version 1.4 erlang's return type is evaluated and additional
	 * exceptions were added to comply with that change.
	 * 
	 * @param topic
	 *            the topic to unsubscribe the url from
	 * @param url
	 *            the url of the subscriber
	 * @throws ConnectionException
	 *             if the connection is not active or a communication error
	 *             occurs or an exit signal was received or the remote node
	 *             sends a message containing an invalid cookie
	 * @throws TimeoutException
	 *             if a timeout occurred while trying to write the value
	 * @throws NotFoundException
	 *             if the topic does not exist or the given subscriber is not
	 *             subscribed to the given topic
	 * @throws UnknownException
	 *             if any other error occurs
	 * @see #unsubscribe(Scalaris, OtpErlangString, OtpErlangString)
	 * 
	 * @since 1.3
	 */
	public void singleUnsubscribe(OtpErlangString topic, OtpErlangString url)
			throws ConnectionException, TimeoutException, NotFoundException,
			UnknownException {
		unsubscribe(connection, topic, url);
	}

	/**
	 * Unsubscribes a url from a {@code topic}. Uses the object's
	 * {@link #connection}.
	 * 
	 * Note: Since version 1.4 erlang's return type is evaluated and additional
	 * exceptions were added to comply with that change.
	 * 
	 * @param topic
	 *            the topic to unsubscribe the url from
	 * @param url
	 *            the url of the subscriber
	 * @throws ConnectionException
	 *             if the connection is not active or a communication error
	 *             occurs or an exit signal was received or the remote node
	 *             sends a message containing an invalid cookie
	 * @throws TimeoutException
	 *             if a timeout occurred while trying to write the value
	 * @throws NotFoundException
	 *             if the topic does not exist or the given subscriber is not
	 *             subscribed to the given topic
	 * @throws UnknownException
	 *             if any other error occurs
	 * @see #unsubscribe(Scalaris, String, String)
	 * 
	 * @since 1.3
	 */
	public void singleUnsubscribe(String topic, String url)
			throws ConnectionException, TimeoutException, NotFoundException,
			UnknownException {
		unsubscribe(connection, topic, url);
	}

	/**
	 * Unsubscribes a url from a {@code topic}. Uses the static
	 * {@link #staticConnection}.
	 * 
	 * Note: Since version 1.4 erlang's return type is evaluated and additional
	 * exceptions were added to comply with that change.
	 * 
	 * @param topic
	 *            the topic to unsubscribe the url from
	 * @param url
	 *            the url of the subscriber
	 * @throws ConnectionException
	 *             if the connection is not active or a communication error
	 *             occurs or an exit signal was received or the remote node
	 *             sends a message containing an invalid cookie
	 * @throws TimeoutException
	 *             if a timeout occurred while trying to write the value
	 * @throws NotFoundException
	 *             if the topic does not exist or the given subscriber is not
	 *             subscribed to the given topic
	 * @throws UnknownException
	 *             if any other error occurs
	 * @see #unsubscribe(Scalaris, OtpErlangString, OtpErlangString)
	 * 
	 * @since 1.3
	 */
	public static void unsubscribe(OtpErlangString topic, OtpErlangString url)
			throws ConnectionException, TimeoutException, NotFoundException,
			UnknownException {
		unsubscribe(staticConnection, topic, url);
	}

	/**
	 * Unsubscribes a url from a {@code topic}. Uses the static
	 * {@link #staticConnection}.
	 * 
	 * Note: Since version 1.4 erlang's return type is evaluated and additional
	 * exceptions were added to comply with that change.
	 * 
	 * @param topic
	 *            the topic to unsubscribe the url from
	 * @param url
	 *            the url of the subscriber
	 * @throws ConnectionException
	 *             if the connection is not active or a communication error
	 *             occurs or an exit signal was received or the remote node
	 *             sends a message containing an invalid cookie
	 * @throws TimeoutException
	 *             if a timeout occurred while trying to write the value
	 * @throws NotFoundException
	 *             if the topic does not exist or the given subscriber is not
	 *             subscribed to the given topic
	 * @throws UnknownException
	 *             if any other error occurs
	 * @see #unsubscribe(Scalaris, String, String)
	 * 
	 * @since 1.3
	 */
	public static void unsubscribe(String topic, String url)
			throws ConnectionException, TimeoutException, NotFoundException,
			UnknownException {
		unsubscribe(staticConnection, topic, url);
	}

	// /////////////////////////////
	// get subscribers methods
	// /////////////////////////////
	
	/**
	 * Converts the given erlang {@code list} of erlang strings to a Vector
	 * with java strings.
	 * 
	 * @param list
	 *            the list to convert
	 * @return the converted list
	 */
	private static Vector<String> erlStrListToStrVector(OtpErlangList list) {
		Vector<String> result = new Vector<String>(list.arity());
		for (int i = 0; i < list.arity(); ++i) {
			OtpErlangString elem = (OtpErlangString) list.elementAt(i);
			result.add(elem.stringValue());
		}
		return result;
	}
	
	/**
	 * Gets a list of subscribers to a {@code topic}. Uses the given
	 * {@code connection}.
	 * 
	 * @param connection
	 *            the connection to perform the operation on
	 * @param topic
	 *            the topic to get the subscribers for
	 * @return the subscriber URLs
	 * @throws ConnectionException
	 *             if the connection is not active or a communication error
	 *             occurs or an exit signal was received or the remote node
	 *             sends a message containing an invalid cookie
	 * @throws UnknownException
	 *             is thrown if the return type of the erlang method does not
	 *             match the expected one
	 */
	private static OtpErlangList getSubscribers(Scalaris connection,
			OtpErlangString topic) throws ConnectionException, UnknownException {
		if (connection == null) {
			throw new ConnectionException("No connection.");
		}
		try {
			return connection.getSubscribers(topic);
		} catch (de.zib.scalaris.ConnectionException e) {
			// e.printStackTrace();
			throw new ConnectionException(e);
		} catch (de.zib.scalaris.UnknownException e) {
			// e.printStackTrace();
			throw new UnknownException(e);
		}
	}
	
	/**
	 * Gets a list of subscribers to a {@code topic}. Uses the given
	 * {@code connection}.
	 * 
	 * @param connection
	 *            the connection to perform the operation on
	 * @param topic
	 *            the topic to get the subscribers for
	 * @return the subscriber URLs
	 * @throws ConnectionException
	 *             if the connection is not active or a communication error
	 *             occurs or an exit signal was received or the remote node
	 *             sends a message containing an invalid cookie
	 * @throws UnknownException
	 *             is thrown if the return type of the erlang method does not
	 *             match the expected one
	 * 
	 * @since 2.0
	 */
	private static Vector<String> getSubscribers(Scalaris connection,
			String topic) throws ConnectionException, UnknownException {
		if (connection == null) {
			throw new ConnectionException("No connection.");
		}
		try {
			return erlStrListToStrVector(connection
					.getSubscribers(new OtpErlangString(topic)));
		} catch (de.zib.scalaris.ConnectionException e) {
			// e.printStackTrace();
			throw new ConnectionException(e);
		} catch (de.zib.scalaris.UnknownException e) {
			// e.printStackTrace();
			throw new UnknownException(e);
		}
	}
	
	/**
	 * Gets a list of subscribers to a {@code topic}. Uses the object's
	 * {@link #connection}.
	 * 
	 * @param topic
	 *            the topic to get the subscribers for
	 * @return the subscriber URLs
	 * @throws ConnectionException
	 *             if the connection is not active or a communication error
	 *             occurs or an exit signal was received or the remote node
	 *             sends a message containing an invalid cookie
	 * @throws UnknownException
	 *             is thrown if the return type of the erlang method does not
	 *             match the expected one
	 * @see #getSubscribers(Scalaris, OtpErlangString)
	 */
	public OtpErlangList singleGetSubscribers(OtpErlangString topic)
			throws ConnectionException, UnknownException {
		return getSubscribers(connection, topic);
	}

	/**
	 * Gets a list of subscribers to a {@code topic}. Uses the object's
	 * {@link #connection}.
	 * 
	 * @param topic
	 *            the topic to get the subscribers for
	 * @return the subscriber URLs
	 * @throws ConnectionException
	 *             if the connection is not active or a communication error
	 *             occurs or an exit signal was received or the remote node
	 *             sends a message containing an invalid cookie
	 * @throws UnknownException
	 *             is thrown if the return type of the erlang method does not
	 *             match the expected one
	 * @see #getSubscribers(Scalaris, String)
	 */
	public Vector<String> singleGetSubscribers(String topic)
			throws ConnectionException, UnknownException {
		return getSubscribers(connection, topic);
	}

	/**
	 * Gets a list of subscribers to a {@code topic}. Uses the static
	 * {@link #staticConnection}.
	 * 
	 * @param topic
	 *            the topic to get the subscribers for
	 * @return the subscriber URLs
	 * @throws ConnectionException
	 *             if the connection is not active or a communication error
	 *             occurs or an exit signal was received or the remote node
	 *             sends a message containing an invalid cookie
	 * @throws UnknownException
	 *             is thrown if the return type of the erlang method does not
	 *             match the expected one
	 * @see #getSubscribers(Scalaris, OtpErlangString)
	 */
	public static OtpErlangList getSubscribers(OtpErlangString topic)
			throws ConnectionException, UnknownException {
		return getSubscribers(staticConnection, topic);
	}

	/**
	 * Gets a list of subscribers to a {@code topic}. Uses the static
	 * {@link #staticConnection}.
	 * 
	 * @param topic
	 *            the topic to get the subscribers for
	 * @return the subscriber URLs
	 * @throws ConnectionException
	 *             if the connection is not active or a communication error
	 *             occurs or an exit signal was received or the remote node
	 *             sends a message containing an invalid cookie
	 * @throws UnknownException
	 *             is thrown if the return type of the erlang method does not
	 *             match the expected one
	 * @see #getSubscribers(Scalaris, String)
	 */
	public static Vector<String> getSubscribers(String topic)
			throws ConnectionException, UnknownException {
		return getSubscribers(staticConnection, topic);
	}
	
	/**
	 * Closes the transaction's connection to a Scalaris node.
	 * 
	 * Note: Subsequent calls to the other methods will throw
	 * {@link ConnectionException}s!
	 * 
	 * @since 2.0
	 */
	public void closeConnection() {
		connection.closeConnection();
	}
	
	// deprecated methods:
	
	/**
	 * Gets the value stored under the given {@code key}. Uses the object's
	 * {@link #connection}.
	 * 
	 * @param key
	 *            the key to look up
	 * @return the value stored under the given {@code key}
	 * @throws ConnectionException
	 *             if the connection is not active or a communication error
	 *             occurs or an exit signal was received or the remote node
	 *             sends a message containing an invalid cookie
	 * @throws TimeoutException
	 *             if a timeout occurred while trying to fetch the value
	 * @throws NotFoundException
	 *             if the requested key does not exist
	 * @throws UnknownException
	 *             if any other error occurs
	 * @see #read(Scalaris, OtpErlangString)
	 * @deprecated use {@link #singleReadString(OtpErlangString)} instead
	 */
	@Deprecated
	public OtpErlangString singleRead(OtpErlangString key)
			throws ConnectionException, TimeoutException, UnknownException,
			NotFoundException {
		return singleReadString(key);
	}
	
	/**
	 * Gets the value stored under the given {@code key}. Uses the object's
	 * {@link #connection}.
	 * 
	 * @param key
	 *            the key to look up
	 * @return the value stored under the given {@code key}
	 * @throws ConnectionException
	 *             if the connection is not active or a communication error
	 *             occurs or an exit signal was received or the remote node
	 *             sends a message containing an invalid cookie
	 * @throws TimeoutException
	 *             if a timeout occurred while trying to fetch the value
	 * @throws NotFoundException
	 *             if the requested key does not exist
	 * @throws UnknownException
	 *             if any other error occurs
	 * @see #singleReadString(OtpErlangString)
	 * @deprecated use {@link #singleReadString(String)} instead
	 */
	@Deprecated
	public String singleRead(String key) throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException {
		return singleReadString(key);
	}
	
	/**
	 * Gets the value stored under the given {@code key}. Uses the static
	 * {@link #staticConnection}.
	 * 
	 * @param key
	 *            the key to look up
	 * @return the value stored under the given {@code key}
	 * @throws ConnectionException
	 *             if the connection is not active or a communication error
	 *             occurs or an exit signal was received or the remote node
	 *             sends a message containing an invalid cookie
	 * @throws TimeoutException
	 *             if a timeout occurred while trying to fetch the value
	 * @throws NotFoundException
	 *             if the requested key does not exist
	 * @throws UnknownException
	 *             if any other error occurs
	 * @see #read(Scalaris, OtpErlangString)
	 * @deprecated use {@link #readString(OtpErlangString)} instead
	 */
	@Deprecated
	public static OtpErlangString read(OtpErlangString key)
			throws ConnectionException, TimeoutException, UnknownException,
			NotFoundException {
		return readString(key);
	}
	
	/**
	 * Gets the value stored under the given {@code key}. Uses the static
	 * {@link #staticConnection}.
	 * 
	 * @param key
	 *            the key to look up
	 * @return the value stored under the given {@code key}
	 * @throws ConnectionException
	 *             if the connection is not active or a communication error
	 *             occurs or an exit signal was received or the remote node
	 *             sends a message containing an invalid cookie
	 * @throws TimeoutException
	 *             if a timeout occurred while trying to fetch the value
	 * @throws NotFoundException
	 *             if the requested key does not exist
	 * @throws UnknownException
	 *             if any other error occurs
	 * @see #readString(OtpErlangString)
	 * @deprecated use {@link #readString(String)} instead
	 */
	@Deprecated
	public static String read(String key) throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException {
		return readString(key);
	}
}
