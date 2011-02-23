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

import java.io.IOException;

import com.ericsson.otp.erlang.OtpAuthException;
import com.ericsson.otp.erlang.OtpErlangAtom;
import com.ericsson.otp.erlang.OtpErlangExit;
import com.ericsson.otp.erlang.OtpErlangInt;
import com.ericsson.otp.erlang.OtpErlangList;
import com.ericsson.otp.erlang.OtpErlangLong;
import com.ericsson.otp.erlang.OtpErlangObject;
import com.ericsson.otp.erlang.OtpErlangString;
import com.ericsson.otp.erlang.OtpErlangTuple;

/**
 * Provides methods to read and write key/value pairs to a scalaris ring.
 * 
 * <p>
 * Each operation is a single transaction. If you are looking for more
 * transactions, use the {@link Transaction} class instead.
 * </p>
 * 
 * <p>
 * Instances of this class can be generated using a given connection to a
 * scalaris node using {@link #Scalaris(Connection)} or without a
 * connection ({@link #Scalaris()}) in which case a new connection is
 * created using {@link ConnectionFactory#createConnection()}.
 * </p>
 * 
 * <p>
 * There are two paradigms for reading and writing values:
 * <ul>
 *  <li> using Java {@link String}s: {@link #read(String)}, {@link #write(String, String)}
 *       <p>This is the safe way of accessing scalaris where type conversions
 *       are handled by the API and the user doesn't have to worry about anything else.</p>
 *       <p>Be aware though that this is not the most efficient way of handling strings!</p>
 *  <li> using custom {@link OtpErlangObject}s: {@link #readObject(OtpErlangString)},
 *       {@link #writeObject(OtpErlangString, OtpErlangObject)}
 *       <p>Here the user can specify custom behaviour and increase performance.
 *       Handling the stored types correctly is at the user's hand.</p>
 *       <p>An example using erlang objects to improve performance for inserting strings is
 *       provided by {@link de.zib.scalaris.examples.CustomOtpFastStringObject} and can be
 *       tested by {@link de.zib.scalaris.examples.FastStringBenchmark}.</p>
 * </ul> 
 * </p>
 * 
 * <h3>Reading values</h3>
 * <pre>
 * <code style="white-space:pre;">
 *   String key;
 *   OtpErlangString otpKey;
 *   
 *   Scalaris sc = new Scalaris();
 *   String value             = sc.read(key);          // {@link #read(String)}
 *   OtpErlangObject optValue = sc.readObject(otpKey); // {@link #readObject(OtpErlangString)}
 * </code>
 * </pre>
 * 
 * <p>For the full example, see {@link de.zib.scalaris.examples.ScalarisReadExample}</p>
 * 
 * <h3>Writing values</h3>
 * <pre>
 * <code style="white-space:pre;">
 *   String key;
 *   String value;
 *   OtpErlangString otpKey;
 *   OtpErlangString otpValue;
 *   
 *   Scalaris sc = new Scalaris();
 *   sc.write(key, value);             // {@link #write(String, String)}
 *   sc.writeObject(otpKey, otpValue); // {@link #writeObject(OtpErlangString, OtpErlangObject)}
 * </code>
 * </pre>
 * 
 * <p>For the full example, see {@link de.zib.scalaris.examples.ScalarisWriteExample}</p>
 * 
 * <h3>Deleting values</h3>
 * <pre>
 * <code style="white-space:pre;">
 *   String key;
 *   int timeout;
 *   DeleteResult result;
 *   
 *   Scalaris sc = new Scalaris();
 *   sc.delete(key);                    // {@link #delete(String)}
 *   sc.delete(key, timeout);           // {@link #delete(String, int)}
 *   result = sc.getLastDeleteResult(); // {@link #getLastDeleteResult()}
 * </code>
 * </pre>
 * 
 * <h3>Connection errors</h3>
 * 
 * Errors when setting up connections or trying to send/receive RPCs will be
 * handed to the {@link ConnectionPolicy} that has been set when the connection
 * was created. By default, {@link ConnectionFactory} uses
 * {@link DefaultConnectionPolicy} which implements automatic connection-retries
 * by classifying nodes as good or bad depending on their previous state. The
 * number of automatic retries is adjustable (default: 3).
 * 
 * @author Nico Kruber, kruber@zib.de
 * @version 2.5
 * @since 2.0
 */
public class Scalaris {
	/**
	 * Connection to a Scalaris node.
	 */
	private Connection connection;
	
	/**
	 * Stores the result list returned by erlang during a delete operation.
	 * 
	 * @see #delete(String)
	 */
	private OtpErlangList lastDeleteResult = null;
	
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
	 * 
	 * @throws ConnectionException
	 *             if the connection fails
	 */
	public Scalaris(Connection conn) throws ConnectionException {
		connection = conn;
	}
	
	// /////////////////////////////
	// read methods
	// /////////////////////////////
	
	/**
	 * Gets the value stored under the given <tt>key</tt>.
	 * 
	 * @param key
	 *            the key to look up
	 * 
	 * @return the value stored under the given <tt>key</tt>
	 * 
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
	public OtpErlangObject readObject(OtpErlangString key)
            throws ConnectionException, TimeoutException, NotFoundException,
            UnknownException {
		OtpErlangObject received_raw = null;
		try {
			received_raw = connection.doRPC("api_tx", "read",
					new OtpErlangList(key));
			OtpErlangTuple received = (OtpErlangTuple) received_raw;

			/*
			 * possible return values:
			 *  {ok, Value} | {fail, timeout | not_found}
			 */
			if (received.elementAt(0).equals(CommonErlangObjects.failAtom)) {
				OtpErlangObject reason = received.elementAt(1);
				if (reason.equals(CommonErlangObjects.timeoutAtom)) {
					throw new TimeoutException(received_raw);
				} else if (reason.equals(CommonErlangObjects.notFoundAtom)) {
					throw new NotFoundException(received_raw);
				} else {
					throw new UnknownException(received_raw);
				}
			} else if (received.elementAt(0).equals(CommonErlangObjects.okAtom) && received.arity() == 2) {
                OtpErlangObject value = received.elementAt(1);
                return value;
			} else {
                throw new UnknownException(received_raw);
			}
		} catch (OtpErlangExit e) {
			// e.printStackTrace();
			throw new ConnectionException(e);
		} catch (OtpAuthException e) {
			// e.printStackTrace();
			throw new ConnectionException(e);
		} catch (IOException e) {
			// e.printStackTrace();
			throw new ConnectionException(e);
		} catch (ClassCastException e) {
			// e.printStackTrace();
			// received_raw is not null since the first class cast is after the RPC!
			throw new UnknownException(e, received_raw);
		}
	}

	/**
	 * Gets the value stored under the given <tt>key</tt>.
	 * 
	 * @param key
	 *            the key to look up
	 * 
	 * @return the (string) value stored under the given <tt>key</tt>
	 * 
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
	 * @see #readObject(OtpErlangString)
	 */
	public String read(String key) throws ConnectionException,
			TimeoutException, NotFoundException, UnknownException {
		try {
			CustomOtpStringObject result = new CustomOtpStringObject();
			readCustom(key, result);
			return result.getValue();
//			return ((OtpErlangString) readObject(new OtpErlangString(key)))
//					.stringValue();
		} catch (ClassCastException e) {
			// e.printStackTrace();
			throw new UnknownException(e);
		}
	}
	
	/**
	 * Gets the value stored under the given <tt>key</tt>.
	 * 
	 * @param key
	 *            the key to look up
	 * @param value 
	 *            container that stores the value returned by scalaris
	 * 
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
	 * @see #readObject(OtpErlangString)
	 * @since 2.1
	 */
	public void readCustom(String key, CustomOtpObject<?> value)
            throws ConnectionException, TimeoutException, NotFoundException,
            UnknownException {
		try {
			value.setOtpValue(readObject(new OtpErlangString(key)));
		} catch (ClassCastException e) {
			// e.printStackTrace();
			throw new UnknownException(e);
		}
	}

	// /////////////////////////////
	// write methods
	// /////////////////////////////

	/**
	 * Stores the given <tt>key</tt>/<tt>value</tt> pair.
	 * 
	 * @param key
	 *            the key to store the value for
	 * @param value
	 *            the value to store
	 * 
	 * @throws ConnectionException
	 *             if the connection is not active or a communication error
	 *             occurs or an exit signal was received or the remote node
	 *             sends a message containing an invalid cookie
	 * @throws TimeoutException
	 *             if a timeout occurred while trying to write the value
     * @throws AbortException
     *             if the commit of the write failed
	 * @throws UnknownException
	 *             if any other error occurs
	 */
	public void writeObject(OtpErlangString key, OtpErlangObject value)
			throws ConnectionException, TimeoutException, AbortException, UnknownException {
		OtpErlangObject received_raw = null;
		try {
			received_raw = connection.doRPC("api_tx", "write",
					new OtpErlangList(new OtpErlangObject[] { key, value }));
			OtpErlangTuple received = (OtpErlangTuple) received_raw;

            /*
             * possible return values:
             *  {ok} | {fail, timeout | abort}
             */
            if (received.elementAt(0).equals(CommonErlangObjects.failAtom)) {
                OtpErlangObject reason = received.elementAt(1);
                if (reason.equals(CommonErlangObjects.timeoutAtom)) {
                    throw new TimeoutException(received_raw);
                } else if (reason.equals(CommonErlangObjects.abortAtom)) {
                    throw new AbortException(received_raw);
                } else {
                    throw new UnknownException(received_raw);
                }
            } else if (received.equals(CommonErlangObjects.okTupleAtom)) {
                return;
            } else {
                throw new UnknownException(received_raw);
            }
		} catch (OtpErlangExit e) {
			// e.printStackTrace();
			throw new ConnectionException(e);
		} catch (OtpAuthException e) {
			// e.printStackTrace();
			throw new ConnectionException(e);
		} catch (IOException e) {
			// e.printStackTrace();
			throw new ConnectionException(e);
		} catch (ClassCastException e) {
			// e.printStackTrace();
			// received_raw is not null since the first class cast is after the RPC!
			throw new UnknownException(e, received_raw);
		}
	}

	/**
	 * Stores the given <tt>key</tt>/<tt>value</tt> pair.
	 * 
	 * @param key
	 *            the key to store the value for
	 * @param value
	 *            the value to store
	 *
	 * @throws ConnectionException
	 *             if the connection is not active or a communication error
	 *             occurs or an exit signal was received or the remote node
	 *             sends a message containing an invalid cookie
	 * @throws TimeoutException
	 *             if a timeout occurred while trying to write the value
     * @throws AbortException
     *             if the commit of the write failed
	 * @throws UnknownException
	 *             if any other error occurs
	 *
	 * @see #writeObject(OtpErlangString, OtpErlangObject)
	 */
	public void write(String key, String value) throws ConnectionException,
			TimeoutException, AbortException, UnknownException {
		writeCustom(key, new CustomOtpStringObject(value));
//		writeObject(new OtpErlangString(key), new OtpErlangString(value));
	}
	
	/**
	 * Stores the given <tt>key</tt>/<tt>value</tt> pair.
	 * 
	 * @param key
	 *            the key to store the value for
	 * @param value
	 *            the value to store
	 * 
	 * @throws ConnectionException
	 *             if the connection is not active or a communication error
	 *             occurs or an exit signal was received or the remote node
	 *             sends a message containing an invalid cookie
	 * @throws TimeoutException
	 *             if a timeout occurred while trying to write the value
     * @throws AbortException
     *             if the commit of the write failed
	 * @throws UnknownException
	 *             if any other error occurs
	 * 
	 * @see #writeObject(OtpErlangString, OtpErlangObject)
	 * @since 2.1
	 */
	public void writeCustom(String key, CustomOtpObject<?> value)
			throws ConnectionException, TimeoutException, AbortException, UnknownException {
		writeObject(new OtpErlangString(key), value.getOtpValue());
	}
	
	// /////////////////////////////
	// delete methods
	// /////////////////////////////
	
	/**
	 * Tries to delete all replicas of the given <tt>key</tt> in 2000ms.
	 * 
	 * @param key
	 *            the key to delete
	 * 
	 * @return the number of successfully deleted replicas
	 * 
	 * @throws ConnectionException
	 *             if the connection is not active or a communication error
	 *             occurs or an exit signal was received or the remote node
	 *             sends a message containing an invalid cookie
	 * @throws TimeoutException
	 *             if a timeout occurred while trying to delete the value
	 * @throws NodeNotFoundException
	 *             if no scalaris node was found
	 * @throws UnknownException
	 *             if any other error occurs
	 * 
	 * @since 2.2
	 * 
	 * @see #delete(String, int)
	 */
	public long delete(String key) throws ConnectionException,
	TimeoutException, UnknownException, NodeNotFoundException {
		return delete(key, 2000);
	}

	/**
	 * Tries to delete all replicas of the given <tt>key</tt>.
	 * 
	 * WARNING: This function can lead to inconsistent data (e.g. deleted items
	 * can re-appear). Also when re-creating an item the version before the
	 * delete can re-appear.
	 * 
	 * @param key
	 *            the key to delete
	 * @param timeout
	 *            the time (in milliseconds) to wait for results
	 * 
	 * @return the number of successfully deleted replicas
	 * 
	 * @throws ConnectionException
	 *             if the connection is not active or a communication error
	 *             occurs or an exit signal was received or the remote node
	 *             sends a message containing an invalid cookie
	 * @throws TimeoutException
	 *             if a timeout occurred while trying to delete the value
	 * @throws NodeNotFoundException
	 *             if no scalaris node was found
	 * @throws UnknownException
	 *             if any other error occurs
	 * 
	 * @since 2.2
	 * 
	 * @see #delete(String)
	 */
	public long delete(String key, int timeout) throws ConnectionException,
	TimeoutException, UnknownException, NodeNotFoundException {
		OtpErlangObject received_raw = null;
		lastDeleteResult = null;
		try {
			received_raw = connection.doRPC("transaction_api", "delete",
					new OtpErlangList( new OtpErlangObject[] {
							new OtpErlangString(key),
							new OtpErlangInt(timeout) }));
			OtpErlangTuple received = (OtpErlangTuple) received_raw;

			/*
			 * possible return values:
			 *  - {ok, pos_integer(), list()}
			 *  - {fail, timeout}
			 *  - {fail, timeout, pos_integer(), list()}
			 *  - {fail, node_not_found}
			 */
			if (received.elementAt(0).equals(new OtpErlangAtom("fail"))) {
				OtpErlangObject reason = received.elementAt(1);
				if (reason.equals(new OtpErlangAtom("timeout"))) {
					if (received.arity() > 2) {
						lastDeleteResult = (OtpErlangList) received.elementAt(3);
					}
					throw new TimeoutException(received_raw);
				} else if (reason.equals(new OtpErlangAtom("node_not_found"))) {
					throw new NodeNotFoundException(received_raw);
				} else {
					throw new UnknownException(received_raw);
				}
			} else {
				lastDeleteResult = (OtpErlangList) received.elementAt(2);
				long succeeded = ((OtpErlangLong) received.elementAt(1)).longValue();
				return succeeded;
			}
		} catch (OtpErlangExit e) {
			// e.printStackTrace();
			throw new ConnectionException(e);
		} catch (OtpAuthException e) {
			// e.printStackTrace();
			throw new ConnectionException(e);
		} catch (IOException e) {
			// e.printStackTrace();
			throw new ConnectionException(e);
		} catch (ClassCastException e) {
			// e.printStackTrace();
			// received_raw is not null since the first class cast is after the RPC!
			throw new UnknownException(e, received_raw);
		}
	}

	/**
	 * Returns the result of the last call to {@link #delete(String)}.
	 * 
	 * NOTE: This function traverses the result list returned by erlang and
	 * therefore takes some time to process. It is advised to store the returned
	 * result object once generated.
	 * 
	 * @return the delete result
	 * 
	 * @throws UnknownException
	 *             is thrown if an unknown reason was encountered
	 * 
	 * @see #delete(String)
	 */
	public DeleteResult getLastDeleteResult() throws UnknownException {
		try {
			return new DeleteResult(lastDeleteResult);
		} catch (UnknownException e) {
			throw new UnknownException(e, lastDeleteResult);
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
