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
import com.ericsson.otp.erlang.OtpErlangList;
import com.ericsson.otp.erlang.OtpErlangObject;
import com.ericsson.otp.erlang.OtpErlangString;
import com.ericsson.otp.erlang.OtpErlangTuple;

/**
 * Provides means to realise a transaction with the scalaris ring using Java.
 * 
 * <p>
 * Instances of this class can be generated using a given connection to a
 * scalaris node using {@link #Transaction(Connection)} or without a
 * connection ({@link #Transaction()}) in which case a new connection is
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
 * <h3>Example:</h3>
 * <pre>
 * <code style="white-space:pre;">
 *   OtpErlangString otpKey;
 *   OtpErlangString otpValue;
 *   OtpErlangObject otpResult;
 *   
 *   String key;
 *   String value;
 *   String result;
 *   
 *   Transaction t1 = new Transaction(); // {@link #Transaction()}
 *   t1.start();                         // {@link #start()}
 *   
 *   t1.write(key, value);             // {@link #write(String, String)}
 *   t1.writeObject(otpKey, otpValue); // {@link #writeObject(OtpErlangString, OtpErlangObject)}
 *   
 *   result = t1.read(key);             //{@link #read(String)}
 *   otpResult = t1.readObject(otpKey); //{@link #readObject(OtpErlangString)}
 *   
 *   transaction.commit(); // {@link #commit()}
 * </code>
 * </pre>
 * 
 * <p>
 * For more examples, have a look at {@link de.zib.scalaris.examples.TransactionReadExample},
 * {@link de.zib.scalaris.examples.TransactionWriteExample} and
 * {@link de.zib.scalaris.examples.TransactionReadWriteExample}.
 * </p>
 * 
 * <h3>Attention:</h3>
 * <p>
 * If a read or write operation fails within a transaction all subsequent
 * operations on that key will fail as well. This behaviour may particularly be
 * undesirable if a read operation just checks whether a value already exists or
 * not. To overcome this situation call {@link #revertLastOp()} immediately
 * after the failed operation which restores the state as it was before that
 * operation.<br />
 * The {@link de.zib.scalaris.examples.TransactionReadWriteExample} example
 * shows such a use case.
 * </p>
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
 * @version 2.2
 * @since 2.0
 */
public class Transaction {
	/**
	 * erlang transaction log
	 */
	private OtpErlangList transLog = null;
	
	/**
	 * erlang transaction log before the last operation
	 * 
	 * @see #revertLastOp()
	 */
	private OtpErlangList transLog_old = null;

	/**
	 * connection to a scalaris node
	 */
	private Connection connection;
	
	/**
	 * Constructor, uses the default connection returned by
	 * {@link ConnectionFactory#createConnection()}.
	 * 
	 * @throws ConnectionException
	 *             if the connection fails
	 */
	public Transaction() throws ConnectionException {
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
	public Transaction(Connection conn) throws ConnectionException {
		connection = conn;
	}

	/**
	 * Resets the transaction to its initial state.
	 * 
	 * This action is not reversible.
	 */
	public void reset() {
		transLog = null;
		transLog_old = null;
	}
	
	/**
	 * Starts a new transaction by generating a new transaction log.
	 * 
	 * @throws ConnectionException
	 *             if the connection is not active or a communication error
	 *             occurs or an exit signal was received or the remote node
	 *             sends a message containing an invalid cookie
	 * @throws TransactionNotFinishedException
	 *             if an old transaction is not finished (via {@link #commit()}
	 *             or {@link #abort()}) yet
	 * @throws UnknownException
	 *             if the returned value from erlang does not have the expected
	 *             type/structure
	 */
	public void start() throws ConnectionException,
			TransactionNotFinishedException, UnknownException {
		if (transLog != null || transLog_old != null) {
			throw new TransactionNotFinishedException();
		}
		OtpErlangObject received_raw = null;
		try {
			// return value: []
			received_raw = connection
					.doRPC("txlog", "new", new OtpErlangList());
			OtpErlangList received = (OtpErlangList) received_raw;
			transLog = received;
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
	 * Commits the current transaction.
	 * 
	 * <p>
	 * The transaction's log is reset if the commit was successful, otherwise it
	 * still retains in the transaction which must be successfully committed,
	 * aborted or reset in order to be restarted.
	 * </p>
	 * 
	 * @throws UnknownException
	 *             If the commit fails or the returned value from erlang is of
	 *             an unknown type/structure, this exception is thrown. Neither
	 *             the transaction log nor the local operations buffer is
	 *             emptied, so that the commit can be tried again.
	 * @throws ConnectionException
	 *             if the connection is not active or a communication error
	 *             occurs or an exit signal was received or the remote node
	 *             sends a message containing an invalid cookie
	 * 
	 * @see #abort()
	 * @see #reset()
	 */
	public void commit() throws UnknownException, ConnectionException {
		if (transLog == null) {
			throw new TransactionNotStartedException();
		}
		OtpErlangObject received_raw = null;
		try {
			/*
			 * possible return values:
			 *  - {ok}
			 *  - {fail, Reason}
			 */
			received_raw = connection.doRPC("transaction_api", "commit",
					new OtpErlangList(transLog));
			OtpErlangTuple received = (OtpErlangTuple) received_raw;
			if(received.elementAt(0).equals(new OtpErlangAtom("ok"))) {
				// transaction was successful: reset transaction log
				reset();
				return;
			} else {
				// transaction failed
//				OtpErlangObject reason = received.elementAt(1);
//				throw new UnknownException(reason.toString());
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
	 * Cancels the current transaction.
	 * 
	 * <p>
	 * For a transaction to be cancelled, only the {@link #transLog} needs to be
	 * reset. Nothing else needs to be done since the data was not modified
	 * until the transaction was committed.
	 * </p>
	 *
	 * @see #commit()
	 * @see #reset()
	 */
	public void abort() {
		reset();
	}

	/**
	 * Gets the value stored under the given <code>key</code>.
	 * 
	 * @param key
	 *            the key to look up
	 *
	 * @return the value stored under the given <code>key</code> as a raw erlang type
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
			throws ConnectionException, TimeoutException, UnknownException,
			NotFoundException {
		if (transLog == null) {
			throw new TransactionNotStartedException();
		}
		OtpErlangObject received_raw = null;
		try {
			/*
			 * possible return values:
			 *  - {{fail, not_found}, TransLog}
			 *  - {{fail, timeout}, TransLog}
			 *  - {{fail, fail}, TransLog}
			 *  - {{value, Value}, NewTransLog}
			 */
			received_raw = connection.doRPC("transaction_api", "jRead",
					new OtpErlangList(new OtpErlangObject[] {key, transLog}));
			OtpErlangTuple received = (OtpErlangTuple) received_raw;
			transLog_old = transLog;
			transLog = (OtpErlangList) received.elementAt(1);
			OtpErlangTuple status = (OtpErlangTuple) received.elementAt(0);
			if (status.elementAt(0).equals(new OtpErlangAtom("value"))) {
				return status.elementAt(1);
			} else {
				if (status.elementAt(1).equals(new OtpErlangAtom("timeout"))) {
					throw new TimeoutException(received_raw);
				} else if (status.elementAt(1).equals(
						new OtpErlangAtom("not_found"))) {
					throw new NotFoundException(received_raw);
				} else {
					throw new UnknownException(received_raw);
				}
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
	 * Gets the value stored under the given <code>key</code>.
	 * 
	 * @param key
	 *            the key to look up
	 *
	 * @return the (string) value stored under the given <code>key</code>
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
			TimeoutException, UnknownException, NotFoundException {
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
	 * Gets the value stored under the given <code>key</code>.
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
			throws ConnectionException, TimeoutException, UnknownException,
			NotFoundException {
		try {
			value.setOtpValue(readObject(new OtpErlangString(key)));
		} catch (ClassCastException e) {
			// e.printStackTrace();
			throw new UnknownException(e);
		}
	}

	/**
	 * Stores the given <code>key</code>/<code>value</code> pair.
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
	 * @throws UnknownException
	 *             if any other error occurs
	 */
	public void writeObject(OtpErlangString key, OtpErlangObject value)
			throws ConnectionException, TimeoutException, UnknownException {
		if (transLog == null) {
			throw new TransactionNotStartedException();
		}
		OtpErlangObject received_raw = null;
		try {
			/*
			 * possible return values:
			 *  - {{fail, not_found}, TransLog}
			 *  - {{fail, timeout}, TransLog}
			 *  - {{fail, fail}, TransLog}
			 *  - {ok, NewTransLog}
			 */
			received_raw = connection.doRPC("transaction_api", "jWrite",
					new OtpErlangList(new OtpErlangObject[] {key, value, transLog}));
			OtpErlangTuple received = (OtpErlangTuple) received_raw;
			transLog_old = transLog;
			transLog = (OtpErlangList) received.elementAt(1);
			if (received.elementAt(0).equals(new OtpErlangAtom("ok"))) {
				return;
			} else {
				OtpErlangTuple status = (OtpErlangTuple) received.elementAt(0);
				if (status.elementAt(1).equals(new OtpErlangAtom("timeout"))) {
					throw new TimeoutException(received_raw);
//				} else if (status.elementAt(1).equals(new OtpErlangAtom("fail"))) {
////					throw new UnknownException(status.elementAt(1).toString());
//					throw new UnknownException(received_raw);
				} else {
					throw new UnknownException(received_raw);
				}
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
	 * Stores the given <code>key</code>/<code>value</code> pair.
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
	 * @throws UnknownException
	 *             if any other error occurs
	 *
	 * @see #writeObject(OtpErlangString, OtpErlangObject)
	 */
	public void write(String key, String value) throws ConnectionException,
			TimeoutException, UnknownException {
		writeCustom(key, new CustomOtpStringObject(value));
//		writeObject(new OtpErlangString(key), new OtpErlangString(value));
	}
	
	/**
	 * Stores the given <code>key</code>/<code>value</code> pair.
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
	 * @throws UnknownException
	 *             if any other error occurs
	 * 
	 * @see #writeObject(OtpErlangString, OtpErlangObject)
	 * @since 2.1
	 */
	public void writeCustom(String key, CustomOtpObject<?> value)
			throws ConnectionException, TimeoutException, UnknownException {
		writeObject(new OtpErlangString(key), value.getOtpValue());
	}

	/**
	 * Reverts the last (read, parallelRead or write) operation by restoring the
	 * last state.
	 * 
	 * If there was no operation or the last operation was already
	 * reverted, this method does nothing.
	 * 
	 * <p>
	 * This method is especially useful if after an unsuccessful read a value
	 * with the same key should be written which is not possible if the failed
	 * read is still in the transaction's log.
	 * </p>
	 * <p>
	 * NOTE: This method works only ONCE! Subsequent calls will do nothing.
	 * </p>
	 */
	public void revertLastOp() {
		if (transLog_old != null) {
			transLog = transLog_old;
			transLog_old = null;
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
