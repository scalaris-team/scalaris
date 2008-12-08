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

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;
import java.util.Vector;

import com.ericsson.otp.erlang.OtpAuthException;
import com.ericsson.otp.erlang.OtpConnection;
import com.ericsson.otp.erlang.OtpErlangAtom;
import com.ericsson.otp.erlang.OtpErlangExit;
import com.ericsson.otp.erlang.OtpErlangList;
import com.ericsson.otp.erlang.OtpErlangObject;
import com.ericsson.otp.erlang.OtpErlangString;
import com.ericsson.otp.erlang.OtpErlangTuple;

import de.zib.chordsharp.examples.TransactionParallelReadsExample;
import de.zib.chordsharp.examples.TransactionReadExample;
import de.zib.chordsharp.examples.TransactionReadWriteExample;
import de.zib.chordsharp.examples.TransactionWriteExample;
import de.zib.tools.PropertyLoader;

/**
 * Provides means to realise a transaction with the chordsharp ring using Java.
 * 
 * <p>
 * It reads the connection parameters from a file called
 * {@code ChordSharpConnection.properties} or uses default properties defined in
 * {@link ChordSharpConnection#defaultProperties}.
 * </p>
 * 
 * <h3>Example:</h3>
 * <code style="white-space:pre;">
 *   OtpErlangString otpKey;
 *   OtpErlangString otpValue;
 *   OtpErlangString otpResult;
 *   
 *   String key;
 *   String value;
 *   String result;
 *   
 *   Transaction transaction = new Transaction(); // {@link #Transaction()}
 *   transaction.start();                         // {@link #start()}
 *   
 *   transaction.write(otpKey, otpValue); // {@link #write(OtpErlangString, OtpErlangObject)}
 *   transaction.write(key, value);       // {@link #write(String, String)}
 *   
 *   otpResult = transaction.readString(otpKey); //{@link #readString(OtpErlangString)}
 *   result = transaction.readString(key);       //{@link #readString(String)}
 *   
 *   transaction.commit(); // {@link #commit()}
 * </code>
 * 
 * <p>
 * For more examples, have a look at {@link TransactionReadExample},
 * {@link TransactionParallelReadsExample}, {@link TransactionWriteExample} and
 * {@link TransactionReadWriteExample}.
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
 * The {@link TransactionReadWriteExample} example shows such a use case.
 * </p>
 * 
 * @author Nico Kruber, kruber@zib.de
 * @version 1.0
 */
public class Transaction {
	/**
	 * the erlang transaction log
	 */
	private OtpErlangList transLog = null;
	
	/**
	 * the erlang transaction log before the last operation
	 */
	private OtpErlangList transLog_old = null;

	/**
	 * the connection to a chorsharp node
	 */
	private OtpConnection connection;
	
	/**
	 * Creates the object's connection to the chordsharp node specified in the
	 * {@code "ChordSharpConnection.properties"} file.
	 * 
	 * @throws ConnectionException
	 *             if the connection fails
	 */
	public Transaction() throws ConnectionException {
		Properties properties = new Properties(ChordSharpConnection.defaultProperties);
		PropertyLoader.loadProperties(properties, "ChordSharpConnection.properties");
		connection = ChordSharpConnection.createConnection(properties);
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
	public void start() throws ConnectionException, TransactionNotFinishedException, UnknownException {
		if (transLog != null) {
			throw new TransactionNotFinishedException(
					"Cannot start a new transaction until the old one is not committed or aborted.");
		}
		try {
			connection.sendRPC("transstore.transaction", "translog_new",
					new OtpErlangList());
			// return value: []
			OtpErlangList received = (OtpErlangList) connection.receiveRPC();
			transLog = received;
		} catch (OtpErlangExit e) {
			// e.printStackTrace();
			throw new ConnectionException(e.getMessage());
		} catch (OtpAuthException e) {
			// e.printStackTrace();
			throw new ConnectionException(e.getMessage());
		} catch (IOException e) {
			// e.printStackTrace();
			throw new ConnectionException(e.getMessage());
		} catch (ClassCastException e) {
			// e.printStackTrace();
			throw new UnknownException(e.getMessage());
		}
	}

	/**
	 * Commits the current transaction. The transaction's log is reset if the
	 * commit was successful, otherwise it still retains in the transaction
	 * which must be successfully committed or aborted in order to be restarted.
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
	 * @see #abort()
	 */
	public void commit() throws UnknownException, ConnectionException {
		if (transLog == null) {
			throw new TransactionNotStartedException("The transaction needs to be started before it is used.");
		}
		try {
			connection.sendRPC("transstore.transaction_api", "commit",
					new OtpErlangList(transLog));
			/*
			 * possible return values:
			 *  - {ok}
			 *  - {fail, Reason}
			 */
			OtpErlangTuple received = (OtpErlangTuple) connection.receiveRPC();
			if(received.elementAt(0).equals(new OtpErlangAtom("ok"))) {
				// transaction was successful: reset transaction log
				transLog = null;
				transLog_old = null;
				return;
			} else {
				// transaction failed
				// TODO: is there a more specific type?
				OtpErlangObject reason = received.elementAt(1);
				throw new UnknownException("Erlang: " + reason.toString());
			}
		} catch (OtpErlangExit e) {
			// e.printStackTrace();
			throw new ConnectionException(e.getMessage());
		} catch (OtpAuthException e) {
			// e.printStackTrace();
			throw new ConnectionException(e.getMessage());
		} catch (IOException e) {
			// e.printStackTrace();
			throw new ConnectionException(e.getMessage());
		} catch (ClassCastException e) {
			// e.printStackTrace();
			throw new UnknownException(e.getMessage());
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
	 */
	public void abort() {
		transLog = null;
		transLog_old = null;
	}

	/**
	 * Gets the value stored under the given {@code key}.
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
	 */
	public OtpErlangObject readObject(OtpErlangString key)
			throws ConnectionException, TimeoutException, UnknownException,
			NotFoundException {
		if (transLog == null) {
			throw new TransactionNotStartedException("The transaction needs to be started before it is used.");
		}
		try {
			connection.sendRPC("transstore.transaction_api", "jRead",
					new OtpErlangList(new OtpErlangObject[] {key, transLog}));
			/*
			 * possible return values:
			 *  - {{fail, not_found}, TransLog}
			 *  - {{fail, timeout}, TransLog}
			 *  - {{fail, fail}, TransLog}
			 *  - {{value, Value}, NewTransLog}
			 */
			OtpErlangTuple received = (OtpErlangTuple) connection.receiveRPC();
			transLog_old = transLog;
			transLog = (OtpErlangList) received.elementAt(1);
			OtpErlangTuple status = (OtpErlangTuple) received.elementAt(0);
			if (status.elementAt(0).equals(new OtpErlangAtom("value"))) {
				return status.elementAt(1);
			} else {
				if (status.elementAt(1).equals(new OtpErlangAtom("timeout"))) {
					throw new TimeoutException();
				} else if (status.elementAt(1).equals(
						new OtpErlangAtom("not_found"))) {
					throw new NotFoundException();
				} else {
					throw new UnknownException();
				}
			}
		} catch (OtpErlangExit e) {
			// e.printStackTrace();
			throw new ConnectionException(e.getMessage());
		} catch (OtpAuthException e) {
			// e.printStackTrace();
			throw new ConnectionException(e.getMessage());
		} catch (IOException e) {
			// e.printStackTrace();
			throw new ConnectionException(e.getMessage());
		} catch (ClassCastException e) {
			// e.printStackTrace();
			throw new UnknownException(e.getMessage());
		}
	}
	
	/**
	 * Gets the value stored under the given {@code key}.
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
	 */
	public OtpErlangString readString(OtpErlangString key) throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException {
		try {
			return (OtpErlangString) readObject(key);
		} catch (ClassCastException e) {
			// e.printStackTrace();
			throw new UnknownException(e.getMessage());
		}
	}
	
	/**
	 * Gets the value stored under the given {@code key}.
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
	 */
	public String readString(String key) throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException {
		return readString(new OtpErlangString(key)).stringValue();
	}

	/**
	 * Stores the given {@code key}/{@code value} pair.
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
	 */
	public void write(OtpErlangString key, OtpErlangObject value)
			throws ConnectionException, TimeoutException, UnknownException {
		if (transLog == null) {
			throw new TransactionNotStartedException("The transaction needs to be started before it is used.");
		}
		try {
			connection.sendRPC("transstore.transaction_api", "jWrite",
					new OtpErlangList(new OtpErlangObject[] {key, value, transLog}));
			/*
			 * possible return values:
			 *  - {{fail, not_found}, TransLog}
			 *  - {{fail, timeout}, TransLog}
			 *  - {{fail, fail}, TransLog}
			 *  - {ok, NewTransLog}
			 */
			OtpErlangTuple received = (OtpErlangTuple) connection.receiveRPC();
			transLog_old = transLog;
			transLog = (OtpErlangList) received.elementAt(1);
			if (received.elementAt(0).equals(new OtpErlangAtom("ok"))) {
				return;
			} else {
				OtpErlangTuple status = (OtpErlangTuple) received.elementAt(0);
				if (status.elementAt(1).equals(new OtpErlangAtom("timeout"))) {
					throw new TimeoutException();
				} else {
					throw new UnknownException(status.elementAt(1).toString());
				}
			}
		} catch (OtpErlangExit e) {
			// e.printStackTrace();
			throw new ConnectionException(e.getMessage());
		} catch (OtpAuthException e) {
			// e.printStackTrace();
			throw new ConnectionException(e.getMessage());
		} catch (IOException e) {
			// e.printStackTrace();
			throw new ConnectionException(e.getMessage());
		} catch (ClassCastException e) {
			// e.printStackTrace();
			throw new UnknownException(e.getMessage());
		}
	}

	/**
	 * Stores the given {@code key}/{@code value} pair.
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
	 */
	
	public void write(String key, String value) throws ConnectionException,
			TimeoutException, UnknownException {
		write(new OtpErlangString(key), new OtpErlangString(value));
	}

	/**
	 * Reverts the last (read, parallelRead or write) operation by restoring the
	 * last state. If there was no operation or the last operation was already
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
	
	
	// /////////////////////////////
	// currently broken / unsupported methods
	// /////////////////////////////
	
	/**
	 * BROKEN: Gets the values stored under the given {@code keys}.
	 * 
	 * @param keys
	 *            the keys to look up ({@link OtpErlangString} elements in an {@link OtpErlangList})
	 * @return the values stored under the given {@code key} with the format [{value, Value}]
	 * @throws ConnectionException
	 *             if the connection is not active or a communication error
	 *             occurs or an exit signal was received or the remote node
	 *             sends a message containing an invalid cookie
	 * @throws UnknownException
	 *             if any other error occurs
	 */
	@Deprecated
	private OtpErlangList parallelReads1(OtpErlangList keys)
			throws ConnectionException, UnknownException {
		throw new UnknownException("parallelReads is currently broken");
//		if (transLog == null) {
//			throw new TransactionNotStartedException("The transaction needs to be started before it is used.");
//		}
//		try {
//			connection.sendRPC("transstore.transaction_api", "jParallel_reads",
//					new OtpErlangList(new OtpErlangObject[] {keys, transLog}));
//			/*
//			 * possible return values:
//			 *  - {fail, NewTransLog}
//			 *  - {[{value, Value}], NewTransLog}
//			 */
//			OtpErlangTuple received = (OtpErlangTuple) connection.receiveRPC();
//			transLog_old = transLog;
//			transLog = (OtpErlangList) received.elementAt(1);
//			if (received.elementAt(0).equals(new OtpErlangAtom("fail"))) {
//				throw new UnknownException();
//			} else {
//				OtpErlangList values = (OtpErlangList) received.elementAt(0);
//				return values;
//			}
//		} catch (OtpErlangExit e) {
//			// e.printStackTrace();
//			throw new ConnectionException(e.getMessage());
//		} catch (OtpAuthException e) {
//			// e.printStackTrace();
//			throw new ConnectionException(e.getMessage());
//		} catch (IOException e) {
//			// e.printStackTrace();
//			throw new ConnectionException(e.getMessage());
//		} catch (ClassCastException e) {
//			// e.printStackTrace();
//			throw new UnknownException(e.getMessage());
//		}
	}
	
	/**
	 * BROKEN: Gets the values stored under the given {@code keys}.
	 * 
	 * @param keys
	 *            the keys to look up ({@link OtpErlangString} elements in an {@link OtpErlangList})
	 * @return the value stored under the given {@code key}
	 * @throws ConnectionException
	 *             if the connection is not active or a communication error
	 *             occurs or an exit signal was received or the remote node
	 *             sends a message containing an invalid cookie
	 * @throws UnknownException
	 *             if any other error occurs
	 */
	@Deprecated
	public OtpErlangList parallelReads(OtpErlangList keys) throws ConnectionException,
			UnknownException {
		OtpErlangList result = parallelReads1(new OtpErlangList(keys));
		// result value: [{value, Value}]
		
		// convert result list:
		OtpErlangString[] erlangValues = new OtpErlangString[result.arity()];
		try {
			for (int i = 0; i < result.arity(); ++i) {
				OtpErlangString value = (OtpErlangString) ((OtpErlangTuple) result.elementAt(i)).elementAt(1);
				erlangValues[i] = value;
			}
		} catch (ClassCastException e) {
			// e.printStackTrace();
			throw new UnknownException(e.getMessage());
		}
		return new OtpErlangList(erlangValues);
	}
	
	/**
	 * BROKEN: Gets the values stored under the given {@code keys}.
	 * 
	 * @param keys
	 *            the keys to look up
	 * @return the value stored under the given {@code key}
	 * @throws ConnectionException
	 *             if the connection is not active or a communication error
	 *             occurs or an exit signal was received or the remote node
	 *             sends a message containing an invalid cookie
	 * @throws UnknownException
	 *             if any other error occurs
	 */
	@Deprecated
	public Vector<String> parallelReads(String[] keys)
			throws ConnectionException, UnknownException {
		return parallelReads(new Vector<String>(Arrays.asList(keys)));
	}
	
	/**
	 * BROKEN: Gets the values stored under the given {@code keys}.
	 * 
	 * @param keys
	 *            the keys to look up
	 * @return the value stored under the given {@code key}
	 * @throws ConnectionException
	 *             if the connection is not active or a communication error
	 *             occurs or an exit signal was received or the remote node
	 *             sends a message containing an invalid cookie
	 * @throws UnknownException
	 *             if any other error occurs
	 */
	@Deprecated
	public Vector<String> parallelReads(Vector<String> keys) throws ConnectionException,
			UnknownException {
		// convert keys to an erlang list of erlang strings:
		OtpErlangString[] erlangKeys = new OtpErlangString[keys.size()];
		int i = 0;
		for (Iterator<String> iterator = keys.iterator(); iterator.hasNext();) {
			OtpErlangString key = new OtpErlangString(iterator.next());
			erlangKeys[i++] = key;
		}
		
		OtpErlangList result = parallelReads1(new OtpErlangList(new OtpErlangList(erlangKeys)));
		// result value: [{value, Value}]
		
		// convert result list:
		Vector<String> values = new Vector<String>(result.arity());
		try {
			for (i = 0; i < result.arity(); ++i) {
				OtpErlangString value = (OtpErlangString) ((OtpErlangTuple) result
						.elementAt(i)).elementAt(1);
				values.add(value.stringValue());
			}
		} catch (ClassCastException e) {
			// e.printStackTrace();
			throw new UnknownException(e.getMessage());
		}
		return values;
	}
	
	// /////////////////////////////
	// deprecated methods
	// /////////////////////////////
	
	/**
	 * Gets the value stored under the given {@code key}.
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
	 */
	@Deprecated
	public OtpErlangString read(OtpErlangString key)
			throws ConnectionException, TimeoutException, UnknownException,
			NotFoundException {
		return readString(key);
	}

	/**
	 * Gets the value stored under the given {@code key}.
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
	 */
	@Deprecated
	public String read(String key) throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException {
		return read(new OtpErlangString(key)).stringValue();
	}
}
