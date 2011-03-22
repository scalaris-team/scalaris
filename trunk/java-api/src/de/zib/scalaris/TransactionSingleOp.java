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

import com.ericsson.otp.erlang.OtpErlangList;
import com.ericsson.otp.erlang.OtpErlangObject;
import com.ericsson.otp.erlang.OtpErlangString;
import com.ericsson.otp.erlang.OtpErlangTuple;

/**
 * Provides methods to read and write key/value pairs to/from a scalaris ring.
 * 
 * <p>
 * Each operation is a single transaction. If you are looking for more
 * transactions, use the {@link Transaction} class instead.
 * </p>
 * 
 * <p>
 * Instances of this class can be generated using a given connection to a
 * scalaris node ({@link #TransactionSingleOp(Connection)}) or without a
 * connection ({@link #TransactionSingleOp()}) in which case a new connection
 * is created using {@link ConnectionFactory#createConnection()}.
 * </p>
 * 
 * <p>
 * There are two paradigms for reading and writing values:
 * <ul>
 *  <li> using arbitrary erlang objects extending OtpErlangObject:
 *       {@link #read(OtpErlangString)},
 *       {@link #write(OtpErlangString, OtpErlangObject)}
 *  <li> using (supported) Java objects:
 *       {@link #read(String)}, {@link #write(String, Object)}
 *       <p>These types can be accessed from any Scalaris API and translate to
 *       each language's native types, e.g. String and OtpErlangString.
 *       A list of supported types can be found in the {@link ErlangValue}
 *       class which will perform the conversion.</p>
 *       <p>Additional (custom) types can be used by providing a class that
 *       extends the {@link ErlangValue} class.
 *       The user can specify custom behaviour but the correct
 *       handling of these values is at the user's hand.</p>
 *       <p>An example using erlang objects to improve performance for
 *       inserting strings is provided by
 *       {@link de.zib.scalaris.examples.ErlangValueFastString} and can be
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
 *   TransactionSingleOp sc = new TransactionSingleOp();
 *   String value             = sc.read(key).stringValue(); // {@link #read(String)}
 *   OtpErlangObject optValue = sc.read(otpKey).value();    // {@link #read(OtpErlangString)}
 * </code>
 * </pre>
 * 
 * <p>For the full example, see
 * {@link de.zib.scalaris.examples.TransactionSingleOpReadExample}</p>
 * 
 * <h3>Writing values</h3>
 * <pre>
 * <code style="white-space:pre;">
 *   String key;
 *   String value;
 *   OtpErlangString otpKey;
 *   OtpErlangString otpValue;
 *   
 *   TransactionSingleOp sc = new TransactionSingleOp();
 *   sc.write(key, value);             // {@link #write(String, Object)}
 *   sc.writeObject(otpKey, otpValue); // {@link #write(OtpErlangString, OtpErlangObject)}
 * </code>
 * </pre>
 * 
 * <p>For the full example, see
 * {@link de.zib.scalaris.examples.TransactionSingleOpWriteExample}</p>
 * 
 * <h3>Connection errors</h3>
 * 
 * Errors when setting up connections or trying to send/receive RPCs will be
 * handed to the {@link ConnectionPolicy} that has been set when the connection
 * was created. By default, {@link ConnectionFactory} uses
 * {@link DefaultConnectionPolicy} which implements automatic connection
 * retries by classifying nodes as good or bad depending on their previous
 * state. The number of automatic retries is adjustable (default: 3).
 * 
 * @author Nico Kruber, kruber@zib.de
 * @version 3.4
 * @since 2.0
 */
public class TransactionSingleOp {
    /**
     * Connection to a TransactionSingleOp node.
     */
    private Connection connection;
    
    /**
     * Constructor, uses the default connection returned by
     * {@link ConnectionFactory#createConnection()}.
     * 
     * @throws ConnectionException
     *             if the connection fails
     */
    public TransactionSingleOp() throws ConnectionException {
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
    public TransactionSingleOp(Connection conn) throws ConnectionException {
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
     * 
     * @since 2.9
     */
    public ErlangValue read(OtpErlangString key)
            throws ConnectionException, TimeoutException, NotFoundException,
            UnknownException {
        OtpErlangObject received_raw = connection.doRPC("api_tx", "read",
                new OtpErlangList(key));
        return new ErlangValue(CommonErlangObjects.processResult_read(received_raw));
    }

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
     * 
     * @see #read(OtpErlangString)
     * @since 2.9
     */
    public ErlangValue read(String key) throws ConnectionException,
            TimeoutException, NotFoundException, UnknownException {
        return read(new OtpErlangString(key));
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
     * 
     * @since 2.9
     */
    public void write(OtpErlangString key, OtpErlangObject value)
            throws ConnectionException, TimeoutException, AbortException, UnknownException {
        OtpErlangObject received_raw = connection.doRPC("api_tx", "write",
                new OtpErlangObject[] { key, value });
        CommonErlangObjects.processResult_commit(received_raw);
    }

    /**
     * Stores the given <tt>key</tt>/<tt>value</tt> pair.
     * 
     * @param <T>
     *            the type of the value to store.
     *            See {@link ErlangValue} for a list of supported types.
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
     * @see #write(OtpErlangString, OtpErlangObject)
     * @since 2.9
     */
    public <T> void write(String key, T value) throws ConnectionException,
            TimeoutException, AbortException, UnknownException {
        write(new OtpErlangString(key), ErlangValue.convertToErlang(value));
    }

    // /////////////////////////////
    // test and set
    // /////////////////////////////

    /**
     * Stores the given <tt>key</tt>/<tt>new_value</tt> pair if the old value
     * at <tt>key</tt> is <tt>old_value</tt> (atomic test_and_set).
     * 
     * @param key
     *            the key to store the value for
     * @param old_value
     *            the old value to check
     * @param new_value
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
     * @throws NotFoundException
     *             if the requested key does not exist
     * @throws UnknownException
     *             if any other error occurs
     * @throws KeyChangedException
     *             if the key did not match <tt>old_value</tt>
     * 
     * @since 2.9
     */
    public void testAndSet(OtpErlangString key,
            OtpErlangObject old_value, OtpErlangObject new_value)
            throws ConnectionException, TimeoutException, AbortException,
            NotFoundException, KeyChangedException, UnknownException {
        OtpErlangObject received_raw = connection.doRPC("api_tx", "test_and_set",
                new OtpErlangObject[] { key, old_value, new_value });
        /*
         * possible return values:
         *  {ok} | {fail, timeout | abort | not_found | {key_changed, RealOldValue}
         */
        try {
            OtpErlangTuple received = (OtpErlangTuple) received_raw;
            if (received.equals(CommonErlangObjects.okTupleAtom)) {
                return;
            } else if (received.elementAt(0).equals(CommonErlangObjects.failAtom) && received.arity() == 2) {
                OtpErlangObject reason = received.elementAt(1);
                if (reason.equals(CommonErlangObjects.timeoutAtom)) {
                    throw new TimeoutException(received_raw);
                } else if (reason.equals(CommonErlangObjects.abortAtom)) {
                    throw new AbortException(received_raw);
                } else if (reason.equals(CommonErlangObjects.notFoundAtom)) {
                    throw new NotFoundException(received_raw);
                } else {
                    OtpErlangTuple reason_tpl = (OtpErlangTuple) reason;
                    if (reason_tpl.elementAt(0).equals(
                            CommonErlangObjects.keyChangedAtom)
                            && reason_tpl.arity() == 2) {
                        throw new KeyChangedException(reason_tpl.elementAt(1));
                    }
                }
            }
            throw new UnknownException(received_raw);
        } catch (ClassCastException e) {
            // e.printStackTrace();
            throw new UnknownException(e, received_raw);
        }
    }

    /**
     * Stores the given <tt>key</tt>/<tt>new_value</tt> pair if the old value
     * at <tt>key</tt> is <tt>old_value</tt> (atomic test_and_set).
     * 
     * @param <OldT>
     *            the type of the stored (old) value.
     *            See {@link ErlangValue} for a list of supported types.
     * @param <NewT>
     *            the type of the (new) value to store.
     *            See {@link ErlangValue} for a list of supported types.
     * @param key
     *            the key to store the value for
     * @param old_value
     *            the old value to check
     * @param new_value
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
     * @throws NotFoundException
     *             if the requested key does not exist
     * @throws KeyChangedException
     *             if the key did not match <tt>old_value</tt>
     * @throws UnknownException
     *             if any other error occurs
     *
     * @see #testAndSet(OtpErlangString, OtpErlangObject, OtpErlangObject)
     * @since 2.9
     */
    public <OldT, NewT> void testAndSet(String key, OldT old_value, NewT new_value)
            throws ConnectionException, TimeoutException, AbortException,
            NotFoundException, KeyChangedException, UnknownException {
        testAndSet(new OtpErlangString(key),
                ErlangValue.convertToErlang(old_value),
                ErlangValue.convertToErlang(new_value));
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
