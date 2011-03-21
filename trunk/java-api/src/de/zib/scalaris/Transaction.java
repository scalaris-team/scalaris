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
import java.util.ArrayList;
import java.util.List;

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
 *   Transaction t1 = new Transaction();  // {@link #Transaction()}
 *   
 *   t1.write(key, value);                // {@link #write(String, Object)}
 *   t1.write(otpKey, otpValue);          // {@link #write(OtpErlangString, OtpErlangObject)}
 *   
 *   result = t1.read(key).stringValue(); //{@link #read(String)}
 *   otpResult = t1.readObject(otpKey);   //{@link #read(OtpErlangString)}
 *   
 *   transaction.commit(); // {@link #commit()}
 * </code>
 * </pre>
 * 
 * <p>
 * For more examples, have a look at
 * {@link de.zib.scalaris.examples.TransactionReadExample},
 * {@link de.zib.scalaris.examples.TransactionWriteExample} and
 * {@link de.zib.scalaris.examples.TransactionReadWriteExample}.
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
 * @version 2.9
 * @since 2.0
 */
public class Transaction {
    /**
     * erlang transaction log
     */
    private OtpErlangObject transLog = null;
    
    private OtpErlangList lastResult = null;

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
     * Executes all requests in <code>req</code>.
     * 
     * @param req
     *            the request to issue
     * 
     * @return
     *            results of read requests in the same order as they appear in
     *            <code>req</code>
     * 
     * @throws ConnectionException
     *             if the connection is not active or a communication error
     *             occurs or an exit signal was received or the remote node
     *             sends a message containing an invalid cookie
     * @throws TimeoutException
     *             if a timeout occurred while trying to execute the requests
     * @throws NotFoundException
     *             if a requested key did not exist
     * @throws AbortException
     *             if the request contained a commit which was failed
     * @throws UnknownException
     *             if any other error occurs
     */
    private List<OtpErlangObject> req_list(OtpErlangList req)
            throws ConnectionException, TimeoutException, NotFoundException,
            AbortException, UnknownException {
        OtpErlangObject received_raw = null;
        try {
            /*
             * possible return values:
             *  {tx_tlog:tlog(), [{ok} | {ok, Value} | {fail, abort | timeout | not_found}]}
             */
            if (transLog == null) {
                received_raw = connection.doRPC("api_tx", "req_list",
                        new OtpErlangList(new OtpErlangObject[] { req }));
            } else {
                received_raw = connection.doRPC("api_tx", "req_list",
                        new OtpErlangList(
                                new OtpErlangObject[] { transLog, req }));
            }
            
            OtpErlangTuple received = (OtpErlangTuple) received_raw;
            transLog = received.elementAt(0);
            lastResult = (OtpErlangList) received.elementAt(1);
            
            // TODO: go through req and check result for each command!
            // TODO: improve handling if only a single op fails but the others
            // are ok, e.g. if a key is not found
            ArrayList<OtpErlangObject> result = new ArrayList<OtpErlangObject>(lastResult.arity());
            for (OtpErlangObject result_i : lastResult) {
                OtpErlangTuple result_i_tpl = (OtpErlangTuple) result_i;
                OtpErlangAtom result_i_state = (OtpErlangAtom) result_i_tpl.elementAt(0);
                if(result_i_tpl.equals(CommonErlangObjects.okTupleAtom)) {
                    // result of a commit request
                } else if (result_i_state.equals(CommonErlangObjects.okAtom) && result_i_tpl.arity() == 2) {
                    // contains a value
                    result.add(result_i_tpl.elementAt(1));
                } else if (result_i_state.equals(CommonErlangObjects.failAtom) && result_i_tpl.arity() == 2) {
                    // transaction failed
                    OtpErlangObject reason = result_i_tpl.elementAt(1);
                    if (reason.equals(CommonErlangObjects.abortAtom)) {
                        abort();
                        throw new AbortException(received_raw);
                    } else if (reason.equals(CommonErlangObjects.timeoutAtom)) {
                        throw new TimeoutException(received_raw);
                    } else if (reason.equals(CommonErlangObjects.notFoundAtom)) {
                        throw new NotFoundException(received_raw);
                    } else {
                        throw new UnknownException(received_raw);
                    }
                } else {
                    throw new UnknownException(received_raw);
                }
            }
            result.trimToSize();
            return result;
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
     * aborted or reset in order to be (re-)used for another transaction.
     * </p>
     * 
     * @throws ConnectionException
     *             if the connection is not active or a communication error
     *             occurs or an exit signal was received or the remote node
     *             sends a message containing an invalid cookie
     * @throws TimeoutException
     *             if a timeout occurred while trying to commit the transaction
     * @throws AbortException
     *             if the commit failed
     * @throws UnknownException
     *             If the commit fails or the returned value from erlang is of
     *             an unknown type/structure, this exception is thrown. Neither
     *             the transaction log nor the local operations buffer is
     *             emptied, so that the commit can be tried again.
     * 
     * @see #abort()
     */
    public void commit() throws ConnectionException, TimeoutException, AbortException, UnknownException {
        try {
            // not_found should never be returned by a commit
            OtpErlangList req = new OtpErlangList(CommonErlangObjects.commitTupleAtom);
            req_list(req);
            // transaction was successful: reset transaction log
            transLog = null;
        } catch (NotFoundException e) {
            // e.printStackTrace();
            throw new UnknownException(e);
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
     */
    public void abort() {
        transLog = null;
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
     * 
     * @since 2.9
     */
    public OtpErlangObject read(OtpErlangString key)
            throws ConnectionException, TimeoutException, NotFoundException,
            UnknownException {
        try {
            // abort should never be returned by a read
            OtpErlangList req =
                new OtpErlangList(
                        new OtpErlangTuple(
                                new OtpErlangObject[] {CommonErlangObjects.readAtom, key}));
            List<OtpErlangObject> res = req_list(req);
            return res.get(0);
        } catch (AbortException e) {
            // e.printStackTrace();
            throw new UnknownException(e);
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
     * @see #read(OtpErlangString)
     * @since 2.9
     */
    public ErlangValue read(String key) throws ConnectionException,
            TimeoutException, NotFoundException, UnknownException {
        return new ErlangValue(read(new OtpErlangString(key)));
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
     * @since 2.9
     */
    public void write(OtpErlangString key, OtpErlangObject value)
            throws ConnectionException, TimeoutException, UnknownException {
        try {
            // abort and not_found should never be returned by a write
            OtpErlangList req =
                new OtpErlangList(
                        new OtpErlangTuple(
                                new OtpErlangObject[] {CommonErlangObjects.writeAtom, key, value}));
            req_list(req);
        } catch (AbortException e) {
            // e.printStackTrace();
            throw new UnknownException(e);
        } catch (NotFoundException e) {
            // e.printStackTrace();
            throw new UnknownException(e);
        }
    }

    /**
     * Stores the given <code>key</code>/<code>value</code> pair.
     * 
     * @param <T>
     *            the type of the <tt>value</tt>
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
     * @see #write(OtpErlangString, OtpErlangObject)
     * @since 2.9
     */
    public <T> void write(String key, T value) throws ConnectionException,
            TimeoutException, UnknownException {
        write(new OtpErlangString(key), new ErlangValue(value).value());
    }

    /**
     * Gets the raw result list of the last request list send to erlang.
     * 
     * This may be useful for debugging if one of the operations fails with an
     * exception.
     * 
     * @return the most recent result list (may be <code>null</code>)
     */
    public OtpErlangList getLastResult() {
        return lastResult;
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
