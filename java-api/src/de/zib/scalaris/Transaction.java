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

import java.util.ArrayList;
import java.util.List;

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
 *   otpResult = t1.read(otpKey).value(); //{@link #read(OtpErlangString)}
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
 * @version 3.4
 * @since 2.0
 */
public class Transaction {
    /**
     * erlang transaction log
     */
    private OtpErlangObject transLog = null;

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
     * Encapsulates requests that can be used for transactions in
     * {@link Transaction#req_list(RequestList)}.
     * 
     * @author Nico Kruber, kruber@zib.de
     * 
     * @since 3.4
     */
    public static class RequestList {
        private List<OtpErlangObject> requests = new ArrayList<OtpErlangObject>(10);
        
        /**
         * Default constructor.
         */
        public RequestList() {
        }
        
        /**
         * Adds a read operation to the list of requests.
         * 
         * @param key  the key to read
         * 
         * @return this {@link RequestList} object
         */
        public RequestList addRead(OtpErlangObject key) {
            OtpErlangTuple req = new OtpErlangTuple(new OtpErlangObject[] {
                    CommonErlangObjects.readAtom, key });
            requests.add(req);
            return this;
        }

        /**
         * Adds a read operation to the list of requests.
         * 
         * @param key  the key to read
         * 
         * @return this {@link RequestList} object
         */
        public RequestList addRead(String key) {
            return addRead(new OtpErlangString(key));
        }

        /**
         * Adds a write operation to the list of requests.
         * 
         * @param key    the key to write the value to
         * @param value  the value to write
         * 
         * @return this {@link RequestList} object
         */
        public RequestList addWrite(OtpErlangObject key, OtpErlangObject value) {
            OtpErlangTuple req = new OtpErlangTuple(new OtpErlangObject[] {
                    CommonErlangObjects.writeAtom, key, value });
            requests.add(req);
            return this;
        }

        /**
         * Adds a write operation to the list of requests.
         * 
         * @param key    the key to write the value to
         * @param value  the value to write
         * 
         * @return this {@link RequestList} object
         */
        public <T>  RequestList addWrite(String key, T value) {
            return addWrite(new OtpErlangString(key), ErlangValue.convertToErlang(value));
        }

        /**
         * Adds a commit operation to the list of requests.
         * 
         * @return this {@link RequestList} object
         */
        public RequestList addCommit() {
            OtpErlangTuple req = CommonErlangObjects.commitTupleAtom;
            requests.add(req);
            return this;
        }

        /**
         * Gets the whole request list as erlang terms as required by
         * <code>api_tx:req_list/2</code>
         * 
         * @return an erlang list of requests
         */
        OtpErlangList getErlangReqList() {
            return new OtpErlangList(requests.toArray(new OtpErlangObject[0]));
        }
    }
    
    /**
     * Encapsulates a list of results as returned by
     * {@link Transaction#req_list(RequestList)}.
     * 
     * @author Nico Kruber, kruber@zib.de
     * 
     * @since 3.4
     */
    public final static class ResultList {
        private OtpErlangList results = new OtpErlangList();
        
        /**
         * Default constructor.
         * 
         * @param results  the raw results list as returned by scalaris.
         */
        ResultList(OtpErlangList results) {
            this.results = results;
        }
        
        /**
         * Gets the number of results in the list.
         * 
         * @return total number of results
         */
        public int size() {
            return results.arity();
        }

        /**
         * Processes the result at the given position which originated from
         * a read request and returns the value that has been read.
         * 
         * @param pos
         *            the position in the result list
         * 
         * @return the stored value 
         * 
         * @throws TimeoutException
         *             if a timeout occurred while trying to fetch the value
         * @throws NotFoundException
         *             if the requested key does not exist
         * @throws UnknownException
         *             if any other error occurs
         */
        public ErlangValue processReadAt(int pos) throws TimeoutException,
                NotFoundException, UnknownException {
            return new ErlangValue(
                    CommonErlangObjects.processResult_read(results.elementAt(pos)));
        }

        /**
         * Processes the result at the given position which originated from
         * a write request.
         * 
         * @param pos
         *            the position in the result list
         *
         * @throws TimeoutException
         *             if a timeout occurred while trying to write the value
         * @throws UnknownException
         *             if any other error occurs
         */
        public void processWriteAt(int pos) throws TimeoutException,
                UnknownException {
            CommonErlangObjects.processResult_write(results.elementAt(pos));
        }

        /**
         * Processes the result at the given position which originated from
         * a commit request.
         * 
         * @param pos
         *            the position in the result list
         *
         * @throws TimeoutException
         *             if a timeout occurred while trying to write the value
         * @throws AbortException
         *             if the commit failed
         * @throws UnknownException
         *             if any other error occurs
         */
        public void processCommitAt(int pos) throws TimeoutException,
                AbortException, UnknownException {
            CommonErlangObjects.processResult_commit(results.elementAt(pos));
        }
        
        /**
         * Gets the raw results.
         * (for internal use)
         * 
         * @return results as returned by erlang
         */
        OtpErlangList getResults() {
            return results;
        }
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
     * @throws UnknownException
     *             if any other error occurs
     */
    public ResultList req_list(RequestList req)
            throws ConnectionException, UnknownException {
        OtpErlangObject received_raw = null;
        if (transLog == null) {
            received_raw = connection.doRPC("api_tx", "req_list",
                    new OtpErlangObject[] { req.getErlangReqList() });
        } else {
            received_raw = connection.doRPC("api_tx", "req_list",
                    new OtpErlangObject[] { transLog, req.getErlangReqList() });
        }
        try {
            /*
             * possible return values:
             *  {tx_tlog:tlog(), [{ok} | {ok, Value} | {fail, abort | timeout | not_found}]}
             */
            OtpErlangTuple received = (OtpErlangTuple) received_raw;
            transLog = received.elementAt(0);
            if (received.arity() == 2) {
                return new ResultList((OtpErlangList) received.elementAt(1));
            }
            throw new UnknownException(received_raw);
        } catch (ClassCastException e) {
            // e.printStackTrace();
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
        ResultList result = req_list(new RequestList().addCommit());
        if (result.size() == 1) {
            result.processCommitAt(0);
            // transaction was successful: reset transaction log
            transLog = null;
        } else {
            throw new UnknownException(result.getResults());
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
     * @return the value stored under the given <code>key</code>
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
        ResultList result = req_list(new RequestList().addRead(key));
        if (result.size() == 1) {
            return result.processReadAt(0);
        }
        throw new UnknownException(result.getResults());
    }

    /**
     * Gets the value stored under the given <code>key</code>.
     * 
     * @param key
     *            the key to look up
     *
     * @return the value stored under the given <code>key</code>
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
        ResultList result = req_list(new RequestList().addWrite(key, value));
        if (result.size() == 1) {
            result.processWriteAt(0);
        } else {
            throw new UnknownException(result.getResults());
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
        write(new OtpErlangString(key), ErlangValue.convertToErlang(value));
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
