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


import java.util.List;

import com.ericsson.otp.erlang.OtpErlangDouble;
import com.ericsson.otp.erlang.OtpErlangList;
import com.ericsson.otp.erlang.OtpErlangLong;
import com.ericsson.otp.erlang.OtpErlangObject;
import com.ericsson.otp.erlang.OtpErlangString;
import com.ericsson.otp.erlang.OtpErlangTuple;

import de.zib.scalaris.operations.AddDelOnListOp;
import de.zib.scalaris.operations.AddOnNrOp;
import de.zib.scalaris.operations.Operation;
import de.zib.scalaris.operations.ReadOp;
import de.zib.scalaris.operations.TestAndSetOp;
import de.zib.scalaris.operations.TransactionOperation;
import de.zib.scalaris.operations.WriteOp;

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
 * @version 3.9
 * @since 2.0
 */
public class Transaction {
    /**
     * Erlang transaction log.
     */
    private OtpErlangObject transLog = null;

    /**
     * Connection to a Scalaris node.
     */
    private final Connection connection;

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
     */
    public Transaction(final Connection conn) {
        connection = conn;
    }

    /**
     * Encapsulates requests that can be used for transactions in
     * {@link Transaction#req_list(RequestList)}.
     *
     * @author Nico Kruber, kruber@zib.de
     * @version 3.5
     * @since 3.4
     */
    public static class RequestList extends de.zib.scalaris.RequestList {
        /**
         * Default constructor.
         */
        public RequestList() {
            super();
        }

        /**
         * Copy constructor.
         *
         * @param other the request list to copy from
         */
        public RequestList(final RequestList other) {
            super(other);
        }

        /* (non-Javadoc)
         * @see de.zib.scalaris.RequestList#addOp(de.zib.scalaris.operations.Operation)
         */
        @Override
        public RequestList addOp(final Operation op)
                throws UnsupportedOperationException {
            if (!(op instanceof TransactionOperation)) {
                throw new UnsupportedOperationException();
            }
            return (RequestList) super.addOp(op);
        }

        /**
         * Adds all requests of the other request list to the end of this list.
         *
         * @param other another request list
         *
         * @return this {@link RequestList} object
         */
        public RequestList addAll(final RequestList other) {
            return (RequestList) super.addAll_(other);
        }
    }

    /**
     * Encapsulates a list of results as returned by
     * {@link Transaction#req_list(RequestList)}.
     *
     * @author Nico Kruber, kruber@zib.de
     * @version 3.5
     * @since 3.4
     */
    public static class ResultList extends de.zib.scalaris.ResultList {
        /**
         * Default constructor.
         *
         * @param results  the raw results list as returned by scalaris.
         */
        ResultList(final OtpErlangList results) {
            super(results);
        }

        /**
         * Processes the result at the given position which originated from a read
         * request and returns the value that has been read.
         *
         * @param pos
         *            the position in the result list (starting at 0)
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
        @Override
        public ErlangValue processReadAt(final int pos) throws TimeoutException,
                NotFoundException, UnknownException {
            return new ErlangValue(
                    CommonErlangObjects.processResult_read(results.elementAt(pos)));
        }

        /**
         * Processes the result at the given position which originated from
         * a write request.
         *
         * @param pos
         *            the position in the result list (starting at 0)
         *
         * @throws TimeoutException
         *             if a timeout occurred while trying to write the value
         * @throws UnknownException
         *             if any other error occurs
         */
        @Override
        public void processWriteAt(final int pos) throws TimeoutException,
                UnknownException {
            CommonErlangObjects.processResult_write(results.elementAt(pos));
        }

        /**
         * Processes the result at the given position which originated from
         * a add_del_on_list request.
         *
         * @param pos
         *            the position in the result list (starting at 0)
         *
         * @throws TimeoutException
         *             if a timeout occurred while trying to write the value
         * @throws NotAListException
         *             if the previously stored value was no list
         * @throws UnknownException
         *             if any other error occurs
         *
         * @since 3.9
         */
        @Override
        public void processAddDelOnListAt(final int pos) throws TimeoutException,
                NotAListException, UnknownException {
            CommonErlangObjects.processResult_addDelOnList(results.elementAt(pos));
        }

        /**
         * Processes the result at the given position which originated from
         * an add_on_nr request.
         *
         * @param pos
         *            the position in the result list (starting at 0)
         *
         * @throws TimeoutException
         *             if a timeout occurred while trying to write the value
         * @throws NotANumberException
         *             if the previously stored value was not a number
         * @throws UnknownException
         *             if any other error occurs
         *
         * @since 3.9
         */
        @Override
        public void processAddOnNrAt(final int pos) throws TimeoutException,
                NotANumberException, UnknownException {
            CommonErlangObjects.processResult_addOnNr(results.elementAt(pos));
        }

        /**
         * Processes the result at the given position which originated from
         * an add_on_nr request.
         *
         * @param pos
         *            the position in the result list (starting at 0)
         *
         * @throws TimeoutException
         *             if a timeout occurred while trying to fetch/write the value
         * @throws NotFoundException
         *             if the requested key does not exist
         * @throws KeyChangedException
         *             if the key did not match <tt>old_value</tt>
         * @throws UnknownException
         *             if any other error occurs
         *
         * @since 3.8
         */
        @Override
        public void processTestAndSetAt(final int pos) throws TimeoutException,
                NotFoundException, KeyChangedException, UnknownException {
            CommonErlangObjects.processResult_testAndSet(results.elementAt(pos));
        }

        /**
         * Processes the result at the given position which originated from
         * a commit request.
         *
         * Note: it is not necessary to call this method manually! A commit at
         * the end of a request list will be evaluated automatically!
         *
         * @param pos
         *            the position in the result list (starting at 0)
         *
         * @throws TimeoutException
         *             if a timeout occurred while trying to write the value
         * @throws AbortException
         *             if the commit failed
         * @throws UnknownException
         *             if any other error occurs
         */
        public void processCommitAt(final int pos) throws TimeoutException,
                AbortException, UnknownException {
            CommonErlangObjects.processResult_commit(results.elementAt(pos));
        }
    }

    /**
     * Executes all requests in <code>req</code>.
     *
     * <p>
     * The transaction's log is reset if a commit in the request list was
     * successful, otherwise it still retains in the transaction which must be
     * successfully committed, aborted or reset in order to be (re-)used for
     * another request.
     * </p>
     *
     * @param req
     *            the requests to issue
     *
     * @return results of all requests in the same order as they appear in
     *         <code>req</code>
     *
     * @throws ConnectionException
     *             if the connection is not active or a communication error
     *             occurs or an exit signal was received or the remote node
     *             sends a message containing an invalid cookie
     * @throws TimeoutException
     *             if a timeout occurred while trying to write the value
     * @throws AbortException
     *             if the commit failed
     * @throws UnknownException
     *             if any other error occurs
     */
    public ResultList req_list(final RequestList req)
            throws ConnectionException, TimeoutException, AbortException, UnknownException {
        if (req.isEmpty()) {
            return new ResultList(new OtpErlangList());
        }
        OtpErlangObject received_raw = null;
        final OtpErlangList erlangReqList = req.getErlangReqList();
        if (transLog == null) {
            received_raw = connection.doRPC("api_tx", "req_list",
                    new OtpErlangObject[] { erlangReqList });
        } else {
            received_raw = connection.doRPC("api_tx", "req_list",
                    new OtpErlangObject[] { transLog, erlangReqList });
        }
        try {
            /*
             * possible return values:
             *  {tx_tlog:tlog(), [{ok} | {ok, Value} | {fail, abort | timeout | not_found}]}
             */
            final OtpErlangTuple received = (OtpErlangTuple) received_raw;
            transLog = received.elementAt(0);
            if (received.arity() == 2) {
                final ResultList result = new ResultList((OtpErlangList) received.elementAt(1));
                if (req.isCommit()) {
                    if (result.size() >= 1) {
                        result.processCommitAt(result.size() - 1);
                        // transaction was successful: reset transaction log
                        transLog = null;
                    } else {
                        throw new UnknownException(result.getResults());
                    }
                }
                return result;
            }
            throw new UnknownException(received_raw);
        } catch (final ClassCastException e) {
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
     * aborted or reset in order to be (re-)used for another request.
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
        req_list((RequestList) new RequestList().addCommit());
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
    public ErlangValue read(final OtpErlangString key)
            throws ConnectionException, TimeoutException, NotFoundException,
            UnknownException {
        try {
            final ResultList result = req_list(new RequestList().addOp(new ReadOp(key)));
            if (result.size() == 1) {
                return result.processReadAt(0);
            }
            throw new UnknownException(result.getResults());
        } catch (final AbortException e) {
            // should not occur (we did not commit anything)
            throw new UnknownException(e);
        }
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
    public ErlangValue read(final String key) throws ConnectionException,
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
    public void write(final OtpErlangString key, final OtpErlangObject value)
            throws ConnectionException, TimeoutException, UnknownException {
        try {
            final ResultList result = req_list(new RequestList().addOp(new WriteOp(key, value)));
            if (result.size() == 1) {
                result.processWriteAt(0);
            } else {
                throw new UnknownException(result.getResults());
            }
        } catch (final AbortException e) {
            // should not occur (we did not commit anything)
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
    public <T> void write(final String key, final T value) throws ConnectionException,
            TimeoutException, UnknownException {
        write(new OtpErlangString(key), ErlangValue.convertToErlang(value));
    }

    /**
     * Changes the list stored at the given key, i.e. first adds all items in
     * <tt>toAdd</tt> then removes all items in <tt>toRemove</tt>.
     * Assumes en empty list if no value exists at <tt>key</tt>.
     *
     * @param key
     *            the key to write the value to
     * @param toAdd
     *            a list of values to add to a list
     * @param toRemove
     *            a list of values to remove from a list
     *
     * @throws ConnectionException
     *             if the connection is not active or a communication error
     *             occurs or an exit signal was received or the remote node
     *             sends a message containing an invalid cookie
     * @throws TimeoutException
     *             if a timeout occurred while trying to write the value
     * @throws NotAListException
     *             if the previously stored value was no list
     * @throws UnknownException
     *             if any other error occurs
     *
     * @since 3.9
     */
    public void addDelOnList(final OtpErlangString key, final OtpErlangList toAdd,
            final OtpErlangList toRemove) throws ConnectionException,
            TimeoutException, NotAListException, UnknownException {
        try {
            final RequestList reqs = new RequestList();
            reqs.addOp(new AddDelOnListOp(key, toAdd, toRemove));
            final ResultList result = req_list(reqs);
            if (result.size() == 1) {
                result.processAddDelOnListAt(0);
            } else {
                throw new UnknownException(result.getResults());
            }
        } catch (final AbortException e) {
            // should not occur (we did not commit anything)
            throw new UnknownException(e);
        }
    }

    /**
     * Changes the list stored at the given key, i.e. first adds all items in
     * <tt>toAdd</tt> then removes all items in <tt>toRemove</tt>.
     * Assumes en empty list if no value exists at <tt>key</tt>.
     *
     * @param key
     *            the key to write the value to
     * @param toAdd
     *            a list of values to add to a list
     * @param toRemove
     *            a list of values to remove from a list
     *
     * @throws ConnectionException
     *             if the connection is not active or a communication error
     *             occurs or an exit signal was received or the remote node
     *             sends a message containing an invalid cookie
     * @throws TimeoutException
     *             if a timeout occurred while trying to write the value
     * @throws NotAListException
     *             if the previously stored value was no list
     * @throws UnknownException
     *             if any other error occurs
     *
     * @see #addDelOnList(OtpErlangString, OtpErlangList, OtpErlangList)
     * @since 3.9
     */
    public <T> void addDelOnList(final String key, final List<T> toAdd,
            final List<T> toRemove) throws ConnectionException,
            TimeoutException, NotAListException, UnknownException {
        OtpErlangList toAddErl;
        OtpErlangList toRemoveErl;
        try {
            toAddErl = (OtpErlangList) ErlangValue.convertToErlang(toAdd);
            toRemoveErl = (OtpErlangList) ErlangValue.convertToErlang(toRemove);
        } catch (final ClassCastException e) {
            // one of the parameters was no list
            // note: a ClassCastException inside ErlangValue.convertToErlang is
            // converted to an UnknownException
            throw new NotAListException(e);
        }
        addDelOnList(new OtpErlangString(key), toAddErl, toRemoveErl);
    }

    /**
     * Changes the number stored at the given key, i.e. adds some value.
     * Assumes <tt>0</tt> if no value exists at <tt>key</tt>.
     *
     * @param key
     *            the key to write the value to
     * @param toAdd
     *            the number to add to the number stored at key (may also be
     *            negative)
     *
     * @throws ConnectionException
     *             if the connection is not active or a communication error
     *             occurs or an exit signal was received or the remote node
     *             sends a message containing an invalid cookie
     * @throws TimeoutException
     *             if a timeout occurred while trying to write the value
     * @throws NotANumberException
     *             if the previously stored value was no number
     * @throws UnknownException
     *             if any other error occurs
     *
     * @see #addOnNr_(RequestList)
     * @since 3.9
     */
    public void addOnNr(final OtpErlangString key, final OtpErlangLong toAdd)
            throws ConnectionException, TimeoutException, NotANumberException,
            UnknownException {
        final RequestList reqs = new RequestList();
        reqs.addOp(new AddOnNrOp(key, toAdd));
        addOnNr_(reqs);
    }

    /**
     * Changes the number stored at the given key, i.e. adds some value.
     * Assumes <tt>0</tt> if no value exists at <tt>key</tt>.
     *
     * @param key
     *            the key to write the value to
     * @param toAdd
     *            the number to add to the number stored at key (may also be
     *            negative)
     *
     * @throws ConnectionException
     *             if the connection is not active or a communication error
     *             occurs or an exit signal was received or the remote node
     *             sends a message containing an invalid cookie
     * @throws TimeoutException
     *             if a timeout occurred while trying to write the value
     * @throws NotANumberException
     *             if the previously stored value was no number
     * @throws UnknownException
     *             if any other error occurs
     *
     * @see #addOnNr_(RequestList)
     * @since 3.9
     */
    public void addOnNr(final OtpErlangString key, final OtpErlangDouble toAdd)
            throws ConnectionException, TimeoutException, NotANumberException,
            UnknownException {
        final RequestList reqs = new RequestList();
        reqs.addOp(new AddOnNrOp(key, toAdd));
        addOnNr_(reqs);
    }

    /**
     * Changes the number stored at the given key, i.e. adds some value.
     * Assumes <tt>0</tt> if no value exists at <tt>key</tt>.
     *
     * @param reqs
     *            the request list containing a single add_on_nr operation
     *
     * @throws ConnectionException
     *             if the connection is not active or a communication error
     *             occurs or an exit signal was received or the remote node
     *             sends a message containing an invalid cookie
     * @throws TimeoutException
     *             if a timeout occurred while trying to write the value
     * @throws NotANumberException
     *             if the previously stored value was no number
     * @throws UnknownException
     *             if any other error occurs
     *
     * @see #addOnNr(OtpErlangObject, OtpErlangLong)
     * @see #addOnNr(OtpErlangObject, OtpErlangDouble)
     * @since 3.9
     */
    protected void addOnNr_(final RequestList reqs)
            throws ConnectionException, TimeoutException, NotANumberException,
            UnknownException {
        try {
            final ResultList result = req_list(reqs);
            if (result.size() == 1) {
                result.processAddOnNrAt(0);
            } else {
                throw new UnknownException(result.getResults());
            }
        } catch (final AbortException e) {
            // should not occur (we did not commit anything)
            throw new UnknownException(e);
        }
    }

    /**
     * Changes the number stored at the given key, i.e. adds some value.
     * Assumes <tt>0</tt> if no value exists at <tt>key</tt>.
     *
     * @param key
     *            the key to write the value to
     * @param toAdd
     *            the number to add to the number stored at key (may also be
     *            negative)
     *
     * @throws ConnectionException
     *             if the connection is not active or a communication error
     *             occurs or an exit signal was received or the remote node
     *             sends a message containing an invalid cookie
     * @throws TimeoutException
     *             if a timeout occurred while trying to write the value
     * @throws NotANumberException
     *             if the previously stored value was no number
     * @throws UnknownException
     *             if any other error occurs
     *
     * @see #addOnNr(OtpErlangString, OtpErlangLong)
     * @see #addOnNr(OtpErlangString, OtpErlangDouble)
     * @since 3.9
     */
    public <T> void addOnNr(final String key, final T toAdd)
            throws ConnectionException, TimeoutException, NotANumberException,
            UnknownException {
        final OtpErlangObject toAddErl = ErlangValue.convertToErlang(toAdd);
        if (toAddErl instanceof OtpErlangLong) {
            addOnNr(new OtpErlangString(key), (OtpErlangLong) toAddErl);
        } else if (toAddErl instanceof OtpErlangDouble) {
            addOnNr(new OtpErlangString(key), (OtpErlangDouble) toAddErl);
        } else {
            throw new NotANumberException(toAddErl);
        }
    }

    /**
     * Stores the given <tt>key</tt>/<tt>new_value</tt> pair if the old value
     * at <tt>key</tt> is <tt>old_value</tt> (atomic test_and_set).
     *
     * @param key
     *            the key to store the value for
     * @param oldValue
     *            the old value to check
     * @param newValue
     *            the value to store
     *
     * @throws ConnectionException
     *             if the connection is not active or a communication error
     *             occurs or an exit signal was received or the remote node
     *             sends a message containing an invalid cookie
     * @throws TimeoutException
     *             if a timeout occurred while trying to write the value
     * @throws NotFoundException
     *             if the requested key does not exist
     * @throws KeyChangedException
     *             if the key did not match <tt>old_value</tt>
     * @throws UnknownException
     *             if any other error occurs
     *
     * @since 3.9
     */
    public void testAndSet(final OtpErlangString key,
            final OtpErlangObject oldValue, final OtpErlangObject newValue)
            throws ConnectionException, TimeoutException, NotFoundException,
            KeyChangedException, UnknownException {
        try {
            final RequestList reqs = new RequestList();
            reqs.addOp(new TestAndSetOp(key, oldValue, newValue));
            final ResultList result = req_list(reqs);
            if (result.size() == 1) {
                result.processTestAndSetAt(0);
            } else {
                throw new UnknownException(result.getResults());
            }
        } catch (final AbortException e) {
            // should not occur (we did not commit anything)
            throw new UnknownException(e);
        }
    }

    /**
     * Stores the given <tt>key</tt>/<tt>new_value</tt> pair if the old value
     * at <tt>key</tt> is <tt>old_value</tt> (atomic test_and_set).
     *
     * @param key
     *            the key to store the value for
     * @param oldValue
     *            the old value to check
     * @param newValue
     *            the value to store
     *
     * @throws ConnectionException
     *             if the connection is not active or a communication error
     *             occurs or an exit signal was received or the remote node
     *             sends a message containing an invalid cookie
     * @throws TimeoutException
     *             if a timeout occurred while trying to write the value
     * @throws NotFoundException
     *             if the requested key does not exist
     * @throws KeyChangedException
     *             if the key did not match <tt>old_value</tt>
     * @throws UnknownException
     *             if any other error occurs
     *
     * @see #testAndSet(OtpErlangString, OtpErlangObject, OtpErlangObject)
     * @since 3.9
     */
    public <OldT, NewT> void testAndSet(final String key, final OldT oldValue,
            final NewT newValue) throws ConnectionException, TimeoutException,
            NotFoundException, KeyChangedException, UnknownException {
        testAndSet(new OtpErlangString(key),
                ErlangValue.convertToErlang(oldValue),
                ErlangValue.convertToErlang(newValue));
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
