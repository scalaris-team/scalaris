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

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;

import com.ericsson.otp.erlang.OtpErlangDouble;
import com.ericsson.otp.erlang.OtpErlangList;
import com.ericsson.otp.erlang.OtpErlangLong;
import com.ericsson.otp.erlang.OtpErlangObject;
import com.ericsson.otp.erlang.OtpErlangString;
import com.ericsson.otp.erlang.OtpErlangTuple;

import de.zib.scalaris.operations.AddDelOnListOp;
import de.zib.scalaris.operations.AddOnNrOp;
import de.zib.scalaris.operations.CommitOp;
import de.zib.scalaris.operations.Operation;
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
 * @version 3.18
 * @since 2.0
 */
public class Transaction extends
        AbstractTransaction<de.zib.scalaris.Transaction.RequestList, de.zib.scalaris.Transaction.ResultList> {
    /**
     * Erlang transaction log.
     */
    protected final Translog transLog = getTranslogImpl();

    /**
     * Gets the {@link Translog} implementation to use for {@link #transLog}.
     * Will be executed only once during construction of the object!
     *
     * Re-define in sub-classes to use a different implementation.
     *
     * @return a translog object
     */
    protected Translog getTranslogImpl() {
        return new FilteringTransLog();
    }

    /**
     * Constructor, uses the default connection returned by
     * {@link ConnectionFactory#createConnection()}.
     *
     * @throws ConnectionException
     *             if the connection fails
     */
    public Transaction() throws ConnectionException {
        super();
    }

    /**
     * Constructor, uses the given connection to an erlang node.
     *
     * @param conn
     *            connection to use for the transaction
     */
    public Transaction(final Connection conn) {
        super(conn);
    }

    /**
     * Encapsulates requests that can be used for transactions in
     * {@link Transaction#req_list(RequestList)}.
     *
     * @author Nico Kruber, kruber@zib.de
     * @version 3.18
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
         * Creates a new request list with the given operation.
         *
         * Provided for convenience.
         *
         * @since 3.18
         */
        protected RequestList(final TransactionOperation op) {
            super(op);
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
         * @param results
         *            the raw results list as returned by scalaris
         * @param compressed
         *            whether the value part in the term is encoded, i.e.
         *            compressed into an Erlang binary, or not
         * @param requests
         *            request list which created this result list
         */
        ResultList(final OtpErlangList results, final boolean compressed,
                final RequestList requests) {
            super(results, compressed, requests);
        }

        /**
         * Processes the result at the given position which originated from
         * a write request.
         *
         * @param pos
         *            the position in the result list (starting at 0)
         *
         * @throws UnknownException
         *             if any other error occurs
         */
        @Override
        public void processWriteAt(final int pos) throws UnknownException {
            ((WriteOp) get(pos)).processResult();
        }

        /**
         * Processes the result at the given position which originated from
         * a add_del_on_list request.
         *
         * @param pos
         *            the position in the result list (starting at 0)
         *
         * @throws NotAListException
         *             if the previously stored value was no list
         * @throws UnknownException
         *             if any other error occurs
         *
         * @since 3.9
         */
        @Override
        public void processAddDelOnListAt(final int pos)
                throws NotAListException, UnknownException {
            ((AddDelOnListOp) get(pos)).processResult();
        }

        /**
         * Processes the result at the given position which originated from
         * an add_on_nr request.
         *
         * @param pos
         *            the position in the result list (starting at 0)
         *
         * @throws NotANumberException
         *             if the previously stored value was not a number
         * @throws UnknownException
         *             if any other error occurs
         *
         * @since 3.9
         */
        @Override
        public void processAddOnNrAt(final int pos) throws NotANumberException,
                UnknownException {
            ((AddOnNrOp) get(pos)).processResult();
        }

        /**
         * Processes the result at the given position which originated from
         * an add_on_nr request.
         *
         * @param pos
         *            the position in the result list (starting at 0)
         *
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
        public void processTestAndSetAt(final int pos)
                throws NotFoundException, KeyChangedException, UnknownException {
            ((TestAndSetOp) get(pos)).processResult();
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
         * @throws AbortException
         *             if the commit failed
         * @throws UnknownException
         *             if any other error occurs
         */
        public void processCommitAt(final int pos) throws AbortException,
                UnknownException {
            ((CommitOp) get(pos)).processResult();
        }
    }

    /**
     * TransLog abstraction layer, tightly coupled with <tt>tx_tlog</tt>.
     *
     * @author Nico Kruber, kruber@zib.de
     * @version 3.17
     * @since 3.17
     */
    protected static interface Translog {
        /**
         * Merges a new tlog object (from Scalaris) with this translog by adding
         * new entries and replacing existing ones.
         *
         * @param newTLog
         *            new tlog entries from Scalaris
         *
         * @return this object
         */
        public abstract Translog merge(final OtpErlangObject newTLog);

        /**
         * Checks whether the translog is empty.
         *
         * @return <tt>true</tt> if empty
         */
        public abstract boolean isEmpty();

        /**
         * Clears the translog.
         */
        public abstract void reset();

        /**
         * Creates a minimal tlog only containing entries for the keys in the
         * given request list.
         *
         * @param req
         *            the request list
         *
         * @return minimal tlog
         */
        public abstract OtpErlangObject filter(final RequestList req);
    }

    /**
     * TransLog abstraction layer sending the whole tlog for each request,
     * using the Scalaris-provided tlog as is.
     *
     * @author Nico Kruber, kruber@zib.de
     * @version 3.17
     * @since 3.17
     */
    protected static class FullTransLog implements Translog {
        protected OtpErlangObject tlog = null;

        /* (non-Javadoc)
         * @see de.zib.scalaris.Translog#merge(com.ericsson.otp.erlang.OtpErlangObject)
         */
        @Override
        public Translog merge(final OtpErlangObject newTLog) {
            this.tlog = newTLog;
            return this;
        }

        /* (non-Javadoc)
         * @see de.zib.scalaris.Translog#isEmpty()
         */
        @Override
        public boolean isEmpty() {
            return this.tlog == null;
        }

        /* (non-Javadoc)
         * @see de.zib.scalaris.Translog#reset()
         */
        @Override
        public void reset() {
            this.tlog = null;
        }

        /* (non-Javadoc)
         * @see de.zib.scalaris.Translog#filter(de.zib.scalaris.Transaction.RequestList)
         */
        @Override
        public OtpErlangObject filter(final RequestList req) {
            return this.tlog;
        }
    }

    /**
     * TransLog abstraction layer only sending the subset of the tlog that is
     * actually required for a request, tightly coupled with <tt>tx_tlog</tt>.
     *
     * @author Nico Kruber, kruber@zib.de
     * @version 3.17
     * @since 3.17
     */
    protected static class FilteringTransLog implements Translog {
        protected LinkedHashMap<OtpErlangString, OtpErlangTuple> entries = new LinkedHashMap<OtpErlangString, OtpErlangTuple>();

        /* (non-Javadoc)
         * @see de.zib.scalaris.Translog#merge(com.ericsson.otp.erlang.OtpErlangObject)
         */
        @Override
        public Translog merge(final OtpErlangObject newTLog) {
            try {
                final OtpErlangList newTLogL = (OtpErlangList) newTLog;
                for (int i = 0; i < newTLogL.arity(); ++i) {
                    final OtpErlangTuple entry = (OtpErlangTuple) newTLogL.elementAt(i);
                    final OtpErlangString key = ErlangValue.otpObjectToOtpString(entry.elementAt(1));
                    entries.put(key, entry);
                }
            } catch (final ClassCastException e) {
                throw new UnknownException(newTLog);
            }
            return this;
        }

        /* (non-Javadoc)
         * @see de.zib.scalaris.Translog#isEmpty()
         */
        @Override
        public boolean isEmpty() {
            return entries.isEmpty();
        }

        /* (non-Javadoc)
         * @see de.zib.scalaris.Translog#reset()
         */
        @Override
        public void reset() {
            entries.clear();
        }

        /* (non-Javadoc)
         * @see de.zib.scalaris.Translog#filter(de.zib.scalaris.Transaction.RequestList)
         */
        @Override
        public OtpErlangObject filter(final RequestList req) {
            OtpErlangList result;
            if (req.isCommit()) {
                result = new OtpErlangList(entries.values()
                        .toArray(new OtpErlangTuple[entries.size()]));
            } else {
                final HashSet<OtpErlangTuple> resultJ = new HashSet<OtpErlangTuple>(
                        req.size());
                for (final Operation op : req.getRequests()) {
                    final OtpErlangTuple entry = entries.get(op.getKey());
                    if (entry != null) {
                        resultJ.add(entry);
                    }
                }
                result = new OtpErlangList(
                        resultJ.toArray(new OtpErlangTuple[resultJ.size()]));
            }
            return result;
        }
    }

    /**
     * Executes the given operation.
     *
     * @param op
     *            the operation to execute
     *
     * @return results list containing a single result of the given operation
     *
     * @throws ConnectionException
     *             if the connection is not active or a communication error
     *             occurs or an exit signal was received or the remote node
     *             sends a message containing an invalid cookie
     * @throws AbortException
     *             if a commit failed (if there was one)
     * @throws UnknownException
     *             if any other error occurs
     *
     * @see #req_list(RequestList)
     *
     * @since 3.18
     */
    public ResultList req_list(final TransactionOperation op)
            throws ConnectionException, AbortException, UnknownException {
        return super.req_list(op);
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
     * @throws AbortException
     *             if the commit failed
     * @throws UnknownException
     *             if any other error occurs
     */
    @Override
    public ResultList req_list(final RequestList req)
            throws ConnectionException, AbortException, UnknownException {
        if (req.isEmpty()) {
            return new ResultList(new OtpErlangList(), compressed, req);
        }
        OtpErlangObject received_raw = null;
        final OtpErlangList erlangReqList = req.getErlangReqList(compressed);
        if (transLog.isEmpty()) {
            received_raw = connection.doRPC(module(), "req_list",
                    new OtpErlangObject[] { erlangReqList });
        } else {
            received_raw = connection.doRPC(module(), "req_list",
                    new OtpErlangObject[] { transLog.filter(req), erlangReqList });
        }
        try {
            /*
             * possible return values:
             *  {tx_tlog:tlog(), [{ok} | {ok, Value} | {fail, abort | timeout | not_found}]}
             */
            final OtpErlangTuple received = (OtpErlangTuple) received_raw;
            transLog.merge(received.elementAt(0));
            if (received.arity() == 2) {
                final ResultList result = new ResultList((OtpErlangList) received.elementAt(1), compressed, req);
                if (req.isCommit()) {
                    req.getCommit().processResult();
                    // transaction was successful: reset transaction log
                    transLog.reset();
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
    public void commit() throws ConnectionException, AbortException, UnknownException {
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
        transLog.reset();
    }

    @Override
    protected RequestList newReqList() {
        return new RequestList();
    }

    // some overrides (we do not commit transactions in these methods and thus
    // do not throw an AbortException):

    @Override
    public void write(final OtpErlangString key, final OtpErlangObject value)
            throws ConnectionException, UnknownException {
        try {
            super.write(key, value);
        } catch (final AbortException e) {
            // should not occur (we did not commit anything)
            throw new UnknownException(e);
        }
    }

    @Override
    public <T> void write(final String key, final T value)
            throws ConnectionException, UnknownException {
        try {
            super.write(key, value);
        } catch (final AbortException e) {
            // should not occur (we did not commit anything)
            throw new UnknownException(e);
        }
    }

    @Override
    public void addDelOnList(final OtpErlangString key,
            final OtpErlangList toAdd, final OtpErlangList toRemove)
            throws ConnectionException, NotAListException, UnknownException {
        try {
            super.addDelOnList(key, toAdd, toRemove);
        } catch (final AbortException e) {
            // should not occur (we did not commit anything)
            throw new UnknownException(e);
        }
    }

    @Override
    public <T> void addDelOnList(final String key, final List<T> toAdd,
            final List<T> toRemove) throws ConnectionException,
            NotAListException, UnknownException {
        try {
            super.addDelOnList(key, toAdd, toRemove);
        } catch (final AbortException e) {
            // should not occur (we did not commit anything)
            throw new UnknownException(e);
        }
    }

    @Override
    public void addOnNr(final OtpErlangString key, final OtpErlangLong toAdd)
            throws ConnectionException, NotANumberException, UnknownException {
        try {
            super.addOnNr(key, toAdd);
        } catch (final AbortException e) {
            // should not occur (we did not commit anything)
            throw new UnknownException(e);
        }
    }

    @Override
    public void addOnNr(final OtpErlangString key, final OtpErlangDouble toAdd)
            throws ConnectionException, NotANumberException, UnknownException {
        try {
            super.addOnNr(key, toAdd);
        } catch (final AbortException e) {
            // should not occur (we did not commit anything)
            throw new UnknownException(e);
        }
    }

    @Override
    public <T> void addOnNr(final String key, final T toAdd)
            throws ConnectionException, NotANumberException, UnknownException {
        try {
            super.addOnNr(key, toAdd);
        } catch (final AbortException e) {
            // should not occur (we did not commit anything)
            throw new UnknownException(e);
        }
    }

    @Override
    public void testAndSet(final OtpErlangString key,
            final OtpErlangObject oldValue, final OtpErlangObject newValue)
            throws ConnectionException, NotFoundException, KeyChangedException,
            UnknownException {
        try {
            super.testAndSet(key, oldValue, newValue);
        } catch (final AbortException e) {
            // should not occur (we did not commit anything)
            throw new UnknownException(e);
        }
    }

    @Override
    public <OldT, NewT> void testAndSet(final String key, final OldT oldValue,
            final NewT newValue) throws ConnectionException, NotFoundException,
            KeyChangedException, UnknownException {
        try {
            super.testAndSet(key, oldValue, newValue);
        } catch (final AbortException e) {
            // should not occur (we did not commit anything)
            throw new UnknownException(e);
        }
    }
}
