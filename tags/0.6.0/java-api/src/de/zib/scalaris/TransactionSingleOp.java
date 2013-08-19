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

import de.zib.scalaris.operations.AddDelOnListOp;
import de.zib.scalaris.operations.AddOnNrOp;
import de.zib.scalaris.operations.Operation;
import de.zib.scalaris.operations.TestAndSetOp;
import de.zib.scalaris.operations.TransactionSingleOpOperation;
import de.zib.scalaris.operations.WriteOp;

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
 * @version 3.18
 * @since 2.0
 */
public class TransactionSingleOp extends
        AbstractTransaction<de.zib.scalaris.TransactionSingleOp.RequestList, de.zib.scalaris.TransactionSingleOp.ResultList> {

    /**
     * Constructor, uses the default connection returned by
     * {@link ConnectionFactory#createConnection()}.
     *
     * @throws ConnectionException
     *             if the connection fails
     */
    public TransactionSingleOp() throws ConnectionException {
        super();
    }

    /**
     * Constructor, uses the given connection to an erlang node.
     *
     * @param conn
     *            connection to use for the transaction
     */
    public TransactionSingleOp(final Connection conn) {
        super(conn);
    }

    /**
     * Encapsulates requests that can be used for transactions in
     * {@link TransactionSingleOp#req_list(RequestList)}.
     *
     * @author Nico Kruber, kruber@zib.de
     * @version 3.18
     * @since 3.5
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
        protected RequestList(final TransactionSingleOpOperation op) {
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
            if (!(op instanceof TransactionSingleOpOperation)) {
                throw new UnsupportedOperationException();
            }
            return (RequestList) super.addOp(op);
        }

        /**
         * Throws an {@link UnsupportedOperationException} as a commit is not
         * supported here.
         *
         * @return this {@link RequestList} object
         *
         * @throws UnsupportedOperationException
         *             always thrown in this class
         */
        @Override
        public RequestList addCommit() {
            throw new UnsupportedOperationException();
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
     * {@link TransactionSingleOp#req_list(RequestList)}.
     *
     * @author Nico Kruber, kruber@zib.de
     * @version 3.8
     * @since 3.5
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
         * @throws AbortException
         *             if the commit of the write failed
         * @throws UnknownException
         *             if any other error occurs
         */
        @Override
        public void processWriteAt(final int pos) throws AbortException,
                UnknownException {
            ((WriteOp) get(pos)).processResultSingle();
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
         * @throws AbortException
         *             if the commit of the write failed
         * @throws UnknownException
         *             if any other error occurs
         *
         * @since 3.9
         */
        @Override
        public void processAddDelOnListAt(final int pos)
                throws NotAListException, AbortException, UnknownException {
            ((AddDelOnListOp) get(pos)).processResultSingle();
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
         * @throws AbortException
         *             if the commit of the write failed
         * @throws UnknownException
         *             if any other error occurs
         *
         * @since 3.9
         */
        @Override
        public void processAddOnNrAt(final int pos) throws NotANumberException,
                AbortException, UnknownException {
            ((AddOnNrOp) get(pos)).processResultSingle();
        }

        /**
         * Processes the result at the given position which originated from
         * a test_and_set request.
         *
         * @param pos
         *            the position in the result list (starting at 0)
         *
         * @throws NotFoundException
         *             if the requested key does not exist
         * @throws KeyChangedException
         *             if the key did not match <tt>old_value</tt>
         * @throws AbortException
         *             if the commit of the write failed
         * @throws UnknownException
         *             if any other error occurs
         *
         * @since 3.8
         */
        @Override
        public void processTestAndSetAt(final int pos)
                throws NotFoundException, KeyChangedException, AbortException,
                UnknownException {
            ((TestAndSetOp) get(pos)).processResultSingle();
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
     * @throws UnknownException
     *             if any other error occurs
     *
     * @see #req_list(RequestList)
     *
     * @since 3.18
     */
    public ResultList req_list(final TransactionSingleOpOperation op)
            throws ConnectionException, UnknownException {
        try {
            return super.req_list(op);
        } catch (final AbortException e) {
            // should not occur!
            throw new UnknownException(e);
        }
    }

    /**
     * Executes all requests in <code>req</code> and commits each one of them in
     * a single transaction.
     *
     * NOTE: The execution order of multiple requests on the same key is
     * undefined!
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
     * @throws UnknownException
     *             if any other error occurs
     */
    @Override
    public ResultList req_list(final RequestList req)
            throws ConnectionException, UnknownException {
        if (req.isEmpty()) {
            return new ResultList(new OtpErlangList(), compressed, req);
        }
        final OtpErlangObject received_raw = connection.doRPC(module(), "req_list_commit_each",
                    new OtpErlangObject[] { req.getErlangReqList(compressed) });
        try {
            /*
             * possible return values:
             *  [api_tx:result()]
             */
            return new ResultList((OtpErlangList) received_raw, compressed, req);
        } catch (final ClassCastException e) {
            // e.printStackTrace();
            throw new UnknownException(e, received_raw);
        }
    }

    @Override
    protected RequestList newReqList() {
        return new RequestList();
    }
}
