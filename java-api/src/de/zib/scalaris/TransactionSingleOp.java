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
 * @version 3.9
 * @since 2.0
 */
public class TransactionSingleOp {
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
    public TransactionSingleOp() throws ConnectionException {
        connection = ConnectionFactory.getInstance().createConnection();
    }

    /**
     * Constructor, uses the given connection to an erlang node.
     *
     * @param conn
     *            connection to use for the transaction
     */
    public TransactionSingleOp(final Connection conn) {
        connection = conn;
    }

    /**
     * Encapsulates requests that can be used for transactions in
     * {@link TransactionSingleOp#req_list(RequestList)}.
     *
     * @author Nico Kruber, kruber@zib.de
     * @version 3.5
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
         * Copy constructor.
         *
         * @param other the request list to copy from
         */
        public RequestList(final RequestList other) {
            super(other);
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
         * @throws AbortException
         *             if the commit of the write failed
         * @throws UnknownException
         *             if any other error occurs
         */
        public void processWriteAt(final int pos) throws TimeoutException,
                AbortException, UnknownException {
            CommonErlangObjects.checkResult_failAbort(results.elementAt(pos));
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
         * @throws AbortException
         *             if the commit of the write failed
         * @throws UnknownException
         *             if any other error occurs
         *
         * @since 3.9
         */
        public void processAddDelOnList(final int pos) throws TimeoutException,
                NotAListException, AbortException, UnknownException {
            CommonErlangObjects.checkResult_failAbort(results.elementAt(pos));
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
         * @throws AbortException
         *             if the commit of the write failed
         * @throws UnknownException
         *             if any other error occurs
         *
         * @since 3.9
         */
        public void processAddOnNrAt(final int pos) throws TimeoutException,
                NotANumberException, AbortException, UnknownException {
            CommonErlangObjects.checkResult_failAbort(results.elementAt(pos));
            CommonErlangObjects.processResult_addOnNr(results.elementAt(pos));
        }

        /**
         * Processes the result at the given position which originated from
         * a test_and_set request.
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
         * @throws AbortException
         *             if the commit of the write failed
         * @throws UnknownException
         *             if any other error occurs
         *
         * @since 3.8
         */
        public void processTestAndSetAt(final int pos) throws TimeoutException,
                NotFoundException, KeyChangedException, AbortException,
                UnknownException {
            CommonErlangObjects.checkResult_failAbort(results.elementAt(pos));
            CommonErlangObjects.processResult_testAndSet(results.elementAt(pos));
        }
    }

    /**
     * Executes all requests in <code>req</code> and commits each one of them
     * in a single transaction.
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
    public ResultList req_list(final RequestList req)
            throws ConnectionException, UnknownException {
        if (req.isEmpty()) {
            return new ResultList(new OtpErlangList());
        }
        final OtpErlangObject received_raw = connection.doRPC("api_tx", "req_list_commit_each",
                    new OtpErlangObject[] { req.getErlangReqList() });
        try {
            /*
             * possible return values:
             *  [api_tx:result()]
             */
            return new ResultList((OtpErlangList) received_raw);
        } catch (final ClassCastException e) {
            // e.printStackTrace();
            throw new UnknownException(e, received_raw);
        }
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
     * @since 2.9
     */
    public ErlangValue read(final OtpErlangString key)
            throws ConnectionException, TimeoutException, NotFoundException,
            UnknownException {
        final OtpErlangObject received_raw = connection.doRPC("api_tx", "read",
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
    public ErlangValue read(final String key) throws ConnectionException,
            TimeoutException, NotFoundException, UnknownException {
        return read(new OtpErlangString(key));
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
     * @since 2.9
     */
    public void write(final OtpErlangString key, final OtpErlangObject value)
            throws ConnectionException, TimeoutException, AbortException, UnknownException {
        final OtpErlangObject received_raw = connection.doRPC("api_tx", "write",
                new OtpErlangObject[] { key, value });
        CommonErlangObjects.checkResult_failAbort(received_raw);
        CommonErlangObjects.processResult_write(received_raw);
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
    public <T> void write(final String key, final T value) throws ConnectionException,
            TimeoutException, AbortException, UnknownException {
        write(new OtpErlangString(key), ErlangValue.convertToErlang(value));
    }

    /**
     * Changes the list stored at the given key, i.e. first adds all items in
     * <tt>toAdd</tt> then removes all items in <tt>toRemove</tt>.
     * Assumes an empty list if no value exists at <tt>key</tt>.
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
     * @throws AbortException
     *             if the commit of the write failed
     * @throws UnknownException
     *             if any other error occurs
     *
     * @see #addDelOnList(OtpErlangObject, OtpErlangList, OtpErlangList)
     * @since 3.9
     */
    public void addDelOnList(final OtpErlangObject key,
            final OtpErlangList toAdd, final OtpErlangList toRemove)
            throws ConnectionException, TimeoutException, NotAListException,
            AbortException, UnknownException {
        final OtpErlangObject received_raw = connection.doRPC("api_tx", "add_del_on_list",
                new OtpErlangObject[] { key, toAdd, toRemove });
        CommonErlangObjects.checkResult_failAbort(received_raw);
        CommonErlangObjects.processResult_addDelOnList(received_raw);
    }

    /**
     * Changes the list stored at the given key, i.e. first adds all items in
     * <tt>toAdd</tt> then removes all items in <tt>toRemove</tt>.
     * Assumes an empty list if no value exists at <tt>key</tt>.
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
     * @throws AbortException
     *             if the commit of the write failed
     * @throws UnknownException
     *             if any other error occurs
     *
     * @see #addDelOnList(OtpErlangObject, OtpErlangList, OtpErlangList)
     * @since 3.9
     */
    public <T> void addDelOnList(final String key, final List<T> toAdd,
            final List<T> toRemove) throws ConnectionException,
            TimeoutException, NotAListException, AbortException,
            UnknownException {
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
     * @throws AbortException
     *             if the commit of the write failed
     * @throws UnknownException
     *             if any other error occurs
     *
     * @see #addOnNr_(OtpErlangObject, OtpErlangObject)
     * @since 3.9
     */
    public void addOnNr(final OtpErlangObject key, final OtpErlangLong toAdd)
            throws ConnectionException, TimeoutException, NotANumberException,
            AbortException, UnknownException {
        addOnNr_(key, toAdd);
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
     * @throws AbortException
     *             if the commit of the write failed
     * @throws UnknownException
     *             if any other error occurs
     *
     * @see #addOnNr_(OtpErlangObject, OtpErlangObject)
     * @since 3.9
     */
    public void addOnNr(final OtpErlangObject key, final OtpErlangDouble toAdd)
            throws ConnectionException, TimeoutException, NotANumberException,
            AbortException, UnknownException {
        addOnNr_(key, toAdd);
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
     * @throws AbortException
     *             if the commit of the write failed
     * @throws UnknownException
     *             if any other error occurs
     *
     * @see #addOnNr(OtpErlangObject, OtpErlangLong)
     * @see #addOnNr(OtpErlangObject, OtpErlangDouble)
     * @since 3.9
     */
    protected void addOnNr_(final OtpErlangObject key,
            final OtpErlangObject toAdd) throws ConnectionException,
            TimeoutException, NotANumberException, AbortException,
            UnknownException {
        final OtpErlangObject received_raw = connection.doRPC("api_tx", "add_on_nr",
                new OtpErlangObject[] { key, toAdd });
        CommonErlangObjects.checkResult_failAbort(received_raw);
        CommonErlangObjects.processResult_addOnNr(received_raw);
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
     * @throws AbortException
     *             if the commit of the write failed
     * @throws UnknownException
     *             if any other error occurs
     *
     * @see #addOnNr(OtpErlangObject, OtpErlangLong)
     * @see #addOnNr(OtpErlangObject, OtpErlangDouble)
     * @since 3.9
     */
    public <T> void addOnNr(final String key, final T toAdd)
            throws ConnectionException, TimeoutException, NotANumberException,
            AbortException, UnknownException {
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
     * Stores the given <tt>key</tt>/<tt>newValue</tt> pair if the old value
     * at <tt>key</tt> is <tt>oldValue</tt> (atomic test_and_set).
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
     *             if a timeout occurred while trying to fetch/write the value
     * @throws AbortException
     *             if the commit of the write failed
     * @throws NotFoundException
     *             if the requested key does not exist
     * @throws KeyChangedException
     *             if the key did not match <tt>old_value</tt>
     * @throws UnknownException
     *             if any other error occurs
     *
     * @since 2.9
     */
    public void testAndSet(final OtpErlangString key,
            final OtpErlangObject oldValue, final OtpErlangObject newValue)
            throws ConnectionException, TimeoutException, AbortException,
            NotFoundException, KeyChangedException, UnknownException {
        final OtpErlangObject received_raw = connection.doRPC("api_tx", "test_and_set",
                new OtpErlangObject[] { key, oldValue, newValue });
        CommonErlangObjects.checkResult_failAbort(received_raw);
        CommonErlangObjects.processResult_testAndSet(received_raw);
    }

    /**
     * Stores the given <tt>key</tt>/<tt>newValue</tt> pair if the old value
     * at <tt>key</tt> is <tt>oldValue</tt> (atomic test_and_set).
     *
     * @param <OldT>
     *            the type of the stored (old) value.
     *            See {@link ErlangValue} for a list of supported types.
     * @param <NewT>
     *            the type of the (new) value to store.
     *            See {@link ErlangValue} for a list of supported types.
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
    public <OldT, NewT> void testAndSet(final String key, final OldT oldValue,
            final NewT newValue) throws ConnectionException, TimeoutException,
            AbortException, NotFoundException, KeyChangedException,
            UnknownException {
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
