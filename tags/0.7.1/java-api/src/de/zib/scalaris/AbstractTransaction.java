/**
 *  Copyright 2012 Zuse Institute Berlin
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

import de.zib.scalaris.operations.AddDelOnListOp;
import de.zib.scalaris.operations.AddOnNrOp;
import de.zib.scalaris.operations.Operation;
import de.zib.scalaris.operations.ReadOp;
import de.zib.scalaris.operations.TestAndSetOp;
import de.zib.scalaris.operations.WriteOp;

/**
 * Generic base class for {@link Transaction} and {@link TransactionSingleOp}.
 *
 * @param <ReqL> {@link RequestList} type
 * @param <ResL> {@link ResultList} type
 *
 * @author Nico Kruber, kruber@zib.de
 * @version 3.18
 * @since 3.14
 */
public abstract class AbstractTransaction<ReqL extends RequestList, ResL extends ResultList> {

    /**
     * Connection to a Scalaris node.
     */
    protected final Connection connection;

    /**
     * Whether to compress the transfer of values.
     *
     * @since 3.14
     */
    protected boolean compressed = true;

    /**
     * Constructor, uses the default connection returned by
     * {@link ConnectionFactory#createConnection()}.
     *
     * @throws ConnectionException
     *             if the connection fails
     */
    public AbstractTransaction() throws ConnectionException {
        super();
        connection = ConnectionFactory.getInstance().createConnection();
    }

    /**
     * Constructor, uses the given connection to an erlang node.
     *
     * @param conn
     *            connection to use for the transaction
     */
    public AbstractTransaction(final Connection conn) {
        connection = conn;
    }

    abstract protected ReqL newReqList();

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
    protected ResL req_list(final Operation op) throws ConnectionException,
            AbortException, UnknownException {
        final ReqL reqList = newReqList();
        reqList.addOp(op);
        return req_list(reqList);
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
     *             if a commit failed (if there was one)
     * @throws UnknownException
     *             if any other error occurs
     */
    abstract public ResL req_list(final ReqL req) throws ConnectionException,
            AbortException, UnknownException;

    /**
     * Selects the module to use depending in the {@link #compressed} property.
     *
     * @return a module name
     */
    protected String module() {
        return compressed ? "api_txc" : "api_tx";
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
     * @throws NotFoundException
     *             if the requested key does not exist
     * @throws UnknownException
     *             if any other error occurs
     *
     * @since 2.9
     */
    public ErlangValue read(final OtpErlangString key)
            throws ConnectionException, NotFoundException, UnknownException {
        try {
            final ResL result = req_list(new ReadOp(key));
            return result.processReadAt(0);
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
     * @throws NotFoundException
     *             if the requested key does not exist
     * @throws UnknownException
     *             if any other error occurs
     *
     * @see #read(OtpErlangString)
     * @since 2.9
     */
    public ErlangValue read(final String key) throws ConnectionException,
            NotFoundException, UnknownException {
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
     * @throws AbortException
     *             if the commit failed (if there was one)
     * @throws UnknownException
     *             if any other error occurs
     *
     * @since 2.9
     */
    public void write(final OtpErlangString key, final OtpErlangObject value)
            throws ConnectionException, AbortException, UnknownException {
        final ResL result = req_list(new WriteOp(key, value));
        result.processWriteAt(0);
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
     * @throws AbortException
     *             if the commit failed (if there was one)
     * @throws UnknownException
     *             if any other error occurs
     *
     * @see #write(OtpErlangString, OtpErlangObject)
     * @since 2.9
     */
    public <T> void write(final String key, final T value)
            throws ConnectionException, AbortException, UnknownException {
        write(new OtpErlangString(key), ErlangValue.convertToErlang(value));
    }

    /**
     * Changes the list stored at the given key, i.e. first adds all items in
     * <tt>toAdd</tt> then removes all items in <tt>toRemove</tt>. Assumes en
     * empty list if no value exists at <tt>key</tt>.
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
     * @throws NotAListException
     *             if the previously stored value was no list
     * @throws AbortException
     *             if the commit failed (if there was one)
     * @throws UnknownException
     *             if any other error occurs
     *
     * @since 3.9
     */
    public void addDelOnList(final OtpErlangString key,
            final OtpErlangList toAdd, final OtpErlangList toRemove)
            throws ConnectionException, NotAListException, AbortException,
            UnknownException {
        final ResL result = req_list(new AddDelOnListOp(key, toAdd, toRemove));
        result.processAddDelOnListAt(0);
    }

    /**
     * Changes the list stored at the given key, i.e. first adds all items in
     * <tt>toAdd</tt> then removes all items in <tt>toRemove</tt>. Assumes en
     * empty list if no value exists at <tt>key</tt>.
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
     * @throws NotAListException
     *             if the previously stored value was no list
     * @throws AbortException
     *             if the commit failed (if there was one)
     * @throws UnknownException
     *             if any other error occurs
     *
     * @see #addDelOnList(OtpErlangString, OtpErlangList, OtpErlangList)
     * @since 3.9
     */
    public <T> void addDelOnList(final String key, final List<T> toAdd,
            final List<T> toRemove) throws ConnectionException,
            NotAListException, AbortException, UnknownException {
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
     * Changes the number stored at the given key, i.e. adds some value. Assumes
     * <tt>0</tt> if no value exists at <tt>key</tt>.
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
     * @throws NotANumberException
     *             if the previously stored value was no number
     * @throws AbortException
     *             if the commit failed (if there was one)
     * @throws UnknownException
     *             if any other error occurs
     *
     * @see #addOnNr_(AddOnNrOp)
     * @since 3.9
     */
    public void addOnNr(final OtpErlangString key, final OtpErlangLong toAdd)
            throws ConnectionException, NotANumberException, AbortException,
            UnknownException {
        addOnNr_(new AddOnNrOp(key, toAdd));
    }

    /**
     * Changes the number stored at the given key, i.e. adds some value. Assumes
     * <tt>0</tt> if no value exists at <tt>key</tt>.
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
     * @throws NotANumberException
     *             if the previously stored value was no number
     * @throws AbortException
     *             if the commit failed (if there was one)
     * @throws UnknownException
     *             if any other error occurs
     *
     * @see #addOnNr_(AddOnNrOp)
     * @since 3.9
     */
    public void addOnNr(final OtpErlangString key, final OtpErlangDouble toAdd)
            throws ConnectionException, NotANumberException, AbortException,
            UnknownException {
        addOnNr_(new AddOnNrOp(key, toAdd));
    }

    /**
     * Changes the number stored at the given key, i.e. adds some value. Assumes
     * <tt>0</tt> if no value exists at <tt>key</tt>.
     *
     * @param op
     *            the add_on_nr operation
     *
     * @throws ConnectionException
     *             if the connection is not active or a communication error
     *             occurs or an exit signal was received or the remote node
     *             sends a message containing an invalid cookie
     * @throws NotANumberException
     *             if the previously stored value was no number
     * @throws AbortException
     *             if the commit failed (if there was one)
     * @throws UnknownException
     *             if any other error occurs
     *
     * @see #addOnNr(OtpErlangString, OtpErlangLong)
     * @see #addOnNr(OtpErlangString, OtpErlangDouble)
     * @since 3.9
     */
    protected void addOnNr_(final AddOnNrOp op) throws ConnectionException,
            NotANumberException, AbortException, UnknownException {
        final ResL result = req_list(op);
        result.processAddOnNrAt(0);
    }

    /**
     * Changes the number stored at the given key, i.e. adds some value. Assumes
     * <tt>0</tt> if no value exists at <tt>key</tt>.
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
     * @throws NotANumberException
     *             if the previously stored value was no number
     * @throws AbortException
     *             if the commit failed (if there was one)
     * @throws UnknownException
     *             if any other error occurs
     *
     * @see #addOnNr(OtpErlangString, OtpErlangLong)
     * @see #addOnNr(OtpErlangString, OtpErlangDouble)
     * @since 3.9
     */
    public <T> void addOnNr(final String key, final T toAdd)
            throws ConnectionException, NotANumberException, AbortException,
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
     * Stores the given <tt>key</tt>/<tt>new_value</tt> pair if the old value at
     * <tt>key</tt> is <tt>old_value</tt> (atomic test_and_set).
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
     * @throws NotFoundException
     *             if the requested key does not exist
     * @throws KeyChangedException
     *             if the key did not match <tt>old_value</tt>
     * @throws AbortException
     *             if the commit failed (if there was one)
     * @throws UnknownException
     *             if any other error occurs
     *
     * @since 3.9
     */
    public void testAndSet(final OtpErlangString key,
            final OtpErlangObject oldValue, final OtpErlangObject newValue)
            throws ConnectionException, NotFoundException, KeyChangedException,
            AbortException, UnknownException {
        final ResL result = req_list(new TestAndSetOp(key, oldValue, newValue));
        result.processTestAndSetAt(0);
    }

    /**
     * Stores the given <tt>key</tt>/<tt>new_value</tt> pair if the old value at
     * <tt>key</tt> is <tt>old_value</tt> (atomic test_and_set).
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
     * @throws NotFoundException
     *             if the requested key does not exist
     * @throws KeyChangedException
     *             if the key did not match <tt>old_value</tt>
     * @throws AbortException
     *             if the commit failed (if there was one)
     * @throws UnknownException
     *             if any other error occurs
     *
     * @see #testAndSet(OtpErlangString, OtpErlangObject, OtpErlangObject)
     * @since 3.9
     */
    public <OldT, NewT> void testAndSet(final String key, final OldT oldValue,
            final NewT newValue) throws ConnectionException, NotFoundException,
            KeyChangedException, AbortException, UnknownException {
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

    /**
     * Checks whether the transfer of values is compressed or not.
     *
     * @return <tt>true</tt> if compressed, otherwise <tt>false</tt>
     *
     * @since 3.14
     */
    public boolean isCompressed() {
        return compressed;
    }

    /**
     * Sets whether to compress the transfer of values or not.
     *
     * @param compressed
     *            <tt>true</tt> if compressed, otherwise <tt>false</tt>
     *
     * @since 3.14
     */
    public void setCompressed(final boolean compressed) {
        this.compressed = compressed;
    }

}