/**
 *  Copyright 2011 Zuse Institute Berlin
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

import java.math.BigInteger;
import java.util.ArrayList;
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
import de.zib.scalaris.operations.TransactionOperation;
import de.zib.scalaris.operations.WriteOp;

/**
 * Generic request list.
 *
 * @author Nico Kruber, kruber@zib.de
 * @version 3.14
 * @since 3.5
 */
public abstract class RequestList {
    protected final List<Operation> requests = new ArrayList<Operation>(10);
    private boolean isCommit = false;

    protected static class CommitOp implements TransactionOperation {
        public CommitOp() {
        }

        public OtpErlangObject getErlang() {
            return CommonErlangObjects.commitTupleAtom;
        }
        public OtpErlangString getKey() {
            return null;
        }
    }

    /**
     * Default constructor.
     */
    protected RequestList() {
    }

    /**
     * Copy constructor.
     *
     * @param other the request list to copy from
     */
    protected RequestList(final RequestList other) {
        addAll_(other);
    }

    /**
     * Adds a generic operation to the list of requests.
     *
     * @param op
     *            the operation to add
     *
     * @return this {@link RequestList} object
     *
     * @throws UnsupportedOperationException
     *             if the operation is unsupported, e.g. there may only be one
     *             "commit" in a request list and no request after that
     */
    public RequestList addOp(final Operation op) throws UnsupportedOperationException {
        if (isCommit) {
            throw new UnsupportedOperationException(
                    "No further request supported after a commit!");
        }
        requests.add(op);
        return this;
    }

    /**
     * Adds a read operation to the list of requests.
     *
     * @param key
     *            the key to read
     *
     * @return this {@link RequestList} object
     *
     * @throws UnsupportedOperationException
     *             if the operation is unsupported, e.g. there may only be one
     *             "commit" in a request list and no request after that
     */
    @Deprecated
    public RequestList addRead(final OtpErlangString key)
            throws UnsupportedOperationException {
        return addOp(new ReadOp(key));
    }

    /**
     * Adds a read operation to the list of requests.
     *
     * @param key
     *            the key to read
     *
     * @return this {@link RequestList} object
     *
     * @throws UnsupportedOperationException
     *             if the operation is unsupported, e.g. there may only be one
     *             "commit" in a request list and no request after that
     */
    @Deprecated
    public RequestList addRead(final String key)
            throws UnsupportedOperationException {
        return addOp(new ReadOp(key));
    }

    /**
     * Adds a write operation to the list of requests.
     *
     * @param key
     *            the key to write the value to
     * @param value
     *            the value to write
     *
     * @return this {@link RequestList} object
     *
     * @throws UnsupportedOperationException
     *             if the operation is unsupported, e.g. there may only be one
     *             "commit" in a request list and no request after that
     */
    @Deprecated
    public RequestList addWrite(final OtpErlangString key, final OtpErlangObject value)
            throws UnsupportedOperationException {
        return addOp(new WriteOp(key, value));
    }

    /**
     * Adds a write operation to the list of requests.
     *
     * @param <T>
     *            type of the value to write
     * @param key
     *            the key to write the value to
     * @param value
     *            the value to write
     *
     * @return this {@link RequestList} object
     *
     * @throws UnsupportedOperationException
     *             if the operation is unsupported, e.g. there may only be one
     *             "commit" in a request list and no request after that
     */
    @Deprecated
    public <T> RequestList addWrite(final String key, final T value)
            throws UnsupportedOperationException {
        return addOp(new WriteOp(key, value));
    }

    /**
     * Adds a add_del_on_list operation to the list of requests.
     *
     * @param key
     *            the key to write the value to
     * @param toAdd
     *            a list of values to add to a list
     * @param toRemove
     *            a list of values to remove from a list
     *
     * @return this {@link RequestList} object
     *
     * @throws UnsupportedOperationException
     *             if the operation is unsupported, e.g. there may only be one
     *             "commit" in a request list and no request after that
     *
     * @since 3.9
     */
    @Deprecated
    public RequestList addAddDelOnList(final OtpErlangString key, final OtpErlangList toAdd, final OtpErlangList toRemove)
            throws UnsupportedOperationException {
        return addOp(new AddDelOnListOp(key, toAdd, toRemove));
    }

    /**
     * Adds a add_del_on_list operation to the list of requests.
     *
     * @param <T>
     *            type of the value to write
     * @param key
     *            the key to write the value to
     * @param toAdd
     *            a list of values to add to a list
     * @param toRemove
     *            a list of values to remove from a list
     *
     * @return this {@link RequestList} object
     *
     * @throws UnsupportedOperationException
     *             if the operation is unsupported, e.g. there may only be one
     *             "commit" in a request list and no request after that
     *
     * @since 3.9
     */
    @Deprecated
    public <T> RequestList addAddDelOnList(final String key, final List<T> toAdd, final List<T> toRemove)
            throws UnsupportedOperationException {
        return addOp(new AddDelOnListOp(key, toAdd, toRemove));
    }

    /**
     * Adds an add_on_nr operation to the list of requests.
     *
     * @param key
     *            the key to write the value to
     * @param toAdd
     *            the number to add to the number stored at key
     *
     * @return this {@link RequestList} object
     *
     * @throws UnsupportedOperationException
     *             if the operation is unsupported, e.g. there may only be one
     *             "commit" in a request list and no request after that
     *
     * @since 3.9
     */
    @Deprecated
    public RequestList addAddOnNr(final OtpErlangString key, final OtpErlangLong toAdd)
            throws UnsupportedOperationException {
        return addOp(new AddOnNrOp(key, toAdd));
    }

    /**
     * Adds an add_on_nr operation to the list of requests.
     *
     * @param key
     *            the key to write the value to
     * @param toAdd
     *            the number to add to the number stored at key
     *
     * @return this {@link RequestList} object
     *
     * @throws UnsupportedOperationException
     *             if the operation is unsupported, e.g. there may only be one
     *             "commit" in a request list and no request after that
     *
     * @since 3.9
     */
    @Deprecated
    public RequestList addAddOnNr(final OtpErlangString key, final OtpErlangDouble toAdd)
            throws UnsupportedOperationException {
        return addOp(new AddOnNrOp(key, toAdd));
    }

    /**
     * Adds an add_on_nr operation to the list of requests.
     *
     * @param <T>
     *            type of the value to write; WARNING: the actual supported
     *            types only include {@link Integer}, {@link Long},
     *            {@link BigInteger} and {@link Double} - see
     *            {@link ErlangValue#convertToErlang(Object)}.
     * @param key
     *            the key to write the value to
     * @param toAdd
     *            the number to add to the number stored at key
     *
     * @return this {@link RequestList} object
     *
     * @throws UnsupportedOperationException
     *             if the operation is unsupported, e.g. there may only be one
     *             "commit" in a request list and no request after that
     *
     * @since 3.9
     */
    @Deprecated
    public <T extends Number> RequestList addAddOnNr(final String key, final T toAdd)
            throws UnsupportedOperationException {
        return addOp(new AddOnNrOp(key, toAdd));
    }

    /**
     * Adds an add_on_nr operation to the list of requests.
     *
     * @param key
     *            the key to write the value to
     * @param toAdd
     *            the number to add to the number stored at key
     *
     * @return this {@link RequestList} object
     *
     * @throws UnsupportedOperationException
     *             if the operation is unsupported, e.g. there may only be one
     *             "commit" in a request list and no request after that
     *
     * @since 3.9
     */
    @Deprecated
    public RequestList addAddOnNr(final String key, final Double toAdd)
            throws UnsupportedOperationException {
        return addOp(new AddOnNrOp(key, toAdd));
    }

    /**
     * Adds a test_and_set operation to the list of requests (<tt>newValue</tt>
     * is only written if the currently stored value is <tt>oldValue</tt>).
     *
     * @param key
     *            the key to write the value to
     * @param oldValue
     *            the old value to verify
     * @param newValue
     *            the new value to write of oldValue is correct
     *
     * @return this {@link RequestList} object
     *
     * @throws UnsupportedOperationException
     *             if the operation is unsupported, e.g. there may only be one
     *             "commit" in a request list and no request after that
     *
     * @since 3.8
     */
    @Deprecated
    public RequestList addTestAndSet(final OtpErlangString key, final OtpErlangObject oldValue, final OtpErlangObject newValue)
            throws UnsupportedOperationException {
        return addOp(new TestAndSetOp(key, oldValue, newValue));
    }

    /**
     * Adds a test_and_set operation to the list of requests (<tt>newValue</tt>
     * is only written if the currently stored value is <tt>oldValue</tt>).
     *
     * @param <OldT>
     *            the type of the stored (old) value. See {@link ErlangValue}
     *            for a list of supported types.
     * @param <NewT>
     *            the type of the (new) value to store. See {@link ErlangValue}
     *            for a list of supported types.
     * @param key
     *            the key to write the value to
     * @param oldValue
     *            the old value to verify
     * @param newValue
     *            the new value to write of oldValue is correct
     *
     * @return this {@link RequestList} object
     *
     * @throws UnsupportedOperationException
     *             if the operation is unsupported, e.g. there may only be one
     *             "commit" in a request list and no request after that
     *
     * @since 3.8
     */
    @Deprecated
    public <OldT, NewT> RequestList addTestAndSet(final String key, final OldT oldValue, final NewT newValue)
            throws UnsupportedOperationException {
        return addOp(new TestAndSetOp(key, oldValue, newValue));
    }

    /**
     * Adds a commit operation to the list of requests.
     *
     * @return this {@link RequestList} object
     *
     * @throws UnsupportedOperationException
     *             if the operation is unsupported, e.g. there may only be one
     *             "commit" in a request list and no request after that
     */
    public RequestList addCommit() throws UnsupportedOperationException {
        addOp(new CommitOp());
        isCommit = true;
        return this;
    }

    /**
     * Gets the whole request list as erlang terms as required by
     * <code>api_tx:req_list/2</code>
     * Note: this parses through the requests to create the erlang objects.
     *
     * @return an erlang list of requests
     */
    OtpErlangList getErlangReqList() {
        final OtpErlangObject[] result = new OtpErlangObject[requests.size()];
        int i = 0;
        for (final Operation op : requests) {
            result[i++] = op.getErlang();
        }
        return new OtpErlangList(result);
    }

    /**
     * Returns whether the transactions contains a commit or not.
     *
     * @return <tt>true</tt> if the operation contains a commit,
     *         <tt>false</tt> otherwise
     */
    public boolean isCommit() {
        return isCommit;
    }

    /**
     * Checks whether the request list is empty.
     *
     * @return <tt>true</tt> is empty, <tt>false</tt> otherwise
     */
    public boolean isEmpty() {
        return requests.isEmpty();
    }

    /**
     * Gets the number of requests in the list.
     *
     * @return number of requests
     */
    public int size() {
        return requests.size();
    }

    /**
     * Gets a list of all keys in the request list.
     * Note: this parses through the requests to create the list of keys.
     *
     * @return involved keys
     *
     * @since 3.12
     */
    public List<String> keyList() {
        final ArrayList<String> result = new ArrayList<String>(requests.size());
        for (final Operation op : requests) {
            final OtpErlangString key = op.getKey();
            if (key != null) {
                result.add(key.stringValue());
            }
        }
        return result;
    }

    /**
     * Adds all requests of the other request list to the end of this list.
     *
     * Use in implementation in sub-classes with according types as different
     * request lists may not be compatible with each other.
     *
     * @param other another request list
     *
     * @return this {@link RequestList} object
     */
    protected RequestList addAll_(final RequestList other) {
        requests.addAll(other.requests);
        return this;
    }
}
