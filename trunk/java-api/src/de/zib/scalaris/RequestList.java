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

import java.util.ArrayList;
import java.util.List;

import com.ericsson.otp.erlang.OtpErlangList;
import com.ericsson.otp.erlang.OtpErlangObject;
import com.ericsson.otp.erlang.OtpErlangString;
import com.ericsson.otp.erlang.OtpErlangTuple;

/**
 * Generic request list.
 *
 * @author Nico Kruber, kruber@zib.de
 * @version 3.5
 * @since 3.5
 */
public abstract class RequestList {
    protected final List<OtpErlangObject> requests = new ArrayList<OtpErlangObject>(10);
    private final boolean isCommit = false;

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
     * Adds a read operation to the list of requests.
     *
     * @param key the key to read
     *
     * @return this {@link RequestList} object
     *
     * @throws UnsupportedOperationException
     *             if the operation is unsupported, e.g. there may only be one
     *             "commit" in a request list and no request after that
     */
    public RequestList addRead(final OtpErlangObject key)
            throws UnsupportedOperationException {
        if (isCommit) {
            throw new UnsupportedOperationException("No further request supported after a commit!");
        }
        final OtpErlangTuple req = new OtpErlangTuple(new OtpErlangObject[] {
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
     *
     * @throws UnsupportedOperationException
     *             if the operation is unsupported, e.g. there may only be one
     *             "commit" in a request list and no request after that
     */
    public RequestList addRead(final String key)
            throws UnsupportedOperationException {
        return addRead(new OtpErlangString(key));
    }

    /**
     * Adds a write operation to the list of requests.
     *
     * @param key    the key to write the value to
     * @param value  the value to write
     *
     * @return this {@link RequestList} object
     *
     * @throws UnsupportedOperationException
     *             if the operation is unsupported, e.g. there may only be one
     *             "commit" in a request list and no request after that
     */
    public RequestList addWrite(final OtpErlangObject key, final OtpErlangObject value)
            throws UnsupportedOperationException {
        if (isCommit) {
            throw new UnsupportedOperationException("No further request supported after a commit!");
        }
        final OtpErlangTuple req = new OtpErlangTuple(new OtpErlangObject[] {
                CommonErlangObjects.writeAtom, key, value });
        requests.add(req);
        return this;
    }

    /**
     * Adds a write operation to the list of requests.
     *
     * @param <T>    type of the value to write
     * @param key    the key to write the value to
     * @param value  the value to write
     *
     * @return this {@link RequestList} object
     *
     * @throws UnsupportedOperationException
     *             if the operation is unsupported, e.g. there may only be one
     *             "commit" in a request list and no request after that
     */
    public <T> RequestList addWrite(final String key, final T value)
            throws UnsupportedOperationException {
        return addWrite(new OtpErlangString(key), ErlangValue.convertToErlang(value));
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
        if (isCommit) {
            throw new UnsupportedOperationException("Only one commit per request list allowed!");
        }
        final OtpErlangTuple req = CommonErlangObjects.commitTupleAtom;
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
