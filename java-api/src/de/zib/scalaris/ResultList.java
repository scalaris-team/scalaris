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

import java.util.List;

import com.ericsson.otp.erlang.OtpErlangList;
import com.ericsson.otp.erlang.OtpErlangObject;

import de.zib.scalaris.RequestList;
import de.zib.scalaris.operations.Operation;
import de.zib.scalaris.operations.ReadOp;

/**
 * Generic result list.
 *
 * @author Nico Kruber, kruber@zib.de
 * @version 3.8
 * @since 3.5
 */
public abstract class ResultList {
    final protected List<Operation> operations;

    /**
     * Default constructor.
     *
     * @param results
     *            the raw results list as returned by scalaris
     * @param compressed
     *            whether the value part in the term is encoded, i.e. compressed
     *            into an Erlang binary, or not
     * @param requests
     *            request list which created this result list
     *
     * @throws UnknownException
     *             if the result list size does not match the request list size
     */
    protected ResultList(final OtpErlangList results, final boolean compressed,
            final RequestList requests) throws UnknownException {
        if (results.arity() != requests.size()) {
            throw new UnknownException("Result list size different from request list size!");
        }
        this.operations = requests.getRequests();

        // assign the results to their appropriate operations
        for (int i = 0; i < results.arity(); ++i) {
            final OtpErlangObject result = results.elementAt(i);
            this.operations.get(i).setResult(result, compressed);
        }
    }

    /**
     * Gets the number of results in the list.
     *
     * @return total number of results
     */
    public int size() {
        return operations.size();
    }

    /**
     * Returns the operation at the specified position for e.g. further result
     * processing.
     *
     * @param index
     *            index of the operation/result to return
     *
     * @return the operation at the specified position
     *
     * @throws IndexOutOfBoundsException
     *             if the index is out of range (
     *             <tt>index &lt; 0 || index &gt;= size()</tt>)
     *
     * @since 3.18
     */
    public Operation get(final int index) {
        return operations.get(index);
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
     * @throws NotFoundException
     *             if the requested key does not exist
     * @throws UnknownException
     *             if any other error occurs
     *
     * @since 3.13
     */
    public ErlangValue processReadAt(final int pos) throws NotFoundException,
            UnknownException {
        return ((ReadOp) get(pos)).processResult();
    }

    /**
     * Processes the result at the given position which originated from
     * a write request.
     *
     * @param pos
     *            the position in the result list (starting at 0)
     *
     * @throws AbortException
     *             if the commit of the write failed (if there was a commit)
     * @throws UnknownException
     *             if any other error occurs
     *
     * @since 3.13
     */
    public abstract void processWriteAt(final int pos) throws AbortException,
            UnknownException;

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
     *             if the commit of the write failed (if there was a commit)
     * @throws UnknownException
     *             if any other error occurs
     *
     * @since 3.13
     */
    public abstract void processAddDelOnListAt(final int pos)
            throws NotAListException, AbortException, UnknownException;

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
     *             if the commit of the write failed (if there was a commit)
     * @throws UnknownException
     *             if any other error occurs
     *
     * @since 3.13
     */
    public abstract void processAddOnNrAt(final int pos)
            throws NotANumberException, AbortException, UnknownException;

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
     *             if the commit of the write failed (if there was a commit)
     * @throws UnknownException
     *             if any other error occurs
     *
     * @since 3.13
     */
    public abstract void processTestAndSetAt(final int pos)
            throws NotFoundException, KeyChangedException, AbortException,
            UnknownException;

    @Override
    public String toString() {
        int i = 1;
        final StringBuilder sb = new StringBuilder();
        sb.append('[');
        for (final Operation op : operations) {
            sb.append(op.getResult().toString());
            if (i != operations.size()) {
                sb.append(", ");
            }
            ++i;
        }
        sb.append(']');
        return sb.toString();
    }
}
