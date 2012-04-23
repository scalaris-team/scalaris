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

import com.ericsson.otp.erlang.OtpErlangList;

/**
 * Generic result list.
 *
 * @author Nico Kruber, kruber@zib.de
 * @version 3.8
 * @since 3.5
 */
public abstract class ResultList {
    protected OtpErlangList results = new OtpErlangList();

    /**
     * Whether to compress the transfer of values.
     */
    protected final boolean compressed;

    /**
     * Default constructor.
     *
     * @param results
     *            the raw results list as returned by scalaris
     * @param compressed
     *            whether the value part in the term is encoded, i.e. compressed
     *            into an Erlang binary, or not
     */
    protected ResultList(final OtpErlangList results, final boolean compressed) {
        this.results = results;
        this.compressed = compressed;
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
     * Gets the raw results.
     * (for internal use only)
     *
     * @return results as returned by erlang
     */
    OtpErlangList getResults() {
        return results;
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
     *
     * @since 3.13
     */
    public abstract ErlangValue processReadAt(final int pos)
            throws TimeoutException, NotFoundException, UnknownException;

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
     *             if the commit of the write failed (if there was a commit)
     * @throws UnknownException
     *             if any other error occurs
     *
     * @since 3.13
     */
    public abstract void processWriteAt(final int pos) throws TimeoutException,
            AbortException, UnknownException;

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
     *             if the commit of the write failed (if there was a commit)
     * @throws UnknownException
     *             if any other error occurs
     *
     * @since 3.13
     */
    public abstract void processAddDelOnListAt(final int pos)
            throws TimeoutException, NotAListException, AbortException,
            UnknownException;

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
     *             if the commit of the write failed (if there was a commit)
     * @throws UnknownException
     *             if any other error occurs
     *
     * @since 3.13
     */
    public abstract void processAddOnNrAt(final int pos)
            throws TimeoutException, NotANumberException, AbortException,
            UnknownException;

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
     *             if the commit of the write failed (if there was a commit)
     * @throws UnknownException
     *             if any other error occurs
     *
     * @since 3.13
     */
    public abstract void processTestAndSetAt(final int pos)
            throws TimeoutException, NotFoundException, KeyChangedException,
            AbortException, UnknownException;
}
