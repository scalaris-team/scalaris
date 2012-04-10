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

import com.ericsson.otp.erlang.OtpErlangException;
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
     * Default constructor.
     *
     * @param results  the raw results list as returned by scalaris.
     */
    protected ResultList(final OtpErlangList results) {
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
     * @throws OtpErlangException
     *             if a (known) error occurs
     * @throws UnknownException
     *             if any other error occurs
     *
     * @since 3.13
     */
    public abstract void processWriteAt(final int pos) throws TimeoutException,
            OtpErlangException, UnknownException;

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
     * @throws OtpErlangException
     *             if another (known) error occurs
     * @throws UnknownException
     *             if any other error occurs
     *
     * @since 3.13
     */
    public abstract void processAddDelOnListAt(final int pos)
            throws TimeoutException, NotAListException, OtpErlangException,
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
     * @throws OtpErlangException
     *             if another (known) error occurs
     * @throws UnknownException
     *             if any other error occurs
     *
     * @since 3.13
     */
    public abstract void processAddOnNrAt(final int pos)
            throws TimeoutException, NotANumberException, OtpErlangException,
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
     * @throws OtpErlangException
     *             if another (known) error occurs
     * @throws UnknownException
     *             if any other error occurs
     *
     * @since 3.13
     */
    public abstract void processTestAndSetAt(final int pos)
            throws TimeoutException, NotFoundException, KeyChangedException,
            OtpErlangException, UnknownException;
}
