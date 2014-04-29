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
package de.zib.scalaris.operations;

import com.ericsson.otp.erlang.OtpErlangObject;
import com.ericsson.otp.erlang.OtpErlangString;

import de.zib.scalaris.AbortException;
import de.zib.scalaris.EmptyListException;
import de.zib.scalaris.KeyChangedException;
import de.zib.scalaris.NotAListException;
import de.zib.scalaris.NotANumberException;
import de.zib.scalaris.NotFoundException;
import de.zib.scalaris.UnknownException;

/**
 * Generic interface for operations which can be added to a request list.
 *
 * @author Nico Kruber, kruber@zib.de
 * @version 3.18
 * @since 3.14
 */
public interface Operation {
    /**
     * Gets the erlang representation of the operation.
     *
     * @param compressed
     *            whether the value part in the term should be encoded, i.e.
     *            compressed into an Erlang binary, or not
     *
     * @return erlang representation for api_tx:req_list
     */
    abstract public OtpErlangObject getErlang(final boolean compressed);
     /**
      * Gets the key the operation is working on (if available)
      *
      * @return the key or <tt>null</tt>
      */
    abstract public OtpErlangString getKey();

    /**
     * Sets the raw erlang result value. It can be processed using
     * {@link #processResult()}.
     *
     * @param resultRaw
     *            the result
     * @param compressed
     *            whether the value inside the result is compressed or not
     *
     * @since 3.18
     */
    public abstract void setResult(final OtpErlangObject resultRaw,
            final boolean compressed);

    /**
     * Gets the (raw Erlang) result set via
     * {@link #setResult(OtpErlangObject, boolean)}.
     *
     * @return the result object or <tt>null</tt> if not set
     *
     * @since 3.18
     */
    public abstract OtpErlangObject getResult();

    /**
     * Determines if the result set via
     * {@link #setResult(OtpErlangObject, boolean)} is compressed or not.
     *
     * @return <tt>true</tt> if compressed, <tt>false</tt> otherwise, undefined
     *         if no result was set
     *
     * @since 3.18
     */
    public abstract boolean getResultCompressed();

    /**
     * Processes the result set by {@link #setResult(OtpErlangObject, boolean)}.
     *
     * Note: the created value is not cached!
     *
     * @return a (potentially) read value (may be <tt>null</tt>)
     *
     * @throws NotFoundException
     *             if the requested key does not exist
     * @throws KeyChangedException
     *             if the key did not match <tt>old_value</tt>
     * @throws NotANumberException
     *             if the previously stored value was not a number
     * @throws NotAListException
     *             if the previously stored value was no list
     * @throws EmptyListException
     *             if the stored value is an empty list but the op requires a
     *             non-empty list
     * @throws AbortException
     *             if a commit failed
     * @throws UnknownException
     *             if any other error occurs
     *
     * @since 3.18
     */
    public abstract Object processResult() throws NotFoundException,
            KeyChangedException, NotANumberException, NotAListException,
            AbortException, EmptyListException, UnknownException;
}
