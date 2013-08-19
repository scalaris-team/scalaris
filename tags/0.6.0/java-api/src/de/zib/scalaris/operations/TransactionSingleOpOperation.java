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

import de.zib.scalaris.AbortException;
import de.zib.scalaris.EmptyListException;
import de.zib.scalaris.KeyChangedException;
import de.zib.scalaris.NotAListException;
import de.zib.scalaris.NotANumberException;
import de.zib.scalaris.NotFoundException;
import de.zib.scalaris.TransactionSingleOp;
import de.zib.scalaris.UnknownException;

/**
 * An operation suitable for use in {@link TransactionSingleOp}.
 *
 * @author Nico Kruber, kruber@zib.de
 * @version 3.18
 * @since 3.14
 */
public interface TransactionSingleOpOperation extends Operation {

    /**
     * Processes the result set by {@link #setResult(OtpErlangObject, boolean)}
     * assuming that operation was committed.
     *
     * In contrast to {@link #processResult()} operations like {@link WriteOp}
     * will throw a proper {@link AbortException} for their commit part instead
     * of an {@link UnknownException}.
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
    public abstract Object processResultSingle() throws NotFoundException,
            KeyChangedException, NotANumberException, NotAListException,
            AbortException, EmptyListException, UnknownException;
}
