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
package de.zib.scalaris.executor;

import com.ericsson.otp.erlang.OtpErlangException;

import de.zib.scalaris.RequestList;
import de.zib.scalaris.ResultList;
import de.zib.scalaris.UnknownException;
import de.zib.scalaris.operations.WriteOp;

/**
 * Implements a write operation.
 *
 * @param <T> the type of the value to write
 *
 * @author Nico Kruber, kruber@zib.de
 * @version 3.13
 * @since 3.13
 */
public class ScalarisWriteOp<T> implements ScalarisOp {
    final protected String key;
    final protected T value;

    /**
     * Creates a write operation.
     *
     * @param key
     *            the key to write to
     * @param value
     *            the value to write
     */
    public ScalarisWriteOp(final String key, final T value) {
        this.key = key;
        this.value = value;
    }

    public int workPhases() {
        return 1;
    }

    public final int doPhase(final int phase, final int firstOp, final ResultList results,
            final RequestList requests) throws OtpErlangException, UnknownException,
            IllegalArgumentException {
        switch (phase) {
        case 0: return prepareWrite(requests);
        case 1: return checkWrite(firstOp, results);
        default:
            throw new IllegalArgumentException("No phase " + phase);
        }
    }

    /**
     * Adds the write operation to the request list.
     *
     * @param requests  the request list
     *
     * @return <tt>0</tt> (no operation processed)
     */
    protected int prepareWrite(final RequestList requests) throws OtpErlangException,
            UnknownException {
        requests.addOp(new WriteOp(key, value));
        return 0;
    }

    /**
     * Verifies the write operations.
     *
     * @param firstOp   the first operation to process inside the result list
     * @param results   the result list
     *
     * @return <tt>1</tt> operation processed (the write)
     */
    protected int checkWrite(final int firstOp, final ResultList results)
            throws OtpErlangException, UnknownException {
        results.processWriteAt(firstOp);
        return 1;
    }

    /* (non-Javadoc)
     * @see de.zib.scalaris.examples.wikipedia.ScalarisOp#toString()
     */
    @Override
    public String toString() {
        return "Scalaris.write(" + key + ", " + value + ")";
    }

    /**
     * Gets the key to write to.
     *
     * @return the key
     */
    public String getKey() {
        return key;
    }

    /**
     * Gets the value to write.
     *
     * @return the value that will (or has been) be written
     */
    public T getValue() {
        return value;
    }
}
