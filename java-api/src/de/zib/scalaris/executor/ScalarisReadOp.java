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

import de.zib.scalaris.ErlangValue;
import de.zib.scalaris.NotFoundException;
import de.zib.scalaris.RequestList;
import de.zib.scalaris.ResultList;
import de.zib.scalaris.UnknownException;
import de.zib.scalaris.operations.ReadOp;

/**
 * Implements a read operation (tolerates "not found" and in this case contains
 * <tt>null</tt>).
 *
 * @author Nico Kruber, kruber@zib.de
 * @version 3.13
 * @since 3.13
 */
public class ScalarisReadOp implements ScalarisOp {
    final protected String key;
    protected ErlangValue value = null;

    /**
     * Creates a read operation.
     *
     * @param key
     *            the key to read from
     */
    public ScalarisReadOp(final String key) {
        this.key = key;
    }

    public int workPhases() {
        return 1;
    }

    public final int doPhase(final int phase, final int firstOp, final ResultList results,
            final RequestList requests) throws OtpErlangException, UnknownException,
            IllegalArgumentException {
        switch (phase) {
        case 0: return prepareRead(requests);
        case 1: return checkRead(firstOp, results);
        default:
            throw new IllegalArgumentException("No phase " + phase);
        }
    }

    /**
     * Adds the read operation to the request list.
     *
     * @param requests  the request list
     *
     * @return <tt>0</tt> (no operation processed)
     */
    protected int prepareRead(final RequestList requests) throws OtpErlangException,
            UnknownException {
        requests.addOp(new ReadOp(key));
        return 0;
    }

    /**
     * Verifies the read operation and stores the value.
     *
     * @param firstOp   the first operation to process inside the result list
     * @param results   the result list
     *
     * @return <tt>1</tt> operation processed (the read)
     */
    protected int checkRead(final int firstOp, final ResultList results)
            throws OtpErlangException, UnknownException {
        try {
            value = results.processReadAt(firstOp);
        } catch (final NotFoundException e) {
        }
        return 1;
    }

    /* (non-Javadoc)
     * @see de.zib.scalaris.examples.wikipedia.ScalarisOp#toString()
     */
    @Override
    public String toString() {
        return "Scalaris.read(" + key + ", " + value + ")";
    }

    /**
     * Gets the key to read from.
     *
     * @return the key
     */
    public String getKey() {
        return key;
    }

    /**
     * Gets the value from the read.
     *
     * @return the value that has been read (may be <tt>null</tt>
     */
    public ErlangValue getValue() {
        return value;
    }
}
