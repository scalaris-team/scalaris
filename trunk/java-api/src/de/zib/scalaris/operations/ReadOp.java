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

import com.ericsson.otp.erlang.OtpErlangDecodeException;
import com.ericsson.otp.erlang.OtpErlangObject;
import com.ericsson.otp.erlang.OtpErlangString;
import com.ericsson.otp.erlang.OtpErlangTuple;

import de.zib.scalaris.CommonErlangObjects;
import de.zib.scalaris.NotFoundException;
import de.zib.scalaris.UnknownException;

/**
 * Operation reading a value.
 *
 * @author Nico Kruber, kruber@zib.de
 * @version 3.14
 * @since 3.14
 */
public class ReadOp implements TransactionOperation, TransactionSingleOpOperation {
    final protected OtpErlangString key;
    /**
     * Constructor
     *
     * @param key
     *            the key to read
     */
    public ReadOp(final OtpErlangString key) {
        this.key = key;
    }
    /**
     * Constructor
     *
     * @param key
     *            the key to read
     */
    public ReadOp(final String key) {
        this.key = new OtpErlangString(key);
    }

    public OtpErlangObject getErlang(final boolean compressed) {
        return new OtpErlangTuple(new OtpErlangObject[] {
                CommonErlangObjects.readAtom, key });
    }

    public OtpErlangString getKey() {
        return key;
    }

    @Override
    public String toString() {
        return "read(" + key + ")";
    }

    /**
     * Processes the <tt>received_raw</tt> term from erlang interpreting it as a
     * result from a read operation.
     *
     * NOTE: this method should not be called manually by an application and may
     * change without notice!
     *
     * @param received_raw
     *            the object to process
     * @param compressed
     *            whether the transfer of values is compressed or not
     *
     * @return the contained value
     *
     * @throws NotFoundException
     *             if the requested key does not exist
     * @throws UnknownException
     *             if any other error occurs
     */
    public static final OtpErlangObject processResult_read(
            final OtpErlangObject received_raw, final boolean compressed)
            throws NotFoundException, UnknownException {
        /*
         * possible return values:
         *  {ok, Value} | {fail, not_found}
         */
        try {
            final OtpErlangTuple received = (OtpErlangTuple) received_raw;
            final OtpErlangObject state = received.elementAt(0);
            if (received.arity() != 2) {
                throw new UnknownException(received_raw);
            }
            if (state.equals(CommonErlangObjects.okAtom)) {
                OtpErlangObject result = received.elementAt(1);
                if (compressed) {
                    result = CommonErlangObjects.decode(result);
                }
                return result;
            } else if (state.equals(CommonErlangObjects.failAtom)) {
                final OtpErlangObject reason = received.elementAt(1);
                if (reason.equals(CommonErlangObjects.notFoundAtom)) {
                    throw new NotFoundException(received_raw);
                }
            }
            throw new UnknownException(received_raw);
        } catch (final ClassCastException e) {
            // e.printStackTrace();
            throw new UnknownException(e, received_raw);
        } catch (final OtpErlangDecodeException e) {
            // e.printStackTrace();
            throw new UnknownException(e, received_raw);
        }
    }
}
