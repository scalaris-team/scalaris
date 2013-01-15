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
import de.zib.scalaris.ErlangValue;
import de.zib.scalaris.KeyChangedException;
import de.zib.scalaris.NotFoundException;
import de.zib.scalaris.UnknownException;

/**
 * Atomic test-and-set operation, i.e. {@link #newValue} is only written if the
 * currently stored value is {@link #oldValue}.
 *
 * @author Nico Kruber, kruber@zib.de
 * @version 3.14
 * @since 3.14
 */
public class TestAndSetOp implements TransactionOperation, TransactionSingleOpOperation {
    final protected OtpErlangString key;
    final protected OtpErlangObject oldValue;
    final protected OtpErlangObject newValue;

    /**
     * Constructor
     *
     * @param key
     *            the key to write the value to
     * @param oldValue
     *            the old value to verify
     * @param newValue
     *            the new value to write of oldValue is correct
     */
    public TestAndSetOp(final OtpErlangString key, final OtpErlangObject oldValue, final OtpErlangObject newValue) {
        this.key = key;
        this.oldValue = oldValue;
        this.newValue = newValue;
    }

    /**
     * Constructor
     *
     * @param key
     *            the key to write the value to
     * @param oldValue
     *            the old value to verify
     * @param newValue
     *            the new value to write of oldValue is correct
     */
    public <OldT, NewT> TestAndSetOp(final String key, final OldT oldValue, final NewT newValue) {
        this.key = new OtpErlangString(key);
        this.oldValue = ErlangValue.convertToErlang(oldValue);
        this.newValue = ErlangValue.convertToErlang(newValue);
    }

    public OtpErlangObject getErlang(final boolean compressed) {
        return new OtpErlangTuple(new OtpErlangObject[] {
                CommonErlangObjects.testAndSetAtom, key,
                compressed ? CommonErlangObjects.encode(oldValue) : oldValue,
                compressed ? CommonErlangObjects.encode(newValue) : newValue });
    }

    public OtpErlangString getKey() {
        return key;
    }

    @Override
    public String toString() {
        return "test_and_set(" + key + ", " + oldValue + ", " + newValue + ")";
    }

    /**
     * Processes the <tt>received_raw</tt> term from erlang interpreting it as a
     * result from a test_and_set operation.
     *
     * NOTE: this method should not be called manually by an application and may
     * change without prior notice!
     *
     * @param received_raw
     *            the object to process
     * @param compressed
     *            whether the transfer of values is compressed or not
     *
     * @throws NotFoundException
     *             if the requested key does not exist
     * @throws KeyChangedException
     *             if the key did not match <tt>old_value</tt>
     * @throws UnknownException
     *             if any other error occurs
     *
     * @since 3.8
     */
    public static final void processResult_testAndSet(
            final OtpErlangObject received_raw, final boolean compressed)
            throws NotFoundException, KeyChangedException,
            UnknownException {
        /*
         * possible return values:
         *  {ok} | {fail, not_found | {key_changed, RealOldValue}
         */
        try {
            final OtpErlangTuple received = (OtpErlangTuple) received_raw;
            if (received.equals(CommonErlangObjects.okTupleAtom)) {
                return;
            } else if (received.elementAt(0).equals(CommonErlangObjects.failAtom) && (received.arity() == 2)) {
                final OtpErlangObject reason = received.elementAt(1);
                if (reason.equals(CommonErlangObjects.notFoundAtom)) {
                    throw new NotFoundException(received_raw);
                } else {
                    final OtpErlangTuple reason_tpl = (OtpErlangTuple) reason;
                    if (reason_tpl.elementAt(0).equals(
                            CommonErlangObjects.keyChangedAtom)
                            && (reason_tpl.arity() == 2)) {
                        OtpErlangObject result = reason_tpl.elementAt(1);
                        if (compressed) {
                            result = CommonErlangObjects.decode(result);
                        }
                        throw new KeyChangedException(new ErlangValue(result));
                    }
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
