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
import com.ericsson.otp.erlang.OtpErlangTuple;

import de.zib.scalaris.CommonErlangObjects;
import de.zib.scalaris.ErlangValue;

/**
 * Atomic test-and-set operation, i.e. {@link #newValue} is only written if the
 * currently stored value is {@link #oldValue}.
 *
 * @author Nico Kruber, kruber@zib.de
 * @version 3.14
 * @since 3.14
 */
public class TestAndSetOp implements TransactionOperation, TransactionSingleOpOperation {
    final OtpErlangString key;
    final OtpErlangObject oldValue;
    final OtpErlangObject newValue;
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
}
