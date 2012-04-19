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

/**
 * Operation reading a value.
 *
 * @author Nico Kruber, kruber@zib.de
 * @version 3.14
 * @since 3.14
 */
public class ReadOp implements TransactionOperation, TransactionSingleOpOperation {
    final OtpErlangString key;
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

    public OtpErlangObject getErlang() {
        return new OtpErlangTuple(new OtpErlangObject[] {
                CommonErlangObjects.readAtom, key });
    }
    public OtpErlangString getKey() {
        return key;
    }
}
