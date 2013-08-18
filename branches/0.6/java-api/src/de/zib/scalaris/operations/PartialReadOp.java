/**
 *  Copyright 2013 Zuse Institute Berlin
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

/**
 * Operation reading a partial value.
 *
 * @author Nico Kruber, kruber@zib.de
 * @version 3.18
 * @since 3.18
 */
public abstract class PartialReadOp implements TransactionOperation,
        TransactionSingleOpOperation {
    final protected OtpErlangString key;
    protected OtpErlangObject resultRaw = null;
    protected boolean resultCompressed = false;

    /**
     * Constructor
     *
     * @param key
     *            the key to read
     */
    public PartialReadOp(final OtpErlangString key) {
        this.key = key;
    }
    /**
     * Constructor
     *
     * @param key
     *            the key to read
     */
    public PartialReadOp(final String key) {
        this.key = new OtpErlangString(key);
    }

    public OtpErlangString getKey() {
        return key;
    }

    public void setResult(final OtpErlangObject resultRaw, final boolean compressed) {
        this.resultRaw = resultRaw;
        this.resultCompressed = compressed;
    }

    public OtpErlangObject getResult() {
        return this.resultRaw;
    }

    public boolean getResultCompressed() {
        return this.resultCompressed;
    }
}
