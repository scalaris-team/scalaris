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

import de.zib.scalaris.AbortException;
import de.zib.scalaris.CommonErlangObjects;
import de.zib.scalaris.ErlangValue;
import de.zib.scalaris.UnknownException;

/**
 * An operation writing a value.
 *
 * @author Nico Kruber, kruber@zib.de
 * @version 3.14
 * @since 3.14
 */
public class WriteOp implements TransactionOperation, TransactionSingleOpOperation {
    final protected OtpErlangString key;
    final protected OtpErlangObject value;
    protected OtpErlangObject resultRaw = null;
    protected boolean resultCompressed = false;

    /**
     * Constructor
     *
     * @param key
     *            the key to write the value to
     * @param value
     *            the value to write
     */
    public WriteOp(final OtpErlangString key, final OtpErlangObject value) {
        this.key = key;
        this.value = value;
    }
    /**
     * Constructor
     *
     * @param key
     *            the key to write the value to
     * @param value
     *            the value to write
     */
    public <T> WriteOp(final String key, final T value) {
        this.key = new OtpErlangString(key);
        this.value = ErlangValue.convertToErlang(value);
    }

    public OtpErlangObject getErlang(final boolean compressed) {
        return new OtpErlangTuple(new OtpErlangObject[] {
                CommonErlangObjects.writeAtom, key,
                compressed ? CommonErlangObjects.encode(value) : value });
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

    public Object processResult() throws UnknownException {
        /*
         * possible return values:
         *  {ok}
         */
        try {
            final OtpErlangTuple received = (OtpErlangTuple) resultRaw;
            if (received.equals(CommonErlangObjects.okTupleAtom)) {
                return null;
            }
            throw new UnknownException(resultRaw);
        } catch (final ClassCastException e) {
            // e.printStackTrace();
            throw new UnknownException(e, resultRaw);
        }
    }

    public Object processResultSingle() throws AbortException, UnknownException {
        CommonErlangObjects.checkResult_failAbort(resultRaw, resultCompressed);
        return processResult();
    }

    @Override
    public String toString() {
        return "write(" + key + ", " + value + ")";
    }
}
