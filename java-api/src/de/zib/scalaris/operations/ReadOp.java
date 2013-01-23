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

import de.zib.scalaris.AbortException;
import de.zib.scalaris.CommonErlangObjects;
import de.zib.scalaris.ErlangValue;
import de.zib.scalaris.KeyChangedException;
import de.zib.scalaris.NotAListException;
import de.zib.scalaris.NotANumberException;
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
    protected OtpErlangObject resultRaw = null;
    protected boolean resultCompressed = false;

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

    public ErlangValue processResult() throws NotFoundException,
            UnknownException {
        /*
         * possible return values:
         *  {ok, Value} | {fail, not_found}
         */
        try {
            final OtpErlangTuple received = (OtpErlangTuple) resultRaw;
            final OtpErlangObject state = received.elementAt(0);
            if (received.arity() != 2) {
                throw new UnknownException(resultRaw);
            }
            if (state.equals(CommonErlangObjects.okAtom)) {
                OtpErlangObject result = received.elementAt(1);
                if (resultCompressed) {
                    result = CommonErlangObjects.decode(result);
                }
                return new ErlangValue(result);
            } else if (state.equals(CommonErlangObjects.failAtom)) {
                final OtpErlangObject reason = received.elementAt(1);
                if (reason.equals(CommonErlangObjects.notFoundAtom)) {
                    throw new NotFoundException(resultRaw);
                }
            }
            throw new UnknownException(resultRaw);
        } catch (final ClassCastException e) {
            // e.printStackTrace();
            throw new UnknownException(e, resultRaw);
        } catch (final OtpErlangDecodeException e) {
            // e.printStackTrace();
            throw new UnknownException(e, resultRaw);
        }
    }

    public ErlangValue processResultSingle() throws NotFoundException,
            KeyChangedException, NotANumberException, NotAListException,
            AbortException, UnknownException {
        return processResult();
    }

    @Override
    public String toString() {
        return "read(" + key + ")";
    }
}
