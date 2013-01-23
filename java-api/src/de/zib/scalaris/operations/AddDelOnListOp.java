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

import java.util.List;

import com.ericsson.otp.erlang.OtpErlangList;
import com.ericsson.otp.erlang.OtpErlangObject;
import com.ericsson.otp.erlang.OtpErlangString;
import com.ericsson.otp.erlang.OtpErlangTuple;

import de.zib.scalaris.AbortException;
import de.zib.scalaris.CommonErlangObjects;
import de.zib.scalaris.ErlangValue;
import de.zib.scalaris.NotAListException;
import de.zib.scalaris.UnknownException;

/**
 * Operation appending to / removing from a list.
 *
 * @author Nico Kruber, kruber@zib.de
 * @version 3.14
 * @since 3.14
 */
public class AddDelOnListOp implements TransactionOperation, TransactionSingleOpOperation {
    final protected OtpErlangString key;
    final protected OtpErlangObject toAdd;
    final protected OtpErlangObject toRemove;
    protected OtpErlangObject resultRaw = null;
    protected boolean resultCompressed = false;
    /**
     * Constructor
     *
     * @param key
     *            the key to write the value to
     * @param toAdd
     *            a list of values to add to a list
     * @param toRemove
     *            a list of values to remove from a list
     */
    public AddDelOnListOp(final OtpErlangString key, final OtpErlangList toAdd, final OtpErlangList toRemove) {
        this.key = key;
        this.toAdd = toAdd;
        this.toRemove = toRemove;
    }
    /**
     * Constructor
     *
     * @param key
     *            the key to write the value to
     * @param toAdd
     *            a list of values to add to a list
     * @param toRemove
     *            a list of values to remove from a list
     */
    public <T> AddDelOnListOp(final String key, final List<T> toAdd, final List<T> toRemove) {
        this.key = new OtpErlangString(key);
        this.toAdd = (toAdd == null) ? new OtpErlangList() : (OtpErlangList) ErlangValue.convertToErlang(toAdd);
        this.toRemove = (toRemove == null) ? new OtpErlangList() : (OtpErlangList) ErlangValue.convertToErlang(toRemove);
    }

    public OtpErlangObject getErlang(final boolean compressed) {
        return new OtpErlangTuple(new OtpErlangObject[] {
                CommonErlangObjects.addDelOnListAtom, key,
                compressed ? CommonErlangObjects.encode(toAdd) : toAdd,
                compressed ? CommonErlangObjects.encode(toRemove) : toRemove });
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

    public Object processResult() throws UnknownException,
            NotAListException {
        /*
         * possible return values:
         *  {ok} | {fail, not_a_list}.
         */
        try {
            final OtpErlangTuple received = (OtpErlangTuple) resultRaw;
            if (received.equals(CommonErlangObjects.okTupleAtom)) {
                return null;
            } else if (received.elementAt(0).equals(CommonErlangObjects.failAtom) && (received.arity() == 2)) {
                final OtpErlangObject reason = received.elementAt(1);
                if (reason.equals(CommonErlangObjects.notAListAtom)) {
                    throw new NotAListException(resultRaw);
                }
            }
            throw new UnknownException(resultRaw);
        } catch (final ClassCastException e) {
            // e.printStackTrace();
            throw new UnknownException(e, resultRaw);
        }
    }

    public Object processResultSingle() throws AbortException,
            NotAListException, UnknownException {
        CommonErlangObjects.checkResult_failAbort(resultRaw, resultCompressed);
        return processResult();
    }

    @Override
    public String toString() {
        return "add_del_on_list(" + key + ", " + toAdd + ", " + toRemove + ")";
    }
}
